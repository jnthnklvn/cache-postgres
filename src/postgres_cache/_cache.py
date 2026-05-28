"""
Public cache facade for postgres-cache.

Spec: _reversa_sdd/migration/target_architecture.md § BC-1, DA-03, DA-04, DA-10
      _reversa_sdd/migration/target_domain_model.md § Comandos, PostgresCache
      _reversa_sdd/migration/target_business_rules.md § BR-MIGRAR-001, BR-MIGRAR-003,
                                                         BR-MIGRAR-005, BR-MIGRAR-012
      _reversa_sdd/migration/risk_register.md § RISK-003, RISK-004

⚠️ RISK-003: The background scanner thread MUST be stopped gracefully.
             __del__ is unreliable in Python. Always use PostgresCache as a
             context manager, or call close() explicitly:

               with PostgresCache(options) as cache:
                   cache.set("key", b"value")

             OR:

               cache = PostgresCache(options)
               try:
                   cache.set("key", b"value")
               finally:
                   cache.close()
"""

from __future__ import annotations

import logging
import threading
import functools
import inspect
import pickle
from datetime import timedelta
from typing import Callable

from ._db import DatabaseOperations
from ._options import EntryOptions, PostgresCacheOptions
from ._sql import SqlQueries

logger = logging.getLogger(__name__)  # DA-07 / BR-MIGRAR-011

#: Maximum allowed key length — BR-MIGRAR-012
_MAX_KEY_LENGTH: int = 449


class PostgresCache:
    """Distributed cache backed by PostgreSQL.

    Main public interface. Instantiate directly (no DI required):

        options = PostgresCacheOptions(
            dsn="postgresql://user:pass@localhost/db",
            schema="public",
            table="cache",
            create_if_not_exists=True,
        )

        with PostgresCache(options) as cache:
            cache.set("key", b"value")
            data = cache.get("key")   # bytes | None

    The background expiration scanner is started on __enter__ and stopped
    gracefully on __exit__ / close(). Using PostgresCache outside a context
    manager is allowed but requires an explicit cache.close() call.

    Spec: target_architecture.md § BC-1 (Cache Facade)
          target_domain_model.md § Comandos
    """

    def __init__(self, options: PostgresCacheOptions) -> None:
        """
        Args:
            options: Validated PostgresCacheOptions. Use PostgresCacheOptions(...)
                     to build and validate configuration before passing here.
        """
        self._options = options
        self._sql = SqlQueries(
            schema=options.schema,
            table=options.table,
            use_wal=options.use_wal,
        )
        self._db = DatabaseOperations(options=options, sql=self._sql)

        # Scanner thread state — BR-MIGRAR-005, DA-03
        self._stop_event: threading.Event = threading.Event()
        self._scanner_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Context manager — RISK-003, DA-04
    # ------------------------------------------------------------------

    def __enter__(self) -> "PostgresCache":
        """Start the background expiration scanner and return self.

        ⚠️ RISK-003: Required to guarantee the scanner thread is stopped.
        """
        self._start_scanner()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stop the background scanner gracefully."""
        self.close()
        return None  # do not suppress exceptions

    def close(self) -> None:
        """Stop the background expiration scanner thread gracefully.

        Signals the scanner to stop and waits for it to terminate (up to 5 s).
        Safe to call multiple times.

        Spec: target_domain_model.md § Comandos § close
              RISK-003 — __del__ is not reliable; always call close() or use
              the context manager.
        """
        self._stop_event.set()
        if self._scanner_thread is not None and self._scanner_thread.is_alive():
            self._scanner_thread.join(timeout=5.0)
            if self._scanner_thread.is_alive():
                logger.warning(
                    "Scanner thread did not stop within 5 seconds."
                )
        self._scanner_thread = None
        
        # Ensure database connection pool is closed
        self._db.close()

    # ------------------------------------------------------------------
    # Scanner thread — BR-MIGRAR-005, DA-03
    # ------------------------------------------------------------------

    def _start_scanner(self) -> None:
        """Start the background expiration scanner daemon thread.

        Only starts when options.enable_expiration_scan is True.
        No-op if the thread is already running.
        """
        if not self._options.enable_expiration_scan:
            logger.debug("Expiration scanner is disabled (enable_expiration_scan=False).")
            return
        if self._scanner_thread is not None and self._scanner_thread.is_alive():
            return
        self._stop_event.clear()
        self._scanner_thread = threading.Thread(
            target=self._scanner_loop,
            name="postgres-cache-scanner",
            daemon=True,  # DA-03: daemon=True ensures thread dies with process
        )
        self._scanner_thread.start()
        logger.debug("Expiration scanner started (interval=%s).", self._options.expiration_scan_interval)

    def _scanner_loop(self) -> None:
        """Background loop: delete expired entries at the configured interval.

        Uses threading.Event.wait() instead of time.sleep() so that close()
        can interrupt the sleep immediately.

        Spec: target_domain_model.md § Comandos (scanner implicit behavior)
              BR-MIGRAR-005
        """
        interval_seconds = self._options.expiration_scan_interval.total_seconds()
        while not self._stop_event.wait(timeout=interval_seconds):
            try:
                count = self._db.delete_expired()
                if count > 0:
                    logger.debug("Scanner deleted %d expired entries.", count)
            except Exception:
                logger.exception(
                    "Expiration scanner encountered an error. "
                    "Continuing — next scan in %s.",
                    self._options.expiration_scan_interval,
                )

    # ------------------------------------------------------------------
    # Key validation — BR-MIGRAR-012
    # ------------------------------------------------------------------

    def _validate_key(self, key: str) -> None:
        """Reject keys longer than 449 characters.

        Spec: target_domain_model.md § CacheEntry invariants
              BR-MIGRAR-012: key ≤ 449 chars.

        Raises:
            ValueError: If the key exceeds _MAX_KEY_LENGTH characters.
        """
        if not key:
            raise ValueError("Cache key must not be empty.")
        if len(key) > _MAX_KEY_LENGTH:
            raise ValueError(
                f"Cache key exceeds maximum length of {_MAX_KEY_LENGTH} characters "
                f"(got {len(key)}). — BR-MIGRAR-012"
            )

    # ------------------------------------------------------------------
    # Public API — BR-MIGRAR-003
    # ------------------------------------------------------------------

    def get(self, key: str) -> bytes | None:
        """Retrieve a cached value by key.

        Renews sliding expiration atomically in the database on each read
        (BR-MIGRAR-009). Returns None if the key is absent or expired.

        RISK-004: all timestamps computed as tz-aware UTC inside _db.get().

        Args:
            key: Cache key (≤ 449 characters).

        Returns:
            Cached bytes, or None on miss / expiry.
        """
        self._validate_key(key)
        return self._db.get(key)

    def set(
        self,
        key: str,
        value: bytes,
        options: EntryOptions | None = None,
    ) -> None:
        """Store a value in the cache (insert or update).

        Uses an atomic UPSERT via CTE ON CONFLICT (BR-MIGRAR-007).

        Args:
            key:     Cache key (≤ 449 characters).
            value:   Bytes to store. Not interpreted by the library.
            options: Per-entry TTL options. When None, default sliding
                     expiration from PostgresCacheOptions is applied.
        """
        self._validate_key(key)
        self._db.set(key, value, options)

    def refresh(self, key: str) -> None:
        """Renew the sliding expiration of an entry without returning its value.

        No-op if the entry is expired or absent.

        Args:
            key: Cache key (≤ 449 characters).
        """
        self._validate_key(key)
        self._db.refresh(key)

    def remove(self, key: str) -> None:
        """Physically delete a cache entry by key.

        Args:
            key: Cache key (≤ 449 characters).
        """
        self._validate_key(key)
        self._db.remove(key)

    # ------------------------------------------------------------------
    # Distributed Primitives (Counters & Locks)
    # ------------------------------------------------------------------

    def incr(self, key: str, value: int = 1) -> int:
        """Atomically increment a counter.
        
        Args:
            key: Cache key.
            value: Amount to increment (default 1).
            
        Returns:
            The new incremented value.
        """
        self._validate_key(key)
        return self._db.incr(key, value)

    def is_locked(self, key: str, wait: float = 0, step: float = 0.5) -> bool:
        """Check if a lock is currently held, optionally polling until timeout.
        
        Args:
            key: Lock key.
            wait: Max time in seconds to wait for the lock to become available.
            step: Sleep interval in seconds between polls.
            
        Returns:
            True if locked, False if unlocked.
        """
        self._validate_key(key)
        import time
        start = time.time()
        while True:
            if not self._db.is_locked(key):
                return False
            if wait <= 0 or (time.time() - start) >= wait:
                return True
            time.sleep(step)

    def set_lock(self, key: str, value: str, expire: str | timedelta) -> bool:
        """Atomically acquire a lock.
        
        Args:
            key: Lock key.
            value: Lock owner token.
            expire: Lock duration.
            
        Returns:
            True if acquired, False if already held.
        """
        self._validate_key(key)
        from datetime import datetime, timezone
        from ._utils import parse_duration
        
        utc_now = datetime.now(tz=timezone.utc)
        if isinstance(expire, str):
            delta = parse_duration(expire)
            expires_at = utc_now + delta
        else:
            expires_at = utc_now + expire
            
        return self._db.set_lock(key, value.encode('utf-8'), expires_at)

    def unlock(self, key: str, value: str) -> bool:
        """Atomically release a lock held by the specified token.
        
        Args:
            key: Lock key.
            value: Lock owner token.
            
        Returns:
            True if released, False if not held by this token.
        """
        self._validate_key(key)
        return self._db.unlock(key, value.encode('utf-8'))

    import contextlib

    @contextlib.contextmanager
    def lock(self, key: str, expire: str | timedelta = "30s"):
        """Context manager for distributed locking.
        
        Blocks indefinitely until the lock is acquired. Automatically releases
        the lock when exiting the context block.
        
        Args:
            key: Lock key.
            expire: Max duration to hold the lock (auto-release).
        """
        import uuid
        import time
        token = str(uuid.uuid4())
        
        # Poll until we acquire the lock
        while not self.set_lock(key, token, expire):
            time.sleep(0.1)
            
        try:
            yield
        finally:
            self.unlock(key, token)

    def delete_tags(self, *tags: str) -> None:
        """Physically delete all cache entries that contain all of the specified tags.

        Args:
            *tags: Tags to match.
        """
        if tags:
            self._db.delete_by_tags(list(tags))

    def get_many(self, keys: list[str]) -> dict[str, bytes]:
        """Retrieve multiple cached values by their keys.
        
        Args:
            keys: List of cache keys.
            
        Returns:
            Dictionary mapping found keys to their byte values. Missing keys are omitted.
        """
        for key in keys:
            self._validate_key(key)
        return self._db.get_many(keys)

    def set_many(
        self,
        mapping: dict[str, bytes],
        options: EntryOptions | None = None,
    ) -> None:
        """Store multiple values in the cache.
        
        Args:
            mapping: Dictionary of key-value pairs to store.
            options: Per-entry TTL options applied to all items.
        """
        for key in mapping:
            self._validate_key(key)
        self._db.set_many(mapping, options)

    def delete_many(self, keys: list[str]) -> None:
        """Physically delete multiple cache entries by their keys.
        
        Args:
            keys: List of cache keys to delete.
        """
        for key in keys:
            self._validate_key(key)
        self._db.delete_many(keys)

    def get_pattern(self, pattern: str) -> dict[str, bytes]:
        """Retrieve all cached values whose keys match a SQL LIKE pattern.
        
        Args:
            pattern: SQL LIKE pattern (e.g., 'user:%').
            
        Returns:
            Dictionary mapping found keys to their byte values.
        """
        if not pattern:
            raise ValueError("Pattern must not be empty.")
        return self._db.get_pattern(pattern)

    def delete_pattern(self, pattern: str) -> int:
        """Physically delete all cache entries whose keys match a SQL LIKE pattern.
        
        Args:
            pattern: SQL LIKE pattern (e.g., 'user:%').
            
        Returns:
            Number of deleted entries.
        """
        if not pattern:
            raise ValueError("Pattern must not be empty.")
        return self._db.delete_pattern(pattern)

    def get_or_create(
        self,
        key: str,
        factory: Callable[[], bytes],
        options: EntryOptions | None = None,
    ) -> bytes:
        """Retrieve a cached value, or compute and store it if absent.

        Protected against cache stampede via PostgreSQL advisory lock
        (BR-MIGRAR-002, DA-10). Only one concurrent caller executes
        ``factory`` for a given key — all others wait and then receive
        the result.

        ⚠️ RISK-006: Advisory lock requires an explicit transaction.
                     Handled internally by DatabaseOperations.get_or_create().

        Args:
            key:     Cache key (≤ 449 characters).
            factory: Zero-argument callable returning the bytes to cache.
                     Called at most once per key across concurrent callers.
            options: Per-entry TTL options applied when storing the result.

        Returns:
            Cached or freshly computed bytes.
        """
        self._validate_key(key)
        return self._db.get_or_create(key, factory, options)

    def cached(
        self,
        key: str,
        ttl: str | timedelta | None = None,
        tags: list[str] | None = None,
    ) -> Callable:
        """Decorator to cache the result of a synchronous function.

        Args:
            key: Format string for the cache key, e.g. "user:{user_id}".
            ttl: Time-to-live for the cache entry. Can be a timedelta or a duration string like "10m".
            tags: Optional list of tags for group invalidation.
        """
        def decorator(func: Callable) -> Callable:
            sig = inspect.signature(func)

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                resolved_key = key.format(**bound_args.arguments)
                self._validate_key(resolved_key)

                def factory() -> bytes:
                    result = func(*args, **kwargs)
                    return pickle.dumps(result)

                options = EntryOptions(sliding_expiration=ttl, tags=tags)
                cached_bytes = self.get_or_create(resolved_key, factory, options)
                return pickle.loads(cached_bytes)

            return wrapper
        return decorator

    def failover(
        self,
        key: str,
        ttl: str | timedelta | None = None,
        exceptions: tuple[type[Exception], ...] = (Exception,),
        tags: list[str] | None = None,
    ) -> Callable:
        """Decorator to return stale cache if the function raises an exception.
        
        Args:
            key: Format string for the cache key.
            ttl: Time-to-live for the cache entry.
            exceptions: Tuple of exception types to catch and failover.
            tags: Optional list of tags.
        """
        def decorator(func: Callable) -> Callable:
            sig = inspect.signature(func)

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                resolved_key = key.format(**bound_args.arguments)
                self._validate_key(resolved_key)

                def factory() -> bytes:
                    try:
                        result = func(*args, **kwargs)
                        return pickle.dumps(result)
                    except exceptions as e:
                        stale_bytes = self._db.get_stale(resolved_key)
                        if stale_bytes is not None:
                            logger.warning(
                                "failover(%r): function raised %r, returning stale cache.",
                                resolved_key, e
                            )
                            return stale_bytes
                        raise

                options = EntryOptions(sliding_expiration=ttl, tags=tags)
                cached_bytes = self.get_or_create(resolved_key, factory, options)
                return pickle.loads(cached_bytes)

            return wrapper
        return decorator

    def early(
        self,
        key: str,
        ttl: str | timedelta,
        early_ttl: str | timedelta,
        tags: list[str] | None = None,
    ) -> Callable:
        """Decorator to refresh cache in background before it expires.
        
        Args:
            key: Format string for the cache key.
            ttl: Total time-to-live for the cache entry.
            early_ttl: Background refresh is triggered when the age of the cache exceeds this.
            tags: Optional list of tags.
        """
        from ._utils import parse_duration
        
        parsed_ttl = parse_duration(ttl) if isinstance(ttl, str) else ttl
        parsed_early_ttl = parse_duration(early_ttl) if isinstance(early_ttl, str) else early_ttl
        
        if parsed_ttl is None or parsed_early_ttl is None:
            raise ValueError("early() requires both ttl and early_ttl")
            
        early_threshold = parsed_ttl - parsed_early_ttl

        def decorator(func: Callable) -> Callable:
            sig = inspect.signature(func)

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                resolved_key = key.format(**bound_args.arguments)
                self._validate_key(resolved_key)

                options = EntryOptions(sliding_expiration=ttl, tags=tags)

                val_and_ttl = self._db.get_with_ttl(resolved_key)
                if val_and_ttl is not None:
                    val_bytes, remaining = val_and_ttl
                    if remaining < early_threshold:
                        # Spawns a background thread to update the cache
                        def background_refresh():
                            try:
                                logger.debug("early(%r): background refreshing cache.", resolved_key)
                                new_result = func(*args, **kwargs)
                                self.set(resolved_key, pickle.dumps(new_result), options)
                            except Exception:
                                logger.exception("early(%r): background refresh failed.", resolved_key)
                                
                        threading.Thread(target=background_refresh, daemon=True).start()
                    return pickle.loads(val_bytes)

                def factory() -> bytes:
                    result = func(*args, **kwargs)
                    return pickle.dumps(result)

                cached_bytes = self.get_or_create(resolved_key, factory, options)
                return pickle.loads(cached_bytes)

            return wrapper
        return decorator
