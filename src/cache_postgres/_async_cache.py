"""
Async Public cache facade for cache-postgres.
"""

from __future__ import annotations

import logging
import asyncio
import functools
import inspect
import pickle
from datetime import timedelta
from typing import Callable, Awaitable

from ._async_db import AsyncDatabaseOperations
from ._options import EntryOptions, PostgresCacheOptions
from ._sql import SqlQueries

logger = logging.getLogger(__name__)

#: Maximum allowed key length
_MAX_KEY_LENGTH: int = 449


class AsyncPostgresCache:
    """Distributed async cache backed by PostgreSQL.

    Main public interface for asynchronous applications.
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
        self._db = AsyncDatabaseOperations(options=options, sql=self._sql)

        # Scanner task state
        self._stop_event: asyncio.Event = asyncio.Event()
        self._scanner_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "AsyncPostgresCache":
        """Start the background expiration scanner and return self."""
        self._start_scanner()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stop the background scanner gracefully."""
        await self.close()
        return None

    async def close(self) -> None:
        """Stop the background expiration scanner task gracefully.

        Signals the scanner to stop and waits for it to terminate (up to 5 s).
        Safe to call multiple times.
        """
        self._stop_event.set()
        if self._scanner_task is not None and not self._scanner_task.done():
            try:
                await asyncio.wait_for(self._scanner_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Scanner task did not stop within 5 seconds, cancelling.")
                self._scanner_task.cancel()
            except asyncio.CancelledError:
                pass
        self._scanner_task = None
        
        # Ensure database connection pool is closed
        await self._db.close()

    # ------------------------------------------------------------------
    # Scanner task
    # ------------------------------------------------------------------

    def _start_scanner(self) -> None:
        """Start the background expiration scanner task."""
        if not self._options.enable_expiration_scan:
            logger.debug("Expiration scanner is disabled (enable_expiration_scan=False).")
            return
        if self._scanner_task is not None and not self._scanner_task.done():
            return
            
        self._stop_event.clear()
        
        loop = asyncio.get_running_loop()
        self._scanner_task = loop.create_task(self._scanner_loop(), name="cache-postgres-scanner")
        logger.debug("Expiration scanner started (interval=%s).", self._options.expiration_scan_interval)

    async def _scanner_loop(self) -> None:
        """Background loop: delete expired entries at the configured interval."""
        interval_seconds = self._options.expiration_scan_interval.total_seconds()
        
        while not self._stop_event.is_set():
            try:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval_seconds)
                    break  # Event was set, stop scanning
                except asyncio.TimeoutError:
                    pass  # Timeout reached, do a scan
                
                if self._stop_event.is_set():
                    break
                    
                count = await self._db.delete_expired()
                if count > 0:
                    logger.debug("Scanner deleted %d expired entries.", count)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(
                    "Expiration scanner encountered an error. "
                    "Continuing — next scan in %s.",
                    self._options.expiration_scan_interval,
                )

    # ------------------------------------------------------------------
    # Key validation
    # ------------------------------------------------------------------

    def _validate_key(self, key: str) -> None:
        """Reject keys longer than 449 characters."""
        if not key:
            raise ValueError("Cache key must not be empty.")
        if len(key) > _MAX_KEY_LENGTH:
            raise ValueError(
                f"Cache key exceeds maximum length of {_MAX_KEY_LENGTH} characters "
                f"(got {len(key)})."
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get(self, key: str) -> bytes | None:
        """Retrieve a cached value by key."""
        self._validate_key(key)
        return await self._db.get(key)

    async def set(
        self,
        key: str,
        value: bytes,
        options: EntryOptions | None = None,
    ) -> None:
        """Store a value in the cache (insert or update)."""
        self._validate_key(key)
        await self._db.set(key, value, options)

    async def refresh(self, key: str) -> None:
        """Renew the sliding expiration of an entry without returning its value."""
        self._validate_key(key)
        await self._db.refresh(key)

    async def remove(self, key: str) -> None:
        """Physically delete a cache entry by key."""
        self._validate_key(key)
        await self._db.remove(key)

    # ------------------------------------------------------------------
    # Distributed Primitives (Counters & Locks)
    # ------------------------------------------------------------------

    async def incr(self, key: str, value: int = 1) -> int:
        """Atomically increment a counter."""
        self._validate_key(key)
        return await self._db.incr(key, value)

    async def is_locked(self, key: str, wait: float = 0, step: float = 0.5) -> bool:
        """Check if a lock is currently held, optionally polling until timeout."""
        self._validate_key(key)
        import asyncio
        import time
        start = time.time()
        while True:
            if not await self._db.is_locked(key):
                return False
            if wait <= 0 or (time.time() - start) >= wait:
                return True
            await asyncio.sleep(step)

    async def set_lock(self, key: str, value: str, expire: str | timedelta) -> bool:
        """Atomically acquire a lock."""
        self._validate_key(key)
        from datetime import datetime, timezone
        from ._utils import parse_duration
        
        utc_now = datetime.now(tz=timezone.utc)
        if isinstance(expire, str):
            delta = parse_duration(expire)
            expires_at = utc_now + delta
        else:
            expires_at = utc_now + expire
            
        return await self._db.set_lock(key, value.encode('utf-8'), expires_at)

    async def unlock(self, key: str, value: str) -> bool:
        """Atomically release a lock held by the specified token."""
        self._validate_key(key)
        return await self._db.unlock(key, value.encode('utf-8'))

    import contextlib

    @contextlib.asynccontextmanager
    async def lock(self, key: str, expire: str | timedelta = "30s"):
        """Context manager for distributed locking."""
        import uuid
        import asyncio
        token = str(uuid.uuid4())
        
        # Poll until we acquire the lock
        while not await self.set_lock(key, token, expire):
            await asyncio.sleep(0.1)
            
        try:
            yield
        finally:
            await self.unlock(key, token)

    async def delete_tags(self, *tags: str) -> None:
        """Physically delete all cache entries that contain all of the specified tags."""
        if tags:
            await self._db.delete_by_tags(list(tags))

    async def get_many(self, keys: list[str]) -> dict[str, bytes]:
        """Retrieve multiple cached values by their keys."""
        for key in keys:
            self._validate_key(key)
        return await self._db.get_many(keys)

    async def set_many(
        self,
        mapping: dict[str, bytes],
        options: EntryOptions | None = None,
    ) -> None:
        """Store multiple values in the cache."""
        for key in mapping:
            self._validate_key(key)
        await self._db.set_many(mapping, options)

    async def delete_many(self, keys: list[str]) -> None:
        """Physically delete multiple cache entries by their keys."""
        for key in keys:
            self._validate_key(key)
        await self._db.delete_many(keys)

    async def get_pattern(self, pattern: str) -> dict[str, bytes]:
        """Retrieve all cached values whose keys match a SQL LIKE pattern."""
        if not pattern:
            raise ValueError("Pattern must not be empty.")
        return await self._db.get_pattern(pattern)

    async def delete_pattern(self, pattern: str) -> int:
        """Physically delete all cache entries whose keys match a SQL LIKE pattern."""
        if not pattern:
            raise ValueError("Pattern must not be empty.")
        return await self._db.delete_pattern(pattern)

    async def get_or_create(
        self,
        key: str,
        factory: Callable[[], Awaitable[bytes]],
        options: EntryOptions | None = None,
    ) -> bytes:
        """Retrieve a cached value, or compute and store it if absent."""
        self._validate_key(key)
        return await self._db.get_or_create(key, factory, options)

    def cached(
        self,
        key: str,
        ttl: str | timedelta | None = None,
        tags: list[str] | None = None,
    ) -> Callable:
        """Decorator to cache the result of an asynchronous function."""
        def decorator(func: Callable) -> Callable:
            sig = inspect.signature(func)

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                resolved_key = key.format(**bound_args.arguments)
                self._validate_key(resolved_key)

                async def factory() -> bytes:
                    result = await func(*args, **kwargs)
                    return pickle.dumps(result)

                options = EntryOptions(sliding_expiration=ttl, tags=tags)
                cached_bytes = await self.get_or_create(resolved_key, factory, options)
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
        """Decorator to return stale cache if the function raises an exception."""
        def decorator(func: Callable) -> Callable:
            sig = inspect.signature(func)

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                resolved_key = key.format(**bound_args.arguments)
                self._validate_key(resolved_key)

                async def factory() -> bytes:
                    try:
                        result = await func(*args, **kwargs)
                        return pickle.dumps(result)
                    except exceptions as e:
                        stale_bytes = await self._db.get_stale(resolved_key)
                        if stale_bytes is not None:
                            logger.warning(
                                "failover(%r): function raised %r, returning stale cache.",
                                resolved_key, e
                            )
                            return stale_bytes
                        raise

                options = EntryOptions(sliding_expiration=ttl, tags=tags)
                cached_bytes = await self.get_or_create(resolved_key, factory, options)
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
        """Decorator to refresh cache in background before it expires."""
        from ._utils import parse_duration
        
        parsed_ttl = parse_duration(ttl) if isinstance(ttl, str) else ttl
        parsed_early_ttl = parse_duration(early_ttl) if isinstance(early_ttl, str) else early_ttl
        
        if parsed_ttl is None or parsed_early_ttl is None:
            raise ValueError("early() requires both ttl and early_ttl")
            
        early_threshold = parsed_ttl - parsed_early_ttl

        def decorator(func: Callable) -> Callable:
            sig = inspect.signature(func)

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                resolved_key = key.format(**bound_args.arguments)
                self._validate_key(resolved_key)

                options = EntryOptions(sliding_expiration=ttl, tags=tags)

                val_and_ttl = await self._db.get_with_ttl(resolved_key)
                if val_and_ttl is not None:
                    val_bytes, remaining = val_and_ttl
                    if remaining < early_threshold:
                        # Spawns an asyncio task to update the cache
                        async def background_refresh():
                            try:
                                logger.debug("early(%r): background refreshing cache.", resolved_key)
                                new_result = await func(*args, **kwargs)
                                await self.set(resolved_key, pickle.dumps(new_result), options)
                            except Exception:
                                logger.exception("early(%r): background refresh failed.", resolved_key)
                                
                        asyncio.create_task(background_refresh())
                    return pickle.loads(val_bytes)

                async def factory() -> bytes:
                    result = await func(*args, **kwargs)
                    return pickle.dumps(result)

                cached_bytes = await self.get_or_create(resolved_key, factory, options)
                return pickle.loads(cached_bytes)

            return wrapper
        return decorator
