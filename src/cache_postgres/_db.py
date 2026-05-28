"""
Database access layer for cache-postgres.

Critical invariants enforced here:
  - All datetimes use datetime.now(tz=timezone.utc) — NEVER naive.
  - pg_advisory_xact_lock MUST run inside an explicit transaction
    (conn.autocommit = False before the call).
  - DDL executed at most once per instance (double-checked locking
    via threading.Lock + _table_created flag).
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from typing import Callable, Iterator

import psycopg
from psycopg_pool import ConnectionPool

from ._options import EntryOptions, PostgresCacheOptions
from ._sql import SqlQueries

logger = logging.getLogger(__name__)


class DatabaseOperations:
    """All direct interactions with PostgreSQL via psycopg.

    Instantiated internally by PostgresCache — never by library consumers.
    Manages connections, DDL, CRUD, advisory lock, and expiration scan.
    """

    def __init__(self, options: PostgresCacheOptions, sql: SqlQueries) -> None:
        """
        Args:
            options: Validated PostgresCacheOptions.
            sql:     Pre-formatted SqlQueries for this schema/table pair.
        """
        self._options = options
        self._sql = sql

        # Double-checked locking for DDL
        self._table_created: bool = False
        self._ddl_lock: threading.Lock = threading.Lock()

        # Connection Pool
        self._pool: ConnectionPool | None = None
        if self._options.connection_factory is None and self._options.dsn:
            self._pool = ConnectionPool(
                conninfo=self._options.dsn,
                min_size=self._options.pool_min_size,
                max_size=self._options.pool_max_size,
                open=True,
            )

    def close(self) -> None:
        """Close the connection pool if managed by the library."""
        if self._pool is not None:
            self._pool.close()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    @contextmanager
    def _get_connection(self) -> Iterator[psycopg.Connection]:
        """Yield a connection from the pool or the factory.
        
        The caller is responsible for committing or rolling back their
        transactions (e.g. via `with conn.transaction():` or explicitly).
        """
        if self._pool is not None:
            with self._pool.connection() as conn:
                yield conn
        else:
            conn = self._options.connection_factory()
            try:
                yield conn
            finally:
                pass  # Do not close factory connections

    # ------------------------------------------------------------------
    # DDL (double-checked locking)
    # ------------------------------------------------------------------

    def ensure_table_exists(self) -> None:
        """Create the cache table and index if they do not exist yet.

        Uses double-checked locking so DDL is attempted at most once per
        instance, even under concurrent calls.

        Only runs when options.create_if_not_exists is True.
        """
        if not self._options.create_if_not_exists:
            return
        if self._table_created:
            return
        with self._ddl_lock:
            if self._table_created:
                return
            with self._get_connection() as conn:
                try:
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute(self._sql.create_schema)
                            cur.execute(self._sql.create_table)
                            cur.execute(self._sql.create_index)
                            cur.execute(self._sql.create_tags_index)
                    self._table_created = True
                    logger.debug(
                        "Cache table '%s.%s' ensured.",
                        self._options.schema,
                        self._options.table,
                    )
                except Exception:
                    logger.exception(
                        "Failed to create cache table '%s.%s'.",
                        self._options.schema,
                        self._options.table,
                    )
                    raise

    # ------------------------------------------------------------------
    # get
    # ------------------------------------------------------------------

    def get(self, key: str) -> bytes | None:
        """Retrieve a cache value by key, renewing sliding expiration atomically.

        Returns None if the key does not exist or has expired.
        Sliding expiration is recalculated in-database in the same UPDATE
        statement (single round-trip).

        Args:
            key: Cache key (≤ 449 characters).

        Returns:
            Cached bytes, or None on miss / expiry.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)

        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.get_item, (utc_now, key, utc_now))
                        row = cur.fetchone()
                return row[0] if row else None
            except Exception:
                logger.exception("get(%r) failed.", key)
                raise

    def get_stale(self, key: str) -> bytes | None:
        """Retrieve a cached value regardless of its expiration status.

        Returns:
            bytes if found, None if completely absent.
        """
        self.ensure_table_exists()
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.get_stale_item, (key,))
                        row = cur.fetchone()
                return row[0] if row else None
            except Exception:
                logger.exception("get_stale(%r) failed.", key)
                raise

    def get_with_ttl(self, key: str) -> tuple[bytes, timedelta] | None:
        """Retrieve a cached value and its remaining TTL.

        Returns:
            Tuple of (bytes, remaining timedelta) or None if absent/expired.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.get_item_with_ttl, (utc_now, key, utc_now))
                        row = cur.fetchone()
                if row:
                    val: bytes = row[0]
                    expires_at: datetime = row[1]
                    remaining = expires_at - utc_now
                    return val, remaining
                return None
            except Exception:
                logger.exception("get_with_ttl(%r) failed.", key)
                raise

    def incr(self, key: str, value: int = 1) -> int:
        """Atomic counter increment.
        
        Args:
            key: Cache key.
            value: Integer value to increment by.
            
        Returns:
            The new incremented value.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(
                            self._sql.increment_item, 
                            (key, value, utc_now, value, value, utc_now)
                        )
                        row = cur.fetchone()
                return int(row[0]) if row else value
            except Exception:
                logger.exception("incr(%r) failed.", key)
                raise

    def set_lock(self, key: str, value: bytes, expire: datetime) -> bool:
        """Atomically acquire a lock.
        
        Returns:
            True if the lock was acquired, False otherwise.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(
                            self._sql.set_lock_item,
                            (key, value, expire, utc_now)
                        )
                        return cur.rowcount > 0
            except Exception:
                logger.exception("set_lock(%r) failed.", key)
                raise

    def unlock(self, key: str, value: bytes) -> bool:
        """Atomically release a lock.
        
        Returns:
            True if the lock was released, False if it was already released or held by another owner.
        """
        self.ensure_table_exists()
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.unlock_item, (key, value))
                        return cur.rowcount > 0
            except Exception:
                logger.exception("unlock(%r) failed.", key)
                raise

    def is_locked(self, key: str) -> bool:
        """Check if a lock is currently held.
        
        Returns:
            True if locked, False otherwise.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.is_locked_item, (key, utc_now))
                        return cur.fetchone() is not None
            except Exception:
                logger.exception("is_locked(%r) failed.", key)
                raise

    def set(
        self,
        key: str,
        value: bytes,
        options: EntryOptions | None = None,
    ) -> None:
        """Insert or update a cache entry (UPSERT via CTE ON CONFLICT).

        Computes expiration timestamps at call time using UTC now. All
        timestamps are timezone-aware UTC.

        Args:
            key:     Cache key.
            value:   Raw bytes to store.
            options: Per-entry TTL options. When None, the default sliding
                     expiration from PostgresCacheOptions is applied.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)

        if options is None:
            options = EntryOptions(
                sliding_expiration=self._options.default_sliding_expiration
            )

        sliding_secs = options.resolve_sliding_seconds()
        abs_exp = options.resolve_expires_at(now=utc_now)

        # Compute expires_at: sliding from now, capped by absolute
        if sliding_secs is not None:
            from datetime import timedelta
            expires_at = utc_now + timedelta(seconds=sliding_secs)
            if abs_exp is not None and abs_exp < expires_at:
                expires_at = abs_exp
        elif abs_exp is not None:
            expires_at = abs_exp
        else:
            # No expiration configured — set far future sentinel
            from datetime import timedelta
            expires_at = utc_now + timedelta(days=365 * 100)

        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(
                            self._sql.set_item,
                            (key, value, expires_at, sliding_secs, abs_exp, options.tags),
                        )
            except Exception:
                logger.exception("set(%r) failed.", key)
                raise

    def refresh(self, key: str) -> None:
        """Renew the sliding expiration of a cache entry without returning its value.

        If the entry is expired or absent, this is a no-op.

        Args:
            key: Cache key.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)

        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.refresh_item, (utc_now, key, utc_now))
            except Exception:
                logger.exception("refresh(%r) failed.", key)
                raise

    def remove(self, key: str) -> None:
        """Physically delete a cache entry by key.

        Args:
            key: Cache key.
        """
        self.ensure_table_exists()
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.remove_item, (key,))
            except Exception:
                logger.exception("remove(%r) failed.", key)
                raise

    def delete_by_tags(self, tags: list[str]) -> None:
        """Physically delete all cache entries that contain the specified tags.

        Args:
            tags: List of tags to match.
        """
        if not tags:
            return
            
        self.ensure_table_exists()
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.delete_by_tags, (tags,))
            except Exception:
                logger.exception("delete_by_tags(%r) failed.", tags)
                raise

    # ------------------------------------------------------------------
    # Bulk Operations
    # ------------------------------------------------------------------

    def get_many(self, keys: list[str]) -> dict[str, bytes]:
        """Retrieve multiple cache values by key in a single round-trip."""
        self.ensure_table_exists()
        if not keys:
            return {}
        utc_now = datetime.now(tz=timezone.utc)
        
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.get_many_items, (utc_now, keys, utc_now))
                        rows = cur.fetchall()
                return {row[0]: row[1] for row in rows}
            except Exception:
                logger.exception("get_many(%r) failed.", keys)
                raise

    def set_many(self, mapping: dict[str, bytes], options: EntryOptions | None = None) -> None:
        """Insert or update multiple cache entries in a single round-trip."""
        self.ensure_table_exists()
        if not mapping:
            return
        utc_now = datetime.now(tz=timezone.utc)

        if options is None:
            options = EntryOptions(
                sliding_expiration=self._options.default_sliding_expiration
            )

        sliding_secs = options.resolve_sliding_seconds()
        abs_exp = options.resolve_expires_at(now=utc_now)

        if sliding_secs is not None:
            from datetime import timedelta
            expires_at = utc_now + timedelta(seconds=sliding_secs)
            if abs_exp is not None and abs_exp < expires_at:
                expires_at = abs_exp
        elif abs_exp is not None:
            expires_at = abs_exp
        else:
            from datetime import timedelta
            expires_at = utc_now + timedelta(days=365 * 100)

        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        params = [
                            (key, value, expires_at, sliding_secs, abs_exp, options.tags)
                            for key, value in mapping.items()
                        ]
                        cur.executemany(self._sql.set_item, params)
            except Exception:
                logger.exception("set_many() failed.")
                raise

    def delete_many(self, keys: list[str]) -> None:
        """Physically delete multiple cache entries by key in a single round-trip."""
        self.ensure_table_exists()
        if not keys:
            return
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.delete_many_items, (keys,))
            except Exception:
                logger.exception("delete_many(%r) failed.", keys)
                raise

    # ------------------------------------------------------------------
    # Pattern Matching
    # ------------------------------------------------------------------

    def get_pattern(self, pattern: str) -> dict[str, bytes]:
        """Retrieve cache values matching a SQL LIKE pattern."""
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.get_by_pattern, (utc_now, pattern, utc_now))
                        rows = cur.fetchall()
                return {row[0]: row[1] for row in rows}
            except Exception:
                logger.exception("get_pattern(%r) failed.", pattern)
                raise

    def delete_pattern(self, pattern: str) -> int:
        """Delete cache entries matching a SQL LIKE pattern."""
        self.ensure_table_exists()
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.delete_by_pattern, (pattern,))
                        return cur.rowcount
            except Exception:
                logger.exception("delete_pattern(%r) failed.", pattern)
                raise

    # ------------------------------------------------------------------
    # Expiration Cleanup
    # ------------------------------------------------------------------

    def delete_expired(self) -> int:
        """Delete all expired entries in a single batch statement.

        Called by the background scanner thread in PostgresCache.

        Returns:
            Number of rows deleted.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(self._sql.delete_expired, (utc_now,))
                        count = cur.rowcount
                if count > 0:
                    logger.debug("Deleted %d expired cache entries.", count)
                return count
            except Exception:
                logger.exception("delete_expired() failed.")
                raise

    # ------------------------------------------------------------------
    # get_or_create (stampede protection via advisory lock)
    # ------------------------------------------------------------------

    def get_or_create(
        self,
        key: str,
        factory: Callable[[], bytes],
        options: EntryOptions | None = None,
    ) -> bytes:
        """Get a value by key, or call factory() exactly once if absent.

        Uses pg_advisory_xact_lock to prevent cache stampede: only one
        concurrent caller executes the factory for a given key.

        The lock is transactional — it releases on COMMIT/ROLLBACK.
        All timestamps are tz-aware UTC.

        Implementation pattern:
          1. Open connection, acquire transactional advisory lock on hash(key).
          2. Double-check: re-read after lock (another worker may have set it).
          3. If still miss: call factory(), write result, return it.
          4. Commit (releases advisory lock).

        Args:
            key:     Cache key.
            factory: Zero-argument callable that produces the value bytes.
            options: Per-entry TTL options.

        Returns:
            The cached or freshly computed bytes value.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)

        with self._get_connection() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        # Step 1: acquire advisory lock (blocks until available)
                        cur.execute(self._sql.advisory_lock, (key,))

                        # Step 2: double-check after acquiring lock
                        cur.execute(self._sql.get_item_after_lock, (key, utc_now))
                        row = cur.fetchone()

                    if row is not None:
                        # Another worker already populated the entry
                        return row[0]

                    # Step 3: cache miss even after lock — call factory once
                    value = factory()

                    # Step 4: store the computed value
                    with conn.cursor() as cur:
                        options = options or EntryOptions(
                            sliding_expiration=self._options.default_sliding_expiration
                        )
                        sliding_secs = options.resolve_sliding_seconds()
                        abs_exp = options.resolve_expires_at(now=utc_now)
                        
                        if sliding_secs is not None:
                            from datetime import timedelta
                            expires_at = utc_now + timedelta(seconds=sliding_secs)
                            if abs_exp is not None and abs_exp < expires_at:
                                expires_at = abs_exp
                        elif abs_exp is not None:
                            expires_at = abs_exp
                        else:
                            from datetime import timedelta
                            expires_at = utc_now + timedelta(days=365 * 100)

                        cur.execute(
                            self._sql.set_item,
                            (key, value, expires_at, sliding_secs, abs_exp, options.tags),
                        )

                    return value

            except Exception:
                logger.exception("get_or_create(%r) failed.", key)
                raise
