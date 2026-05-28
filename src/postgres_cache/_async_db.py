"""
Async Database access layer for cache-postgres.
"""

from __future__ import annotations

import logging
import asyncio
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Callable, AsyncIterator, Awaitable

import psycopg
from psycopg_pool import AsyncConnectionPool

from ._options import EntryOptions, PostgresCacheOptions
from ._sql import SqlQueries

logger = logging.getLogger(__name__)


class AsyncDatabaseOperations:
    """All direct interactions with PostgreSQL via psycopg3 async API.

    Instantiated internally by AsyncPostgresCache — never by library consumers.
    Manages connections, DDL, CRUD, advisory lock, and expiration scan asynchronously.
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
        self._ddl_lock: asyncio.Lock = asyncio.Lock()

        # Connection Pool
        self._pool: AsyncConnectionPool | None = None
        if self._options.async_connection_factory is None and self._options.dsn:
            self._pool = AsyncConnectionPool(
                conninfo=self._options.dsn,
                min_size=self._options.pool_min_size,
                max_size=self._options.pool_max_size,
                open=False,
            )

    async def close(self) -> None:
        """Close the connection pool if managed by the library."""
        if self._pool is not None:
            await self._pool.close()

    @asynccontextmanager
    async def _get_connection(self) -> AsyncIterator[psycopg.AsyncConnection]:
        """Yield an async connection from the pool or the factory."""
        if self._pool is not None:
            await self._pool.open(wait=True)
            async with self._pool.connection() as conn:
                yield conn
        else:
            conn = await self._options.async_connection_factory()
            try:
                yield conn
            finally:
                pass  # Do not close factory connections

    async def ensure_table_exists(self) -> None:
        """Create the cache table and index if they do not exist yet."""
        if not self._options.create_if_not_exists:
            return
        if self._table_created:
            return
        async with self._ddl_lock:
            if self._table_created:
                return
            async with self._get_connection() as conn:
                try:
                    async with conn.transaction():
                        async with conn.cursor() as cur:
                            await cur.execute(self._sql.create_schema)
                            await cur.execute(self._sql.create_table)
                            await cur.execute(self._sql.create_index)
                            await cur.execute(self._sql.create_tags_index)
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

    async def get(self, key: str) -> bytes | None:
        """Retrieve a cache value by key, renewing sliding expiration atomically."""
        await self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)

        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.get_item, (utc_now, key, utc_now))
                        row = await cur.fetchone()
                return row[0] if row else None
            except Exception:
                logger.exception("get(%r) failed.", key)
                raise

    async def get_stale(self, key: str) -> bytes | None:
        """Retrieve a cached value regardless of its expiration status."""
        await self.ensure_table_exists()
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.get_stale_item, (key,))
                        row = await cur.fetchone()
                return row[0] if row else None
            except Exception:
                logger.exception("get_stale(%r) failed.", key)
                raise

    async def get_with_ttl(self, key: str) -> tuple[bytes, timedelta] | None:
        """Retrieve a cached value and its remaining TTL."""
        await self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.get_item_with_ttl, (utc_now, key, utc_now))
                        row = await cur.fetchone()
                if row:
                    val: bytes = row[0]
                    expires_at: datetime = row[1]
                    remaining = expires_at - utc_now
                    return val, remaining
                return None
            except Exception:
                logger.exception("get_with_ttl(%r) failed.", key)
                raise

    async def set(
        self,
        key: str,
        value: bytes,
        options: EntryOptions | None = None,
    ) -> None:
        """Insert or update a cache entry (UPSERT via CTE ON CONFLICT)."""
        await self.ensure_table_exists()
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

        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            self._sql.set_item,
                            (key, value, expires_at, sliding_secs, abs_exp, options.tags),
                        )
            except Exception:
                logger.exception("set(%r) failed.", key)
                raise

    async def refresh(self, key: str) -> None:
        """Renew the sliding expiration of a cache entry without returning its value."""
        await self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)

        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.refresh_item, (utc_now, key, utc_now))
            except Exception:
                logger.exception("refresh(%r) failed.", key)
                raise

    async def remove(self, key: str) -> None:
        """Physically delete a cache entry by key."""
        await self.ensure_table_exists()
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.remove_item, (key,))
            except Exception:
                logger.exception("remove(%r) failed.", key)
                raise

    async def delete_by_tags(self, tags: list[str]) -> None:
        """Physically delete all cache entries that contain the specified tags."""
        if not tags:
            return
            
        await self.ensure_table_exists()
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.delete_by_tags, (tags,))
            except Exception:
                logger.exception("delete_by_tags(%r) failed.", tags)
                raise

    # ------------------------------------------------------------------
    # Bulk Operations
    # ------------------------------------------------------------------

    async def get_many(self, keys: list[str]) -> dict[str, bytes]:
        """Retrieve multiple cache values by key in a single round-trip."""
        await self.ensure_table_exists()
        if not keys:
            return {}
        utc_now = datetime.now(tz=timezone.utc)
        
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.get_many_items, (utc_now, keys, utc_now))
                        rows = await cur.fetchall()
                return {row[0]: row[1] for row in rows}
            except Exception:
                logger.exception("get_many(%r) failed.", keys)
                raise

    async def set_many(self, mapping: dict[str, bytes], options: EntryOptions | None = None) -> None:
        """Insert or update multiple cache entries in a single round-trip."""
        await self.ensure_table_exists()
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

        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        params = [
                            (key, value, expires_at, sliding_secs, abs_exp, options.tags)
                            for key, value in mapping.items()
                        ]
                        await cur.executemany(self._sql.set_item, params)
            except Exception:
                logger.exception("set_many() failed.")
                raise

    async def delete_many(self, keys: list[str]) -> None:
        """Physically delete multiple cache entries by key in a single round-trip."""
        await self.ensure_table_exists()
        if not keys:
            return
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.delete_many_items, (keys,))
            except Exception:
                logger.exception("delete_many(%r) failed.", keys)
                raise

    # ------------------------------------------------------------------
    # incr / lock
    # ------------------------------------------------------------------

    async def incr(self, key: str, value: int = 1) -> int:
        """Atomic counter increment."""
        await self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            self._sql.increment_item, 
                            (key, value, utc_now, value, utc_now)
                        )
                        row = await cur.fetchone()
                return int(row[0]) if row else value
            except Exception:
                logger.exception("async incr(%r) failed.", key)
                raise

    async def set_lock(self, key: str, value: bytes, expire: datetime) -> bool:
        """Atomically acquire a lock."""
        await self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            self._sql.set_lock_item,
                            (key, value, expire, utc_now)
                        )
                        return cur.rowcount > 0
            except Exception:
                logger.exception("async set_lock(%r) failed.", key)
                raise

    async def unlock(self, key: str, value: bytes) -> bool:
        """Atomically release a lock."""
        await self.ensure_table_exists()
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.unlock_item, (key, value))
                        return cur.rowcount > 0
            except Exception:
                logger.exception("async unlock(%r) failed.", key)
                raise

    async def is_locked(self, key: str) -> bool:
        """Check if a lock is currently held."""
        await self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.is_locked_item, (key, utc_now))
                        row = await cur.fetchone()
                        return row is not None
            except Exception:
                logger.exception("async is_locked(%r) failed.", key)
                raise

    # ------------------------------------------------------------------
    # Pattern Matching
    # ------------------------------------------------------------------

    async def get_pattern(self, pattern: str) -> dict[str, bytes]:
        """Retrieve cache values matching a SQL LIKE pattern."""
        await self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)
        
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.get_by_pattern, (utc_now, pattern, utc_now))
                        rows = await cur.fetchall()
                return {row[0]: row[1] for row in rows}
            except Exception:
                logger.exception("get_pattern(%r) failed.", pattern)
                raise

    async def delete_pattern(self, pattern: str) -> int:
        """Delete cache entries matching a SQL LIKE pattern."""
        await self.ensure_table_exists()
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.delete_by_pattern, (pattern,))
                        return cur.rowcount
            except Exception:
                logger.exception("delete_pattern(%r) failed.", pattern)
                raise

    async def delete_expired(self) -> int:
        """Delete all expired entries in a single batch statement."""
        utc_now = datetime.now(tz=timezone.utc)
        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql.delete_expired, (utc_now,))
                        count = cur.rowcount
                if count > 0:
                    logger.debug("Deleted %d expired cache entries.", count)
                return count
            except Exception:
                logger.exception("delete_expired() failed.")
                raise

    async def get_or_create(
        self,
        key: str,
        factory: Callable[[], Awaitable[bytes]],
        options: EntryOptions | None = None,
    ) -> bytes:
        """Get a value by key, or call factory() exactly once if absent."""
        await self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)

        async with self._get_connection() as conn:
            try:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        # Step 1: acquire advisory lock (blocks until available)
                        await cur.execute(self._sql.advisory_lock, (key,))

                        # Step 2: double-check after acquiring lock
                        await cur.execute(self._sql.get_item_after_lock, (key, utc_now))
                        row = await cur.fetchone()

                    if row is not None:
                        # Another worker already populated the entry
                        return row[0]

                    # Step 3: cache miss even after lock — call factory once
                    value = await factory()

                    # Step 4: store the computed value
                    async with conn.cursor() as cur:
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

                        await cur.execute(
                            self._sql.set_item,
                            (key, value, expires_at, sliding_secs, abs_exp, options.tags),
                        )

                    return value

            except Exception:
                logger.exception("get_or_create(%r) failed.", key)
                raise
