"""
Integration tests for cache-postgres — Asynchronous parity validation suite.

These tests require a real PostgreSQL database.
Set the environment variable PGCACHE_TEST_DSN before running.
"""

import os
import asyncio
import uuid
from datetime import timedelta
from typing import AsyncGenerator

import pytest
import psycopg

from cache_postgres import AsyncPostgresCache, PostgresCacheOptions, EntryOptions

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DSN = os.environ.get("PGCACHE_TEST_DSN")
SKIP_REASON = (
    "Integration tests require a real PostgreSQL database. "
    "Set PGCACHE_TEST_DSN=postgresql://user:pass@host/db to enable."
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(DSN is None, reason=SKIP_REASON),
]


@pytest.fixture(scope="module", params=["asyncio"])
def anyio_backend(request):
    return request.param


@pytest.fixture(scope="session")
def dsn() -> str:
    if DSN is None:
        pytest.skip(SKIP_REASON)
    return DSN


@pytest.fixture(scope="session")
def base_options(dsn: str) -> PostgresCacheOptions:
    return PostgresCacheOptions(
        dsn=dsn,
        schema="pgcache_async_integration_tests",
        table="pgcache_async_integration_tests",
        create_if_not_exists=True,
        enable_expiration_scan=False,  # scanner disabled — tests control timing
    )


@pytest.fixture()
async def cache(base_options: PostgresCacheOptions) -> AsyncGenerator[AsyncPostgresCache, None]:
    async with AsyncPostgresCache(base_options) as c:
        yield c


def unique_key(prefix: str = "test-async") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


async def raw_query_async(dsn: str, sql: str, params: tuple = ()) -> list:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Get and Set (basic happy path)
# ---------------------------------------------------------------------------

class TestGetSetBasic:
    @pytest.mark.anyio
    async def test_set_and_get_roundtrip(self, cache: AsyncPostgresCache):
        key = unique_key("par01-roundtrip")
        value = b"hello async world"
        await cache.set(key, value, EntryOptions(sliding_expiration=timedelta(minutes=20)))
        assert await cache.get(key) == value

    @pytest.mark.anyio
    async def test_get_missing_key_returns_none(self, cache: AsyncPostgresCache):
        key = unique_key("par01-missing")
        assert await cache.get(key) is None

    @pytest.mark.anyio
    async def test_set_overwrites_existing_value(self, cache: AsyncPostgresCache):
        key = unique_key("par01-upsert")
        await cache.set(key, b"valor-1", EntryOptions(sliding_expiration=timedelta(minutes=20)))
        await cache.set(key, b"valor-2", EntryOptions(sliding_expiration=timedelta(minutes=20)))
        assert await cache.get(key) == b"valor-2"

    @pytest.mark.anyio
    async def test_binary_content_preserved(self, cache: AsyncPostgresCache):
        key = unique_key("par01-binary")
        value = b"\x00\x01\x02\xff\xfe\xfd"
        await cache.set(key, value, EntryOptions(sliding_expiration=timedelta(minutes=20)))
        assert await cache.get(key) == value


# ---------------------------------------------------------------------------
# Expiration (absolute and sliding)
# ---------------------------------------------------------------------------

class TestExpiration:
    @pytest.mark.anyio
    async def test_absolute_expiration_expires(self, cache: AsyncPostgresCache):
        key = unique_key("par02-abs")
        await cache.set(key, b"expiring-soon", EntryOptions(
            absolute_expiration_relative=timedelta(seconds=1)
        ))
        await asyncio.sleep(2)
        assert await cache.get(key) is None

    @pytest.mark.anyio
    async def test_sliding_expiration_renews_on_get(self, cache: AsyncPostgresCache):
        key = unique_key("par02-sliding")
        await cache.set(key, b"alive", EntryOptions(sliding_expiration=timedelta(seconds=4)))
        await asyncio.sleep(2)
        assert await cache.get(key) == b"alive"   # renews the clock
        await asyncio.sleep(2)
        assert await cache.get(key) == b"alive"

    @pytest.mark.anyio
    async def test_default_sliding_expiration_applied_when_entry_options_empty(
        self, cache: AsyncPostgresCache, base_options: PostgresCacheOptions, dsn: str
    ):
        key = unique_key("par02-default")
        await cache.set(key, b"default", None)

        rows = await raw_query_async(
            dsn,
            f"SELECT slidingexpirationinseconds FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        assert rows, "Row not found in database"
        sliding_secs = rows[0][0]
        assert sliding_secs == 1200


# ---------------------------------------------------------------------------
# Stampede protection (get_or_create + advisory lock)
# ---------------------------------------------------------------------------

class TestStampedeProtection:
    @pytest.mark.anyio
    async def test_factory_called_exactly_once_under_concurrency(self, cache: AsyncPostgresCache):
        key = unique_key("par03-stampede")
        factory_call_count = {"n": 0}
        lock = asyncio.Lock()

        async def factory() -> bytes:
            async with lock:
                factory_call_count["n"] += 1
            await asyncio.sleep(0.1)  # simulate slow computation
            return b"created-value"

        results = []
        errors = []

        async def worker():
            try:
                val = await cache.get_or_create(key, factory, EntryOptions(
                    sliding_expiration=timedelta(minutes=5)
                ))
                results.append(val)
            except Exception as e:
                errors.append(e)

        workers = [worker() for _ in range(5)]
        await asyncio.gather(*workers)

        assert not errors, f"Tasks raised exceptions: {errors}"
        assert factory_call_count["n"] == 1
        assert all(r == b"created-value" for r in results)


# ---------------------------------------------------------------------------
# Background expiration scanner task
# ---------------------------------------------------------------------------

class TestScannerBackground:
    @pytest.mark.anyio
    async def test_scanner_deletes_expired_entries(self, base_options: PostgresCacheOptions):
        opts = PostgresCacheOptions(
            dsn=base_options.dsn,
            schema=base_options.schema,
            table=base_options.table,
            create_if_not_exists=True,
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(minutes=5),
        )
        object.__setattr__(opts, "expiration_scan_interval", timedelta(seconds=1))

        key = unique_key("par04-scanner")

        async with AsyncPostgresCache(opts) as cache:
            await cache.set(key, b"will-be-deleted", EntryOptions(
                absolute_expiration_relative=timedelta(milliseconds=500)
            ))
            await asyncio.sleep(3)

        rows = await raw_query_async(
            base_options.dsn,
            f"SELECT COUNT(*) FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        count = rows[0][0]
        assert count == 0


# ---------------------------------------------------------------------------
# DDL auto-create
# ---------------------------------------------------------------------------

class TestDdlAutoCreate:
    @pytest.mark.anyio
    async def test_table_created_with_correct_columns(self, base_options: PostgresCacheOptions, dsn: str):
        async with AsyncPostgresCache(base_options):
            pass

        rows = await raw_query_async(
            dsn,
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (base_options.schema, base_options.table),
        )
        col_names = {r[0] for r in rows}
        required = {"id", "value", "expiresattime", "slidingexpirationinseconds", "absoluteexpiration"}
        assert required.issubset(col_names)


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------

class TestRefresh:
    @pytest.mark.anyio
    async def test_refresh_extends_lifetime(self, cache: AsyncPostgresCache):
        key = unique_key("par06-refresh")
        await cache.set(key, b"alive", EntryOptions(sliding_expiration=timedelta(seconds=3)))
        await asyncio.sleep(2)
        await cache.refresh(key)
        await asyncio.sleep(2)
        assert await cache.get(key) == b"alive"


# ---------------------------------------------------------------------------
# Remove
# ---------------------------------------------------------------------------

class TestRemove:
    @pytest.mark.anyio
    async def test_remove_deletes_row(self, cache: AsyncPostgresCache, base_options: PostgresCacheOptions, dsn: str):
        key = unique_key("par07-remove")
        await cache.set(key, b"to-delete", EntryOptions(sliding_expiration=timedelta(minutes=5)))
        await cache.remove(key)
        assert await cache.get(key) is None

        rows = await raw_query_async(
            dsn,
            f"SELECT COUNT(*) FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        assert rows[0][0] == 0


# ---------------------------------------------------------------------------
# Async Advanced Features (Bulk, Pattern, Tags, Counters, Locks, Decorators)
# ---------------------------------------------------------------------------

class TestAsyncAdvancedFeatures:
    @pytest.mark.anyio
    async def test_bulk_operations(self, cache: AsyncPostgresCache):
        k1, k2 = unique_key("bulk-1"), unique_key("bulk-2")
        mapping = {k1: b"data-1", k2: b"data-2"}

        await cache.set_many(mapping, EntryOptions(sliding_expiration=timedelta(minutes=5)))
        
        # Test get_many
        results = await cache.get_many([k1, k2, "non-existent"])
        assert results == {k1: b"data-1", k2: b"data-2"}

        # Test delete_many
        await cache.delete_many([k1, k2])
        assert await cache.get(k1) is None
        assert await cache.get(k2) is None

    @pytest.mark.anyio
    async def test_pattern_matching(self, cache: AsyncPostgresCache):
        prefix = unique_key("pat")
        k1 = f"{prefix}:1"
        k2 = f"{prefix}:2"
        other = unique_key("other")

        await cache.set(k1, b"val-1")
        await cache.set(k2, b"val-2")
        await cache.set(other, b"val-other")

        # Match pattern
        results = await cache.get_pattern(f"{prefix}:%")
        assert results == {k1: b"val-1", k2: b"val-2"}

        # Delete pattern
        deleted = await cache.delete_pattern(f"{prefix}:%")
        assert deleted == 2
        assert await cache.get(k1) is None
        assert await cache.get(k2) is None
        assert await cache.get(other) == b"val-other"

    @pytest.mark.anyio
    async def test_tag_invalidation(self, cache: AsyncPostgresCache):
        k1 = unique_key("tag-1")
        k2 = unique_key("tag-2")
        k3 = unique_key("tag-3")

        await cache.set(k1, b"t1", EntryOptions(tags=["red", "blue"]))
        await cache.set(k2, b"t2", EntryOptions(tags=["blue"]))
        await cache.set(k3, b"t3", EntryOptions(tags=["green"]))

        # Delete tags
        await cache.delete_tags("blue")
        assert await cache.get(k1) is None
        assert await cache.get(k2) is None
        assert await cache.get(k3) == b"t3"

    @pytest.mark.anyio
    async def test_atomic_counters(self, cache: AsyncPostgresCache):
        k = unique_key("counter")
        assert await cache.incr(k, 1) == 1
        assert await cache.incr(k, 5) == 6
        assert await cache.incr(k, -2) == 4

    @pytest.mark.anyio
    async def test_distributed_locks(self, cache: AsyncPostgresCache):
        k = unique_key("lock")
        token1 = "owner-1"
        token2 = "owner-2"

        # Acquire lock
        assert await cache.set_lock(k, token1, timedelta(seconds=10)) is True
        assert await cache.is_locked(k) is True

        # Conflicting acquire fails
        assert await cache.set_lock(k, token2, timedelta(seconds=10)) is False

        # Unlock with wrong token fails
        assert await cache.unlock(k, token2) is False
        assert await cache.is_locked(k) is True

        # Unlock with correct token succeeds
        assert await cache.unlock(k, token1) is True
        assert await cache.is_locked(k) is False

        # Context manager lock block
        async with cache.lock(k, timedelta(seconds=5)):
            assert await cache.is_locked(k) is True
        assert await cache.is_locked(k) is False

    @pytest.mark.anyio
    async def test_decorators(self, cache: AsyncPostgresCache):
        k_cached = unique_key("dec-cached")
        k_fail = unique_key("dec-fail")
        k_early = unique_key("dec-early")

        # 1. Test @cached
        call_count_cached = {"n": 0}

        @cache.cached(key=k_cached, ttl="5m")
        async def fn_cached(x: int) -> int:
            call_count_cached["n"] += 1
            return x * 2

        assert await fn_cached(10) == 20
        assert await fn_cached(10) == 20
        assert call_count_cached["n"] == 1  # cached

        # 2. Test @failover
        call_count_fail = {"n": 0}

        @cache.failover(key=k_fail, ttl=timedelta(seconds=1), exceptions=(RuntimeError,))
        async def fn_fail(x: int) -> str:
            call_count_fail["n"] += 1
            if call_count_fail["n"] == 2:
                raise RuntimeError("Service offline")
            return f"ok-{x}"

        assert await fn_fail(100) == "ok-100"  # Populates cache
        await asyncio.sleep(2)  # Wait for cache to expire, triggering fallback on the next call
        # Next call triggers exception, should fall back to stale cache
        assert await fn_fail(100) == "ok-100"
        assert call_count_fail["n"] == 2

        # 3. Test @early
        @cache.early(key=k_early, ttl="10m", early_ttl="7m")
        async def fn_early(x: int) -> int:
            return x + 1

        assert await fn_early(5) == 6
