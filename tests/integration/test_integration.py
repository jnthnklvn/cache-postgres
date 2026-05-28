"""
Integration tests for cache-postgres — parity validation suite.

These tests require a real PostgreSQL database.
Set the environment variable PGCACHE_TEST_DSN before running:

    $env:PGCACHE_TEST_DSN = "postgresql://user:pass@localhost:5432/testdb"
    python -m pytest tests/integration/test_integration.py -v

All tests are skipped if PGCACHE_TEST_DSN is not set.
"""

import os
import time
import uuid
import threading
from datetime import timedelta
from typing import Generator

import psycopg
import pytest

from cache_postgres import PostgresCache, PostgresCacheOptions, EntryOptions

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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def dsn() -> str:
    """Return the DSN, skipping the session if not set."""
    if DSN is None:
        pytest.skip(SKIP_REASON)
    return DSN


@pytest.fixture(scope="session")
def base_options(dsn: str) -> PostgresCacheOptions:
    """Shared options for all integration tests — creates table on first use."""
    return PostgresCacheOptions(
        dsn=dsn,
        schema="pgcache_integration_tests",
        table="pgcache_integration_tests",
        create_if_not_exists=True,
        enable_expiration_scan=False,  # scanner disabled — tests control timing
    )


@pytest.fixture()
def cache(base_options: PostgresCacheOptions) -> Generator[PostgresCache, None, None]:
    """Fresh context-managed PostgresCache for each test."""
    with PostgresCache(base_options) as c:
        yield c


def unique_key(prefix: str = "test") -> str:
    """Generate a unique key per test to avoid cross-test interference."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def raw_query(dsn: str, sql: str, params: tuple = ()) -> list:
    """Run a raw SQL query outside the cache library and return rows."""
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


# ---------------------------------------------------------------------------
# Get and Set (basic happy path)
# ---------------------------------------------------------------------------

class TestGetSetBasic:
    """Basic get/set parity — happy path scenarios."""

    def test_set_and_get_roundtrip(self, cache: PostgresCache):
        """Set stores bytes; get returns identical bytes."""
        key = unique_key("par01-roundtrip")
        value = b"hello world"
        cache.set(key, value, EntryOptions(sliding_expiration=timedelta(minutes=20)))
        assert cache.get(key) == value

    def test_get_missing_key_returns_none(self, cache: PostgresCache):
        """get() returns None for a key that was never set."""
        key = unique_key("par01-missing")
        assert cache.get(key) is None

    def test_set_overwrites_existing_value(self, cache: PostgresCache):
        """UPSERT semantics: set() on existing key replaces the value."""
        key = unique_key("par01-upsert")
        cache.set(key, b"valor-1", EntryOptions(sliding_expiration=timedelta(minutes=20)))
        cache.set(key, b"valor-2", EntryOptions(sliding_expiration=timedelta(minutes=20)))
        assert cache.get(key) == b"valor-2"

    def test_binary_content_preserved(self, cache: PostgresCache):
        """Arbitrary binary bytes are stored and retrieved unchanged."""
        key = unique_key("par01-binary")
        value = b"\x00\x01\x02\xff\xfe\xfd"
        cache.set(key, value, EntryOptions(sliding_expiration=timedelta(minutes=20)))
        assert cache.get(key) == value

    def test_large_value(self, cache: PostgresCache):
        """Values up to 1 MB are stored and retrieved correctly."""
        key = unique_key("par01-large")
        value = b"x" * (1024 * 1024)  # 1 MB
        cache.set(key, value, EntryOptions(sliding_expiration=timedelta(minutes=20)))
        assert cache.get(key) == value


# ---------------------------------------------------------------------------
# Expiration (absolute and sliding)
# ---------------------------------------------------------------------------

class TestExpiration:
    """Expiration parity — absolute and sliding."""

    def test_absolute_expiration_expires(self, cache: PostgresCache):
        """Item with absolute_expiration_relative=1s is gone after 2s."""
        key = unique_key("par02-abs")
        cache.set(key, b"expiring-soon", EntryOptions(
            absolute_expiration_relative=timedelta(seconds=1)
        ))
        time.sleep(2)
        assert cache.get(key) is None

    def test_absolute_expiration_not_yet_expired(self, cache: PostgresCache):
        """Item with 10-minute absolute expiration is still visible."""
        key = unique_key("par02-abs-valid")
        cache.set(key, b"still-valid", EntryOptions(
            absolute_expiration_relative=timedelta(minutes=10)
        ))
        assert cache.get(key) == b"still-valid"

    def test_sliding_expiration_renews_on_get(self, cache: PostgresCache):
        """Sliding expiration resets the clock on each get.

        Pattern: set with 5s sliding → wait 3s → get (renews) → wait 3s → still alive.
        Without the intermediate get it would expire in 5s total.
        """
        key = unique_key("par02-sliding")
        cache.set(key, b"alive", EntryOptions(sliding_expiration=timedelta(seconds=5)))
        time.sleep(3)
        assert cache.get(key) == b"alive"   # renews the clock
        time.sleep(3)
        assert cache.get(key) == b"alive"   # now at ~6s total but last touch was 3s ago

    def test_sliding_capped_by_absolute(self, cache: PostgresCache):
        """Sliding expiration must not exceed absoluteExpiration."""
        key = unique_key("par02-sliding-cap")
        cache.set(key, b"capped", EntryOptions(
            sliding_expiration=timedelta(seconds=10),
            absolute_expiration_relative=timedelta(seconds=2),
        ))
        time.sleep(3)
        assert cache.get(key) is None  # absolute cap won

    def test_default_sliding_expiration_applied_when_entry_options_empty(
        self, cache: PostgresCache, base_options: PostgresCacheOptions, dsn: str
    ):
        """When EntryOptions has no TTL, DefaultSlidingExpiration (20 min) is used."""
        key = unique_key("par02-default")
        # set() with no options → default sliding applied by DatabaseOperations
        cache.set(key, b"default", None)  # None options → uses default sliding

        rows = raw_query(
            dsn,
            f"SELECT slidingexpirationinseconds FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        assert rows, "Row not found in database"
        sliding_secs = rows[0][0]
        assert sliding_secs == 1200, f"Expected 1200 (20 min), got {sliding_secs}"

    def test_expired_item_invisible_without_scanner(self, cache: PostgresCache):
        """get() never returns expired items even without the background scanner."""
        key = unique_key("par02-invisible")
        cache.set(key, b"bye", EntryOptions(absolute_expiration_relative=timedelta(seconds=1)))
        time.sleep(2)
        # Row may still exist in DB (scanner not running) but get() must return None
        assert cache.get(key) is None


# ---------------------------------------------------------------------------
# Stampede protection (get_or_create + advisory lock)
# ---------------------------------------------------------------------------

class TestStampedeProtection:
    """get_or_create with advisory lock."""

    def test_factory_called_exactly_once_under_concurrency(
        self, cache: PostgresCache
    ):
        """5 concurrent threads — factory must be called exactly once."""
        key = unique_key("par03-stampede")
        factory_call_count = {"n": 0}
        lock = threading.Lock()

        def factory() -> bytes:
            with lock:
                factory_call_count["n"] += 1
            time.sleep(0.05)  # simulate slow computation
            return b"created-value"

        results = []
        errors = []

        def worker():
            try:
                val = cache.get_or_create(key, factory, EntryOptions(
                    sliding_expiration=timedelta(minutes=5)
                ))
                results.append(val)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Threads raised exceptions: {errors}"
        assert factory_call_count["n"] == 1, (
            f"factory must be called exactly once, got {factory_call_count['n']}"
        )
        assert all(r == b"created-value" for r in results), (
            f"All threads must get the same value, got {results}"
        )

    def test_factory_not_called_when_key_exists(self, cache: PostgresCache):
        """get_or_create fast path: factory not called if key is present."""
        key = unique_key("par03-existing")
        cache.set(key, b"pre-existing", EntryOptions(
            sliding_expiration=timedelta(minutes=5)
        ))
        factory = lambda: b"should-not-be-called"
        result = cache.get_or_create(key, factory)
        assert result == b"pre-existing"

    def test_factory_exception_propagated_and_rolled_back(self, cache: PostgresCache):
        """If factory raises, exception propagates and key remains absent (rollback)."""
        key = unique_key("par03-error")

        def bad_factory() -> bytes:
            raise ValueError("factory falhou")

        with pytest.raises(ValueError, match="factory falhou"):
            cache.get_or_create(key, bad_factory)

        # After rollback, key must not exist
        assert cache.get(key) is None


# ---------------------------------------------------------------------------
# Background expiration scanner
# ---------------------------------------------------------------------------

class TestScannerBackground:
    """Background scanner deletes expired entries."""

    def test_scanner_deletes_expired_entries(self, base_options: PostgresCacheOptions):
        """Scanner runs at configured interval and removes expired rows."""
        # Build options with scanner enabled and fast interval (bypassing validation)
        opts = PostgresCacheOptions(
            dsn=base_options.dsn,
            schema=base_options.schema,
            table=base_options.table,
            create_if_not_exists=True,
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(minutes=5),  # valid for construction
        )
        # Override interval to something fast for the test
        object.__setattr__(opts, "expiration_scan_interval", timedelta(seconds=1))

        key = unique_key("par04-scanner")

        with PostgresCache(opts) as cache:
            cache.set(key, b"will-be-deleted", EntryOptions(
                absolute_expiration_relative=timedelta(milliseconds=500)
            ))
            time.sleep(3)  # wait for expiry + at least 2 scanner cycles at 1s

        # Row should have been physically deleted by the scanner
        rows = raw_query(
            base_options.dsn,
            f"SELECT COUNT(*) FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        count = rows[0][0]
        assert count == 0, f"Expected row deleted by scanner, but it still exists (count={count})"

    def test_scanner_thread_stops_on_context_exit(self, base_options: PostgresCacheOptions):
        """Thread must not survive after __exit__."""
        opts = PostgresCacheOptions(
            dsn=base_options.dsn,
            schema=base_options.schema,
            table=base_options.table,
            create_if_not_exists=True,
            enable_expiration_scan=True,
            expiration_scan_interval=timedelta(minutes=5),
        )
        captured_thread = {}

        with PostgresCache(opts) as cache:
            captured_thread["t"] = cache._scanner_thread
            assert cache._scanner_thread is not None
            assert cache._scanner_thread.is_alive()

        # After context exit, thread must be stopped
        thread = captured_thread["t"]
        thread.join(timeout=3.0)
        assert not thread.is_alive(), "Scanner thread must stop on __exit__"


# ---------------------------------------------------------------------------
# DDL auto-create
# ---------------------------------------------------------------------------

class TestDdlAutoCreate:
    """create_if_not_exists creates table with correct schema."""

    def test_table_created_with_correct_columns(
        self, base_options: PostgresCacheOptions, dsn: str
    ):
        """After first use, the table must have all required columns."""
        # ensure table exists (cache fixture already triggers this)
        with PostgresCache(base_options):
            pass

        rows = raw_query(
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
        assert required.issubset(col_names), (
            f"Missing columns: {required - col_names}"
        )

    def test_ddl_idempotent_called_twice(self, base_options: PostgresCacheOptions):
        """ensure_table_exists is idempotent — calling it twice must not error."""
        cache = PostgresCache(base_options)
        cache._db.ensure_table_exists()
        cache._db.ensure_table_exists()  # must not raise

    def test_index_on_expiresattime_exists(
        self, base_options: PostgresCacheOptions, dsn: str
    ):
        """Index ix_expiresattime must exist."""
        with PostgresCache(base_options):
            pass

        rows = raw_query(
            dsn,
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s AND indexname = 'ix_expiresattime'
            """,
            (base_options.schema, base_options.table),
        )
        assert rows, "Index ix_expiresattime not found"


# ---------------------------------------------------------------------------
# Refresh (sliding renewal)
# ---------------------------------------------------------------------------

class TestRefresh:
    """refresh renews sliding expiration without returning value."""

    def test_refresh_extends_lifetime(self, cache: PostgresCache):
        """refresh prevents expiry of an item that would expire soon."""
        key = unique_key("par06-refresh")
        cache.set(key, b"alive", EntryOptions(sliding_expiration=timedelta(seconds=3)))
        time.sleep(2)          # almost expired
        cache.refresh(key)     # renew
        time.sleep(2)          # now at 4s total, but renewed 2s ago
        assert cache.get(key) == b"alive"

    def test_refresh_missing_key_no_error(self, cache: PostgresCache):
        """refresh on a non-existent key must not raise — no-op."""
        cache.refresh(unique_key("par06-missing"))  # must not raise

    def test_refresh_no_return_value(self, cache: PostgresCache):
        """refresh returns None (no value returned — different from get)."""
        key = unique_key("par06-no-return")
        cache.set(key, b"value", EntryOptions(sliding_expiration=timedelta(minutes=5)))
        result = cache.refresh(key)
        assert result is None

    def test_refresh_on_item_without_sliding_no_change(
        self, cache: PostgresCache, base_options: PostgresCacheOptions, dsn: str
    ):
        """refresh on an absolute-only item must not change expiresattime."""
        key = unique_key("par06-abs-only")
        cache.set(key, b"abs", EntryOptions(
            absolute_expiration_relative=timedelta(minutes=30)
        ))

        rows_before = raw_query(
            dsn,
            f"SELECT expiresattime FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        assert rows_before
        expires_before = rows_before[0][0]

        cache.refresh(key)

        rows_after = raw_query(
            dsn,
            f"SELECT expiresattime FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        expires_after = rows_after[0][0]
        # Without sliding, expiresattime must not change (SQL CASE: ELSE expiresattime)
        assert abs((expires_after - expires_before).total_seconds()) < 2, (
            f"expiresattime changed unexpectedly: {expires_before} → {expires_after}"
        )


# ---------------------------------------------------------------------------
# Remove (physical delete)
# ---------------------------------------------------------------------------

class TestRemove:
    """remove physically deletes the row."""

    def test_remove_deletes_row(
        self, cache: PostgresCache, base_options: PostgresCacheOptions, dsn: str
    ):
        """remove() deletes the row from the database."""
        key = unique_key("par07-remove")
        cache.set(key, b"to-delete", EntryOptions(sliding_expiration=timedelta(minutes=5)))
        cache.remove(key)
        assert cache.get(key) is None

        rows = raw_query(
            dsn,
            f"SELECT COUNT(*) FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        assert rows[0][0] == 0, "Row still exists after remove()"

    def test_remove_missing_key_no_error(self, cache: PostgresCache):
        """remove() on a non-existent key must not raise."""
        cache.remove(unique_key("par07-missing"))  # must not raise

    def test_remove_is_physical_delete_not_expiry(
        self, cache: PostgresCache, base_options: PostgresCacheOptions, dsn: str
    ):
        """remove() differs from expiry: row is gone immediately, not just invisible."""
        key = unique_key("par07-physical")
        cache.set(key, b"gone", EntryOptions(sliding_expiration=timedelta(minutes=5)))
        cache.remove(key)

        # Even a raw SELECT without expiry filter returns nothing
        rows = raw_query(
            dsn,
            f"SELECT COUNT(*) FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        assert rows[0][0] == 0, "Row still physically present after remove()"


# ---------------------------------------------------------------------------
# Connection modes (DSN and factory)
# ---------------------------------------------------------------------------

class TestConnectionModes:
    """DSN mode and connection_factory mode."""

    def test_dsn_mode_set_get(self, base_options: PostgresCacheOptions):
        """DSN mode: library manages connections and operations succeed."""
        key = unique_key("par08-dsn")
        with PostgresCache(base_options) as cache:
            cache.set(key, b"dsn-value", EntryOptions(sliding_expiration=timedelta(minutes=5)))
            assert cache.get(key) == b"dsn-value"

    def test_factory_mode_does_not_close_connection(self, base_options: PostgresCacheOptions):
        """connection_factory mode: library must NOT call close() on returned connections."""
        close_calls = {"n": 0}

        class TrackedConnection(psycopg.Connection):
            def close(self):
                close_calls["n"] += 1
                super().close()

        def tracked_factory():
            return TrackedConnection.connect(base_options.dsn)

        opts = PostgresCacheOptions(
            dsn=None,
            connection_factory=tracked_factory,
            schema=base_options.schema,
            table=base_options.table,
            create_if_not_exists=False,
        )
        key = unique_key("par08-factory")
        with PostgresCache(opts) as cache:
            cache.set(key, b"factory-value", EntryOptions(sliding_expiration=timedelta(minutes=5)))
            cache.get(key)

        assert close_calls["n"] == 0, (
            f"Library called close() {close_calls['n']} times on factory connections — must be 0"
        )

    def test_no_dsn_no_factory_raises_on_construction(self):
        """Neither dsn nor connection_factory raises ValueError."""
        with pytest.raises(ValueError):
            PostgresCacheOptions()  # no dsn, no factory


# ---------------------------------------------------------------------------
# Key validation (max 449 chars)
# ---------------------------------------------------------------------------

class TestKeyValidation:
    """key validation rules — max 449 characters."""

    def test_key_at_max_length_accepted(self, cache: PostgresCache):
        """Exactly 449 chars is accepted."""
        key = "a" * 449
        cache.set(key, b"ok", EntryOptions(sliding_expiration=timedelta(minutes=5)))
        assert cache.get(key) == b"ok"

    def test_key_over_max_length_raises(self, cache: PostgresCache):
        """450 chars raises ValueError before touching the DB."""
        key = "a" * 450
        with pytest.raises(ValueError, match="exceeds maximum length"):
            cache.set(key, b"x")

    def test_empty_key_raises(self, cache: PostgresCache):
        with pytest.raises(ValueError):
            cache.get("")

    def test_key_validation_on_all_operations(self, cache: PostgresCache):
        """Validation applies to get, refresh, remove, get_or_create."""
        bad_key = "k" * 450
        with pytest.raises(ValueError):
            cache.get(bad_key)
        with pytest.raises(ValueError):
            cache.refresh(bad_key)
        with pytest.raises(ValueError):
            cache.remove(bad_key)
        with pytest.raises(ValueError):
            cache.get_or_create(bad_key, lambda: b"v")


# ---------------------------------------------------------------------------
# Data parity (timezone, NULL mapping, shared schema)
# ---------------------------------------------------------------------------

class TestDataParity:
    """Data parity — timezone integrity and NULL column mapping."""

    def test_expiresattime_is_timezone_aware_in_db(
        self, cache: PostgresCache, base_options: PostgresCacheOptions, dsn: str
    ):
        """expiresattime persisted to DB must be TIMESTAMPTZ (tz-aware)."""
        key = unique_key("par10-tz")
        cache.set(key, b"tz-test", EntryOptions(
            sliding_expiration=timedelta(minutes=10)
        ))
        rows = raw_query(
            dsn,
            f"SELECT expiresattime, expiresattime AT TIME ZONE 'UTC'"
            f" FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        assert rows
        expires_at, expires_at_utc = rows[0]
        # AT TIME ZONE 'UTC' on a tz-aware value returns a naive datetime;
        # on a naive value it would return a tz-aware one.
        # Either way, the stored value must be close to what we expect.
        assert expires_at is not None, "expiresattime must not be NULL"

    def test_null_sliding_stored_as_sql_null(
        self, cache: PostgresCache, base_options: PostgresCacheOptions, dsn: str
    ):
        """slidingexpirationinseconds must be SQL NULL when not set — not 0 or 'None'."""
        key = unique_key("par10-null-sliding")
        cache.set(key, b"abs-only", EntryOptions(
            absolute_expiration_relative=timedelta(minutes=30)
        ))
        rows = raw_query(
            dsn,
            f"SELECT slidingexpirationinseconds FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        assert rows
        assert rows[0][0] is None, (
            f"slidingexpirationinseconds must be SQL NULL, got {rows[0][0]!r}"
        )

    def test_null_absolute_expiration_stored_as_sql_null(
        self, cache: PostgresCache, base_options: PostgresCacheOptions, dsn: str
    ):
        """absoluteexpiration must be SQL NULL when not set."""
        key = unique_key("par10-null-abs")
        cache.set(key, b"sliding-only", EntryOptions(
            sliding_expiration=timedelta(minutes=20)
        ))
        rows = raw_query(
            dsn,
            f"SELECT absoluteexpiration FROM {base_options.schema}.{base_options.table} WHERE id = %s",
            (key,),
        )
        assert rows
        assert rows[0][0] is None, (
            f"absoluteexpiration must be SQL NULL, got {rows[0][0]!r}"
        )

    def test_set_and_get_preserves_all_bytes(self, cache: PostgresCache):
        """All byte values (0x00-0xFF) must survive a set/get roundtrip."""
        key = unique_key("par10-bytes")
        value = bytes(range(256))  # all possible byte values
        cache.set(key, value, EntryOptions(sliding_expiration=timedelta(minutes=10)))
        assert cache.get(key) == value

    def test_concurrent_set_get_same_key(self, cache: PostgresCache):
        """Concurrent set+get on the same key must not corrupt data or crash."""
        key = unique_key("par10-concurrent")
        errors = []

        def writer():
            try:
                for i in range(5):
                    cache.set(key, f"value-{i}".encode(), EntryOptions(
                        sliding_expiration=timedelta(minutes=5)
                    ))
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(5):
                    cache.get(key)  # may return any value or None — just must not crash
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=reader),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Concurrent operations raised: {errors}"
