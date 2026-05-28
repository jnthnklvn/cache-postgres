"""
Unit tests for DatabaseOperations (_db.py).

Uses unittest.mock to mock psycopg2 connections — no real database required.

Spec: _reversa_sdd/migration/target_architecture.md § BC-2, DA-01, DA-05, DA-06
      _reversa_sdd/migration/target_business_rules.md § BR-MIGRAR-002, BR-MIGRAR-006
      _reversa_sdd/migration/risk_register.md § RISK-004, RISK-006
"""

import threading
import pytest
import psycopg2
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call, patch, PropertyMock

from postgres_cache._db import DatabaseOperations
from postgres_cache._options import EntryOptions, PostgresCacheOptions
from postgres_cache._sql import SqlQueries


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

def make_options(**kwargs) -> PostgresCacheOptions:
    defaults = dict(
        dsn="postgresql://localhost/testdb",
        schema="public",
        table="cache",
        create_if_not_exists=False,
    )
    defaults.update(kwargs)
    return PostgresCacheOptions(**defaults)


def make_sql(options: PostgresCacheOptions | None = None) -> SqlQueries:
    if options is None:
        options = make_options()
    return SqlQueries(schema=options.schema, table=options.table, use_wal=options.use_wal)


def make_db(options: PostgresCacheOptions | None = None) -> tuple[DatabaseOperations, SqlQueries]:
    opts = options or make_options()
    sql = make_sql(opts)
    db = DatabaseOperations(options=opts, sql=sql)
    return db, sql


def mock_connection(fetchone_return=None):
    """Build a mock psycopg2 connection with a mock cursor."""
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = fetchone_return
    cur.rowcount = 0
    conn.cursor.return_value = cur
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn, cur


# ---------------------------------------------------------------------------
# Connection mode — BR-MIGRAR-010
# ---------------------------------------------------------------------------

class TestConnectionMode:
    def test_dsn_mode_calls_psycopg2_connect(self):
        opts = make_options(dsn="postgresql://localhost/db")
        db, _ = make_db(opts)
        with patch("postgres_cache._db.psycopg2.connect") as mock_connect:
            mock_connect.return_value = mock_connection()[0]
            db._connect()
        mock_connect.assert_called_once_with("postgresql://localhost/db")

    def test_factory_mode_calls_factory(self):
        factory = MagicMock(return_value=mock_connection()[0])
        opts = make_options(dsn=None, connection_factory=factory)
        db, _ = make_db(opts)
        db._connect()
        factory.assert_called_once()

    def test_dsn_mode_is_owned_connection(self):
        opts = make_options(dsn="postgresql://localhost/db")
        db, _ = make_db(opts)
        assert db._is_owned_connection() is True

    def test_factory_mode_is_not_owned_connection(self):
        opts = make_options(dsn=None, connection_factory=lambda: None)
        db, _ = make_db(opts)
        assert db._is_owned_connection() is False

    def test_dsn_mode_closes_owned_connection(self):
        opts = make_options(dsn="postgresql://localhost/db")
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=None)
        cur.rowcount = 0
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.remove("some-key")
        conn.close.assert_called_once()

    def test_factory_mode_does_not_close_connection(self):
        conn, cur = mock_connection(fetchone_return=None)
        cur.rowcount = 0
        factory = MagicMock(return_value=conn)
        opts = make_options(dsn=None, connection_factory=factory)
        db, _ = make_db(opts)
        db.remove("some-key")
        conn.close.assert_not_called()


# ---------------------------------------------------------------------------
# DDL — BR-MIGRAR-006 (double-checked locking)
# ---------------------------------------------------------------------------

class TestEnsureTableExists:
    def test_skipped_when_create_if_not_exists_false(self):
        opts = make_options(create_if_not_exists=False)
        db, _ = make_db(opts)
        with patch("postgres_cache._db.psycopg2.connect") as mock_connect:
            db.ensure_table_exists()
        mock_connect.assert_not_called()

    def test_creates_table_once_when_flag_true(self):
        opts = make_options(create_if_not_exists=True)
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.ensure_table_exists()
            db.ensure_table_exists()  # second call — must be no-op
        # connect called only once (double-check prevents second DDL)
        assert conn.cursor.call_count == 1

    def test_table_created_flag_set_after_ddl(self):
        opts = make_options(create_if_not_exists=True)
        db, _ = make_db(opts)
        conn, _ = mock_connection()
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.ensure_table_exists()
        assert db._table_created is True

    def test_concurrent_ddl_only_runs_once(self):
        """Simulate concurrent calls — DDL must be idempotent per instance."""
        opts = make_options(create_if_not_exists=True)
        db, _ = make_db(opts)
        call_count = {"n": 0}
        original_connect = psycopg2.connect

        def slow_connect(dsn):
            call_count["n"] += 1
            import time
            time.sleep(0.02)
            conn2, _ = mock_connection()
            return conn2

        with patch("postgres_cache._db.psycopg2.connect", side_effect=slow_connect):
            threads = [threading.Thread(target=db.ensure_table_exists) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        assert call_count["n"] == 1  # only one DDL connect


# ---------------------------------------------------------------------------
# get — BR-MIGRAR-003, BR-MIGRAR-009, RISK-004
# ---------------------------------------------------------------------------

class TestGet:
    def test_returns_value_on_hit(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=(b"hello",))
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            result = db.get("my-key")
        assert result == b"hello"

    def test_returns_none_on_miss(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=None)
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            result = db.get("missing-key")
        assert result is None

    def test_utcnow_passed_as_tz_aware(self):
        """RISK-004: parameters passed to SQL must be tz-aware."""
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=None)
        captured_params = []

        def capture_execute(sql, params=None):
            if params:
                captured_params.extend(params)

        cur.execute.side_effect = capture_execute
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.get("some-key")

        # First and third params are utcNow (get_item has 3 params: utcNow, key, utcNow)
        tz_params = [p for p in captured_params if isinstance(p, datetime)]
        for dt in tz_params:
            assert dt.tzinfo is not None, "RISK-004: naive datetime found in get() params"

    def test_commits_on_success(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, _ = mock_connection(fetchone_return=None)
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.get("key")
        conn.commit.assert_called_once()

    def test_rollback_on_error(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        cur.execute.side_effect = Exception("db error")
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            with pytest.raises(Exception, match="db error"):
                db.get("key")
        conn.rollback.assert_called_once()


# ---------------------------------------------------------------------------
# set — BR-MIGRAR-003, BR-MIGRAR-007, RISK-004
# ---------------------------------------------------------------------------

class TestSet:
    def test_executes_set_item_sql(self):
        opts = make_options()
        db, sql = make_db(opts)
        conn, cur = mock_connection()
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.set("key", b"value")
        cur.execute.assert_called_once()
        executed_sql = cur.execute.call_args[0][0]
        assert "INSERT INTO" in executed_sql.upper()
        assert "ON CONFLICT" in executed_sql.upper()

    def test_expires_at_is_tz_aware(self):
        """RISK-004: expires_at must be tz-aware."""
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        captured = []

        def cap(sql, params):
            captured.extend(params)

        cur.execute.side_effect = cap
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.set("key", b"v", EntryOptions(sliding_expiration=timedelta(minutes=10)))

        dt_params = [p for p in captured if isinstance(p, datetime)]
        for dt in dt_params:
            assert dt.tzinfo is not None, "RISK-004: naive datetime in set() params"

    def test_commits_on_success(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, _ = mock_connection()
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.set("k", b"v")
        conn.commit.assert_called_once()

    def test_rollback_on_error(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        cur.execute.side_effect = Exception("write error")
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            with pytest.raises(Exception):
                db.set("k", b"v")
        conn.rollback.assert_called_once()


# ---------------------------------------------------------------------------
# refresh — BR-MIGRAR-009, RISK-004
# ---------------------------------------------------------------------------

class TestRefresh:
    def test_executes_refresh_sql(self):
        opts = make_options()
        db, sql = make_db(opts)
        conn, cur = mock_connection()
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.refresh("key")
        executed_sql = cur.execute.call_args[0][0]
        assert "UPDATE" in executed_sql.upper()
        assert "RETURNING" not in executed_sql.upper()

    def test_utcnow_is_tz_aware(self):
        """RISK-004."""
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        captured = []

        def cap(sql, params):
            captured.extend(params)

        cur.execute.side_effect = cap
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.refresh("key")

        dt_params = [p for p in captured if isinstance(p, datetime)]
        for dt in dt_params:
            assert dt.tzinfo is not None, "RISK-004: naive datetime in refresh() params"


# ---------------------------------------------------------------------------
# remove — BR-MIGRAR-003
# ---------------------------------------------------------------------------

class TestRemove:
    def test_executes_delete_sql(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        cur.rowcount = 1
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.remove("key")
        executed_sql = cur.execute.call_args[0][0]
        assert "DELETE" in executed_sql.upper()


# ---------------------------------------------------------------------------
# delete_expired — BR-MIGRAR-018, RISK-004
# ---------------------------------------------------------------------------

class TestDeleteExpired:
    def test_executes_delete_expired_sql(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        cur.rowcount = 3
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            count = db.delete_expired()
        assert count == 3
        executed_sql = cur.execute.call_args[0][0]
        assert "DELETE" in executed_sql.upper()
        assert "expiresattime" in executed_sql

    def test_utcnow_is_tz_aware(self):
        """RISK-004."""
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        cur.rowcount = 0
        captured = []

        def cap(sql, params):
            captured.extend(params)

        cur.execute.side_effect = cap
        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.delete_expired()

        dt_params = [p for p in captured if isinstance(p, datetime)]
        for dt in dt_params:
            assert dt.tzinfo is not None, "RISK-004: naive datetime in delete_expired() params"


# ---------------------------------------------------------------------------
# get_or_create — BR-MIGRAR-002, RISK-006, RISK-004
# ---------------------------------------------------------------------------

class TestGetOrCreate:
    def _make_goc_db(self, fetchone_first=None, fetchone_second=None):
        """Helper: build DB mock where advisory_lock returns None and
        get_item_after_lock returns fetchone_second."""
        opts = make_options()
        db, sql = make_db(opts)
        conn = MagicMock()
        conn.autocommit = True  # will be set to False by get_or_create

        # Build a cursor that tracks calls
        cur = MagicMock()
        cur.__enter__ = lambda s: s
        cur.__exit__ = MagicMock(return_value=False)

        call_idx = {"n": 0}

        def fetchone_side_effect():
            call_idx["n"] += 1
            return fetchone_second if call_idx["n"] > 1 else fetchone_first

        cur.fetchone.side_effect = fetchone_side_effect
        conn.cursor.return_value = cur
        return db, conn, cur

    def test_autocommit_set_to_false_before_lock(self):
        """RISK-006: autocommit must be False before pg_advisory_xact_lock."""
        opts = make_options()
        db, sql = make_db(opts)
        conn, cur = mock_connection(fetchone_return=None)

        autocommit_at_execute = {}

        def capture(sql_str, params=None):
            if params is None:
                return
            autocommit_at_execute[sql_str[:30]] = conn.autocommit

        cur.execute.side_effect = capture
        factory = MagicMock(return_value=b"computed")

        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            with patch.object(db, "set"):  # skip actual set
                db.get_or_create("key", factory)

        # autocommit must have been set False before the advisory lock SQL
        advisory_key = [k for k in autocommit_at_execute if "pg_advisory" in k.lower()]
        for k in advisory_key:
            assert autocommit_at_execute[k] is False, "RISK-006: autocommit was not False before advisory lock"

    def test_factory_not_called_on_hit(self):
        """If entry exists after acquiring lock, factory must NOT be called."""
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=None)

        # get_item_after_lock is the ONLY fetchone in get_or_create block.
        # Return (b"cached",) on the first fetchone call → cache hit.
        call_idx = {"n": 0}
        def fetchone():
            call_idx["n"] += 1
            return (b"cached",)  # always hit — simulates another worker set it

        cur.fetchone.side_effect = fetchone
        factory = MagicMock(return_value=b"should-not-be-called")

        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            result = db.get_or_create("key", factory)

        factory.assert_not_called()
        assert result == b"cached"

    def test_factory_called_exactly_once_on_miss(self):
        """On cache miss, factory must be called exactly once."""
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=None)
        factory = MagicMock(return_value=b"fresh-value")

        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            with patch.object(db, "set") as mock_set:
                result = db.get_or_create("key", factory)

        factory.assert_called_once()
        mock_set.assert_called_once()
        assert result == b"fresh-value"

    def test_commits_on_hit(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=None)

        call_idx = {"n": 0}
        def fetchone():
            call_idx["n"] += 1
            return (b"cached",) if call_idx["n"] >= 2 else None

        cur.fetchone.side_effect = fetchone
        factory = MagicMock()

        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            db.get_or_create("key", factory)

        conn.commit.assert_called()

    def test_rollback_on_error(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        cur.execute.side_effect = Exception("lock error")
        factory = MagicMock()

        with patch("postgres_cache._db.psycopg2.connect", return_value=conn):
            with pytest.raises(Exception, match="lock error"):
                db.get_or_create("key", factory)

        conn.rollback.assert_called_once()
