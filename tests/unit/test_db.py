"""
Unit tests for DatabaseOperations (_db.py).
"""

import threading
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call, patch

from postgres_cache._db import DatabaseOperations
from postgres_cache._options import EntryOptions, PostgresCacheOptions
from postgres_cache._sql import SqlQueries


def make_options(**kwargs) -> PostgresCacheOptions:
    defaults = dict(
        dsn="postgresql://localhost/testdb",
        schema="public",
        table="cache",
        create_if_not_exists=False,
    )
    defaults.update(kwargs)
    return PostgresCacheOptions(**defaults)


def make_db(options=None):
    opts = options or make_options()
    sql = SqlQueries(schema=opts.schema, table=opts.table)
    with patch("postgres_cache._db.ConnectionPool"):
        db = DatabaseOperations(options=opts, sql=sql)
    return db, sql


def mock_connection(fetchone_return=None):
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = fetchone_return
    cur.rowcount = 0
    conn.cursor.return_value = cur
    
    # Mock transaction block
    tx = MagicMock()
    tx.__enter__ = lambda s: s
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx
    
    return conn, cur


class TestConnectionMode:
    def test_dsn_mode_creates_pool(self):
        opts = make_options(dsn="postgresql://localhost/db")
        sql = SqlQueries(schema=opts.schema, table=opts.table)
        with patch("postgres_cache._db.ConnectionPool") as mock_pool:
            db = DatabaseOperations(options=opts, sql=sql)
            mock_pool.assert_called_once_with(
                conninfo="postgresql://localhost/db",
                min_size=1,
                max_size=10,
                open=True
            )
            assert db._pool is not None

    def test_factory_mode_does_not_create_pool(self):
        factory = MagicMock()
        opts = make_options(dsn=None, connection_factory=factory)
        sql = SqlQueries(schema=opts.schema, table=opts.table)
        with patch("postgres_cache._db.ConnectionPool") as mock_pool:
            db = DatabaseOperations(options=opts, sql=sql)
            mock_pool.assert_not_called()
            assert db._pool is None

    def test_dsn_mode_closes_pool(self):
        opts = make_options(dsn="postgresql://localhost/db")
        sql = SqlQueries(schema=opts.schema, table=opts.table)
        with patch("postgres_cache._db.ConnectionPool") as mock_pool:
            db = DatabaseOperations(options=opts, sql=sql)
            db.close()
            db._pool.close.assert_called_once()


class TestEnsureTableExists:
    def test_skipped_when_create_if_not_exists_false(self):
        opts = make_options(create_if_not_exists=False)
        db, _ = make_db(opts)
        with patch.object(db, "_get_connection") as mock_get_conn:
            db.ensure_table_exists()
            mock_get_conn.assert_not_called()

    def test_creates_table_once_when_flag_true(self):
        opts = make_options(create_if_not_exists=True)
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = MagicMock(return_value=False)
        
        with patch.object(db, "_get_connection", return_value=ctx):
            db.ensure_table_exists()
            db.ensure_table_exists()  # Second call should be a no-op
            assert db._table_created is True
            assert cur.execute.call_count == 3  # schema, table, index


class TestGet:
    def test_returns_value_on_hit(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=(b"hello",))
        
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = MagicMock(return_value=False)
        
        with patch.object(db, "_get_connection", return_value=ctx):
            result = db.get("my-key")
            assert result == b"hello"

    def test_returns_none_on_miss(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=None)
        
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = MagicMock(return_value=False)
        
        with patch.object(db, "_get_connection", return_value=ctx):
            assert db.get("missing-key") is None

    def test_utcnow_passed_as_tz_aware(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        captured = []
        cur.execute.side_effect = lambda sql, params=None: captured.extend(params or [])
        
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = MagicMock(return_value=False)
        
        with patch.object(db, "_get_connection", return_value=ctx):
            db.get("key")
            
        tz_params = [p for p in captured if isinstance(p, datetime)]
        for dt in tz_params:
            assert dt.tzinfo is not None


class TestSet:
    def test_executes_set_item_sql(self):
        opts = make_options()
        db, sql = make_db(opts)
        conn, cur = mock_connection()
        
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = MagicMock(return_value=False)
        
        with patch.object(db, "_get_connection", return_value=ctx):
            db.set("key", b"value")
            cur.execute.assert_called_once()
            assert "INSERT INTO" in cur.execute.call_args[0][0].upper()


class TestRefresh:
    def test_executes_refresh_sql(self):
        opts = make_options()
        db, sql = make_db(opts)
        conn, cur = mock_connection()
        
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = MagicMock(return_value=False)
        
        with patch.object(db, "_get_connection", return_value=ctx):
            db.refresh("key")
            assert "UPDATE" in cur.execute.call_args[0][0].upper()


class TestRemove:
    def test_executes_delete_sql(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = MagicMock(return_value=False)
        
        with patch.object(db, "_get_connection", return_value=ctx):
            db.remove("key")
            assert "DELETE" in cur.execute.call_args[0][0].upper()


class TestDeleteExpired:
    def test_executes_delete_expired_sql(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection()
        cur.rowcount = 3
        
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = MagicMock(return_value=False)
        
        with patch.object(db, "_get_connection", return_value=ctx):
            count = db.delete_expired()
            assert count == 3
            assert "DELETE" in cur.execute.call_args[0][0].upper()


class TestGetOrCreate:
    def test_factory_called_exactly_once_on_miss(self):
        opts = make_options()
        db, _ = make_db(opts)
        conn, cur = mock_connection(fetchone_return=None) # miss
        
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = MagicMock(return_value=False)
        
        factory = MagicMock(return_value=b"fresh-value")
        
        with patch.object(db, "_get_connection", return_value=ctx):
            result = db.get_or_create("key", factory)
            assert result == b"fresh-value"
            factory.assert_called_once()
            # 1. advisory_lock
            # 2. get_item_after_lock
            # 3. set_item
            assert cur.execute.call_count == 3
