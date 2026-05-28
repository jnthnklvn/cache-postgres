"""
Unit tests for SqlQueries (_sql.py).

These tests verify SQL string generation without any database connection.
All assertions are on the generated strings themselves.

Spec: _reversa_sdd/migration/target_architecture.md § BC-2
      _reversa_sdd/migration/target_data_model.md § SQL das operações principais
"""

import pytest

from cache_postgres._sql import SqlQueries, _delimit_identifier, COL_VALUE


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sql() -> SqlQueries:
    """Default SqlQueries instance with schema='public', table='cache'."""
    return SqlQueries(schema="public", table="cache", use_wal=False)


@pytest.fixture
def sql_wal() -> SqlQueries:
    """SqlQueries with use_wal=True (no UNLOGGED)."""
    return SqlQueries(schema="public", table="cache", use_wal=True)


@pytest.fixture
def sql_custom() -> SqlQueries:
    """SqlQueries with custom schema and table."""
    return SqlQueries(schema="myschema", table="mycache", use_wal=False)


# ---------------------------------------------------------------------------
# _delimit_identifier (bug-for-bug no-op — BR-MIGRAR-013)
# ---------------------------------------------------------------------------


class TestDelimitIdentifier:
    def test_returns_escaped(self):
        assert _delimit_identifier("public") == '"public"'

    def test_returns_empty_string_escaped(self):
        assert _delimit_identifier("") == '""'

    def test_escapes_internal_quotes(self):
        assert _delimit_identifier('my"schema') == '"my""schema"'


# ---------------------------------------------------------------------------
# DDL — create_schema
# ---------------------------------------------------------------------------


class TestCreateSchema:
    def test_contains_create_schema(self, sql: SqlQueries):
        assert "CREATE SCHEMA IF NOT EXISTS" in sql.create_schema

    def test_contains_schema_name(self, sql: SqlQueries):
        assert "public" in sql.create_schema

    def test_custom_schema(self, sql_custom: SqlQueries):
        assert "myschema" in sql_custom.create_schema


# ---------------------------------------------------------------------------
# DDL — create_table
# ---------------------------------------------------------------------------


class TestCreateTable:
    def test_unlogged_by_default(self, sql: SqlQueries):
        assert "UNLOGGED" in sql.create_table

    def test_no_unlogged_when_wal(self, sql_wal: SqlQueries):
        # use_wal=True → regular table (no UNLOGGED keyword)
        assert "UNLOGGED" not in sql_wal.create_table

    def test_contains_qualified_table_name(self, sql: SqlQueries):
        assert '"public"."cache"' in sql.create_table

    def test_contains_id_column(self, sql: SqlQueries):
        assert "id" in sql.create_table
        assert "VARCHAR(449)" in sql.create_table

    def test_contains_collate_c(self, sql: SqlQueries):
        assert 'COLLATE "C"' in sql.create_table

    def test_contains_value_bytea(self, sql: SqlQueries):
        assert "value" in sql.create_table
        assert "BYTEA" in sql.create_table

    def test_contains_expiresattime_timestamptz(self, sql: SqlQueries):
        assert "expiresattime" in sql.create_table
        assert "TIMESTAMPTZ" in sql.create_table

    def test_contains_sliding_bigint(self, sql: SqlQueries):
        assert "slidingexpirationinseconds" in sql.create_table
        assert "BIGINT" in sql.create_table

    def test_contains_absoluteexpiration(self, sql: SqlQueries):
        assert "absoluteexpiration" in sql.create_table

    def test_contains_primary_key(self, sql: SqlQueries):
        assert "PRIMARY KEY" in sql.create_table

    def test_custom_schema_table(self, sql_custom: SqlQueries):
        assert '"myschema"."mycache"' in sql_custom.create_table


# ---------------------------------------------------------------------------
# DDL — create_index
# ---------------------------------------------------------------------------


class TestCreateIndex:
    def test_contains_create_index(self, sql: SqlQueries):
        assert "CREATE INDEX IF NOT EXISTS" in sql.create_index

    def test_contains_expiresattime_index_name(self, sql: SqlQueries):
        assert "ix_expiresattime" in sql.create_index

    def test_contains_deduplicate(self, sql: SqlQueries):
        assert "deduplicate_items" in sql.create_index

    def test_on_correct_table(self, sql: SqlQueries):
        assert '"public"."cache"' in sql.create_index


# ---------------------------------------------------------------------------
# get_item — BR-MIGRAR-009 (UPDATE + RETURNING with sliding expiration)
# ---------------------------------------------------------------------------


class TestGetItem:
    def test_is_update_returning(self, sql: SqlQueries):
        assert sql.get_item.strip().upper().startswith("UPDATE")
        assert "RETURNING" in sql.get_item

    def test_contains_sliding_expiration_case(self, sql: SqlQueries):
        assert "slidingexpirationinseconds" in sql.get_item
        assert "CASE" in sql.get_item.upper()
        assert "LEAST" in sql.get_item.upper()

    def test_filters_expired(self, sql: SqlQueries):
        # Must only touch non-expired rows
        assert "expiresattime >=" in sql.get_item

    def test_three_placeholders(self, sql: SqlQueries):
        # Params: (utcNow, key, utcNow)
        assert sql.get_item.count("%s") == 3

    def test_on_correct_table(self, sql: SqlQueries):
        assert '"public"."cache"' in sql.get_item


# ---------------------------------------------------------------------------
# set_item — BR-MIGRAR-007 (UPSERT via CTE ON CONFLICT)
# ---------------------------------------------------------------------------


class TestSetItem:
    def test_is_insert_with_on_conflict(self, sql: SqlQueries):
        upper = sql.set_item.upper()
        assert "INSERT INTO" in upper
        assert "ON CONFLICT" in upper
        assert "DO UPDATE" in upper

    def test_uses_direct_insert(self, sql: SqlQueries):
        upper = sql.set_item.upper()
        assert upper.strip().startswith("INSERT INTO")

    def test_six_placeholders(self, sql: SqlQueries):
        # Params: (key, value, expires_at, sliding_secs, abs_exp, tags)
        assert sql.set_item.count("%s") == 6

    def test_contains_all_columns(self, sql: SqlQueries):
        for col in ("id", "value", "expiresattime", "slidingexpirationinseconds", "absoluteexpiration"):
            assert col in sql.set_item

    def test_on_correct_table(self, sql: SqlQueries):
        assert '"public"."cache"' in sql.set_item


# ---------------------------------------------------------------------------
# refresh_item — BR-MIGRAR-009 (sliding, no RETURNING)
# ---------------------------------------------------------------------------


class TestRefreshItem:
    def test_is_update(self, sql: SqlQueries):
        assert sql.refresh_item.strip().upper().startswith("UPDATE")

    def test_no_returning(self, sql: SqlQueries):
        assert "RETURNING" not in sql.refresh_item.upper()

    def test_three_placeholders(self, sql: SqlQueries):
        # Params: (utcNow, key, utcNow)
        assert sql.refresh_item.count("%s") == 3

    def test_filters_expired(self, sql: SqlQueries):
        assert "expiresattime >=" in sql.refresh_item

    def test_on_correct_table(self, sql: SqlQueries):
        assert '"public"."cache"' in sql.refresh_item


# ---------------------------------------------------------------------------
# remove_item — BR-MIGRAR-003
# ---------------------------------------------------------------------------


class TestRemoveItem:
    def test_is_delete(self, sql: SqlQueries):
        assert sql.remove_item.strip().upper().startswith("DELETE")

    def test_one_placeholder(self, sql: SqlQueries):
        assert sql.remove_item.count("%s") == 1

    def test_on_correct_table(self, sql: SqlQueries):
        assert '"public"."cache"' in sql.remove_item


# ---------------------------------------------------------------------------
# delete_expired — BR-MIGRAR-018 (background scanner batch)
# ---------------------------------------------------------------------------


class TestDeleteExpired:
    def test_is_delete(self, sql: SqlQueries):
        assert sql.delete_expired.strip().upper().startswith("DELETE")

    def test_one_placeholder(self, sql: SqlQueries):
        assert sql.delete_expired.count("%s") == 1

    def test_filters_by_expiry(self, sql: SqlQueries):
        assert "expiresattime <" in sql.delete_expired

    def test_on_correct_table(self, sql: SqlQueries):
        assert '"public"."cache"' in sql.delete_expired


# ---------------------------------------------------------------------------
# advisory_lock — BR-MIGRAR-002 (stampede protection)
# ---------------------------------------------------------------------------


class TestAdvisoryLock:
    def test_uses_pg_advisory_xact_lock(self, sql: SqlQueries):
        assert "pg_advisory_xact_lock" in sql.advisory_lock

    def test_uses_hashtextextended(self, sql: SqlQueries):
        assert "hashtextextended" in sql.advisory_lock

    def test_one_placeholder(self, sql: SqlQueries):
        assert sql.advisory_lock.count("%s") == 1

    def test_is_select(self, sql: SqlQueries):
        assert sql.advisory_lock.strip().upper().startswith("SELECT")


# ---------------------------------------------------------------------------
# get_item_after_lock — double-check after acquiring advisory lock
# ---------------------------------------------------------------------------


class TestGetItemAfterLock:
    def test_is_select(self, sql: SqlQueries):
        assert sql.get_item_after_lock.strip().upper().startswith("SELECT")

    def test_two_placeholders(self, sql: SqlQueries):
        # Params: (key, utcNow)
        assert sql.get_item_after_lock.count("%s") == 2

    def test_filters_expired(self, sql: SqlQueries):
        assert "expiresattime >=" in sql.get_item_after_lock

    def test_returns_value(self, sql: SqlQueries):
        assert COL_VALUE in sql.get_item_after_lock

    def test_on_correct_table(self, sql: SqlQueries):
        assert '"public"."cache"' in sql.get_item_after_lock
