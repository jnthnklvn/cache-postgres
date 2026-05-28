"""
SQL query strings for postgres-cache.

Spec: _reversa_sdd/migration/target_architecture.md § BC-2, DA-02
      _reversa_sdd/migration/target_data_model.md § SQL das operações principais
      _reversa_sdd/migration/target_business_rules.md § BR-MIGRAR-007 to BR-MIGRAR-009,
                                                         BR-MIGRAR-013, BR-MIGRAR-017 to BR-MIGRAR-019

SqlQueries is a stateless class initialized once per PostgresCache instance.
Schema and table are injected at construction time and pre-formatted into all
query strings — identical approach to SqlQueries.cs in the legacy C# code.

Column name constants are inlined here (replaces Columns.cs from the legacy).
All SQL uses psycopg2 placeholder syntax (%s) instead of Npgsql's @param.
"""

# ---------------------------------------------------------------------------
# Column name constants (replaces Columns.cs — BR-MIGRAR-013, topology_decision § mapping)
# ---------------------------------------------------------------------------
COL_ID = "id"
COL_VALUE = "value"
COL_EXPIRES_AT = "expiresattime"
COL_SLIDING_SECONDS = "slidingexpirationinseconds"
COL_ABSOLUTE_EXPIRATION = "absoluteexpiration"
COL_TAGS = "tags"


def _delimit_identifier(name: str) -> str:
    """Return the identifier properly escaped with double quotes.

    Escapes internal double quotes according to standard SQL, ensuring
    safe identifiers for schema and table names.
    """
    safe_name = name.replace('"', '""')
    return f'"{safe_name}"'


class SqlQueries:
    """Pre-formatted SQL query strings for a specific schema/table pair.

    Instantiated once per PostgresCache instance. All queries are formatted
    at construction time so there is zero overhead per operation at runtime.

    Spec: target_architecture.md § BC-2 (Database Access), DA-02
          target_data_model.md § DDL completo + SQL das operações principais
    """

    def __init__(self, schema: str, table: str, use_wal: bool = False) -> None:
        """
        Args:
            schema: PostgreSQL schema name (e.g. "public").
            table:  Cache table name (e.g. "cache").
            use_wal: If True, create a regular (WAL-logged) table.
                     If False (default), create an UNLOGGED table.
        """
        s = _delimit_identifier(schema)
        t = _delimit_identifier(table)
        qualified = f"{s}.{t}"
        unlogged = "" if use_wal else "UNLOGGED"

        # ------------------------------------------------------------------
        # DDL — BR-MIGRAR-006, BR-MIGRAR-008, BR-MIGRAR-017, BR-MIGRAR-019
        # ------------------------------------------------------------------

        # CREATE SCHEMA (idempotent)
        self.create_schema: str = f"CREATE SCHEMA IF NOT EXISTS {s};"

        # CREATE TABLE (UNLOGGED by default — use_wal=False)
        # Schema is identical to legacy C# — BR-MIGRAR-008
        self.create_table: str = (
            f"CREATE {unlogged} TABLE IF NOT EXISTS {qualified} ("
            f"  {COL_ID}                        VARCHAR(449) COLLATE \"C\"  NOT NULL,"
            f"  {COL_VALUE}                     BYTEA                     NOT NULL,"
            f"  {COL_EXPIRES_AT}                TIMESTAMPTZ               NOT NULL,"
            f"  {COL_SLIDING_SECONDS}           BIGINT                    NULL,"
            f"  {COL_ABSOLUTE_EXPIRATION}       TIMESTAMPTZ               NULL,"
            f"  {COL_TAGS}                      TEXT[]                    NULL,"
            f"  CONSTRAINT pk_{t} PRIMARY KEY ({COL_ID})"
            f");"
        )

        # CREATE INDEX — BR-MIGRAR-019
        self.create_index: str = (
            f"CREATE INDEX IF NOT EXISTS ix_{COL_EXPIRES_AT}"
            f"  ON {qualified} ({COL_EXPIRES_AT})"
            f"  WITH (deduplicate_items = True);"
        )

        self.create_tags_index: str = (
            f"CREATE INDEX IF NOT EXISTS ix_{t}_tags"
            f"  ON {qualified} USING GIN ({COL_TAGS});"
        )

        # ------------------------------------------------------------------
        # get — BR-MIGRAR-009 (sliding expiration recalculated atomically in DB)
        #
        # Renews sliding expiration on read via UPDATE + RETURNING (single
        # round-trip). Returns NULL if key not found or already expired.
        # Params: (utcNow, key, utcNow)
        # ------------------------------------------------------------------
        self.get_item: str = (
            f"UPDATE {qualified}"
            f" SET {COL_EXPIRES_AT} = CASE"
            f"     WHEN {COL_SLIDING_SECONDS} IS NOT NULL"
            f"     THEN LEAST("
            f"         %s + ({COL_SLIDING_SECONDS} * INTERVAL '1 second'),"
            f"         COALESCE({COL_ABSOLUTE_EXPIRATION}, 'infinity'::timestamptz)"
            f"     )"
            f"     ELSE {COL_EXPIRES_AT}"
            f" END"
            f" WHERE {COL_ID} = %s"
            f"   AND {COL_EXPIRES_AT} >= %s"
            f" RETURNING {COL_VALUE};"
        )

        # ------------------------------------------------------------------
        # set — BR-MIGRAR-007 (UPSERT via CTE ON CONFLICT)
        #
        # Atomic upsert — no race between concurrent INSERT and UPDATE.
        # Params: (key, value, expires_at, sliding_secs_or_None, abs_exp_or_None, tags_array)
        # ------------------------------------------------------------------
        self.set_item: str = (
            f"INSERT INTO {qualified}"
            f"  ({COL_ID}, {COL_VALUE}, {COL_EXPIRES_AT},"
            f"   {COL_SLIDING_SECONDS}, {COL_ABSOLUTE_EXPIRATION}, {COL_TAGS})"
            f" VALUES (%s::varchar(449), %s::bytea, %s::timestamptz, %s::bigint, %s::timestamptz, %s::text[])"
            f" ON CONFLICT ({COL_ID}) DO UPDATE SET"
            f"   {COL_VALUE}               = EXCLUDED.{COL_VALUE},"
            f"   {COL_EXPIRES_AT}          = EXCLUDED.{COL_EXPIRES_AT},"
            f"   {COL_SLIDING_SECONDS}     = EXCLUDED.{COL_SLIDING_SECONDS},"
            f"   {COL_ABSOLUTE_EXPIRATION} = EXCLUDED.{COL_ABSOLUTE_EXPIRATION},"
            f"   {COL_TAGS}                = EXCLUDED.{COL_TAGS};"
        )

        # ------------------------------------------------------------------
        # refresh — BR-MIGRAR-009 (renew sliding without returning value)
        #
        # Params: (utcNow, key, utcNow)
        # ------------------------------------------------------------------
        self.refresh_item: str = (
            f"UPDATE {qualified}"
            f" SET {COL_EXPIRES_AT} = CASE"
            f"     WHEN {COL_SLIDING_SECONDS} IS NOT NULL"
            f"     THEN LEAST("
            f"         %s + ({COL_SLIDING_SECONDS} * INTERVAL '1 second'),"
            f"         COALESCE({COL_ABSOLUTE_EXPIRATION}, 'infinity'::timestamptz)"
            f"     )"
            f"     ELSE {COL_EXPIRES_AT}"
            f" END"
            f" WHERE {COL_ID} = %s"
            f"   AND {COL_EXPIRES_AT} >= %s;"
        )

        # ------------------------------------------------------------------
        # remove — BR-MIGRAR-003
        #
        # Params: (key,)
        # ------------------------------------------------------------------
        self.remove_item: str = (
            f"DELETE FROM {qualified} WHERE {COL_ID} = %s;"
        )

        # ------------------------------------------------------------------
        # delete_expired — BR-MIGRAR-018 (background scanner batch delete)
        #
        # Params: (utcNow,)
        # ------------------------------------------------------------------
        self.delete_expired: str = (
            f"DELETE FROM {qualified} WHERE {COL_EXPIRES_AT} < %s;"
        )

        # ------------------------------------------------------------------
        # delete_by_tags
        #
        # Deletes all cache entries that contain ALL of the specified tags.
        # Params: (tags_array,)
        # ------------------------------------------------------------------
        self.delete_by_tags: str = (
            f"DELETE FROM {qualified} WHERE {COL_TAGS} @> %s::text[];"
        )

        # ------------------------------------------------------------------
        # advisory_lock — BR-MIGRAR-002 (stampede protection)
        #
        # Must run inside an explicit transaction (conn.autocommit = False).
        # RISK-006: autocommit must be False before this executes.
        # Params: (key,)
        # ------------------------------------------------------------------
        self.advisory_lock: str = (
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0));"
        )

        # ------------------------------------------------------------------
        # get_or_create double-check (after acquiring advisory lock)
        #
        # Re-reads the entry after obtaining the lock — another worker may
        # have already created it. No sliding expiration renewal on this read
        # (the canonical get_item handles that).
        # Params: (key, utcNow)
        # ------------------------------------------------------------------
        self.get_item_after_lock: str = (
            f"SELECT {COL_VALUE} FROM {qualified}"
            f" WHERE {COL_ID} = %s"
            f"   AND {COL_EXPIRES_AT} >= %s;"
        )
