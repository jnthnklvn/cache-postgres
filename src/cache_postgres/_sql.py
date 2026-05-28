"""
SQL query strings for cache-postgres.

SqlQueries is a stateless class initialized once per PostgresCache instance.
Schema and table are injected at construction time and pre-formatted into all
query strings.
"""

# Column name constants
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
        # DDL
        # ------------------------------------------------------------------

        # CREATE SCHEMA (idempotent)
        self.create_schema: str = f"CREATE SCHEMA IF NOT EXISTS {s};"

        # CREATE TABLE (UNLOGGED by default — use_wal=False)
        self.create_table: str = (
            f"CREATE {unlogged} TABLE IF NOT EXISTS {qualified} ("
            f"  {COL_ID}                        VARCHAR(449) COLLATE \"C\"  NOT NULL,"
            f"  {COL_VALUE}                     BYTEA                     NOT NULL,"
            f"  {COL_EXPIRES_AT}                TIMESTAMPTZ               NOT NULL,"
            f"  {COL_SLIDING_SECONDS}           BIGINT                    NULL,"
            f"  {COL_ABSOLUTE_EXPIRATION}       TIMESTAMPTZ               NULL,"
            f"  {COL_TAGS}                      TEXT[]                    NULL,"
            f"  CONSTRAINT {_delimit_identifier('pk_' + table)} PRIMARY KEY ({COL_ID})"
            f");"
        )

        # CREATE INDEX
        self.create_index: str = (
            f"CREATE INDEX IF NOT EXISTS ix_{COL_EXPIRES_AT}"
            f"  ON {qualified} ({COL_EXPIRES_AT})"
            f"  WITH (deduplicate_items = True);"
        )

        self.create_tags_index: str = (
            f"CREATE INDEX IF NOT EXISTS {_delimit_identifier('ix_' + table + '_tags')}"
            f"  ON {qualified} USING GIN ({COL_TAGS});"
        )

        # ------------------------------------------------------------------
        # get
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
        # get_item_with_ttl
        #
        # Retrieve value and its expiration time (for early cache refresh).
        # Renews sliding expiration on read via UPDATE + RETURNING.
        # Params: (utcNow, key, utcNow)
        # ------------------------------------------------------------------
        self.get_item_with_ttl: str = (
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
            f" RETURNING {COL_VALUE}, {COL_EXPIRES_AT};"
        )

        # ------------------------------------------------------------------
        # get_stale_item
        #
        # Retrieve a value without checking expiration (for failover).
        # Does not renew sliding expiration.
        # Params: (key,)
        # ------------------------------------------------------------------
        self.get_stale_item: str = (
            f"SELECT {COL_VALUE} FROM {qualified} WHERE {COL_ID} = %s;"
        )

        # ------------------------------------------------------------------
        # set
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
        # refresh
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
        # remove
        #
        # Params: (key,)
        # ------------------------------------------------------------------
        self.remove_item: str = (
            f"DELETE FROM {qualified} WHERE {COL_ID} = %s;"
        )

        # ------------------------------------------------------------------
        # delete_expired
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
        # increment_item
        #
        # Atomic counter increment. Converts string to integer, increments,
        # and converts back to string bytes. 
        # Params: (key, value, utcNow, value, utcNow)
        # ------------------------------------------------------------------
        self.increment_item: str = (
            f"INSERT INTO {qualified} ({COL_ID}, {COL_VALUE}, {COL_EXPIRES_AT})"
            f" VALUES (%s, %s::text::bytea, 'infinity')"
            f" ON CONFLICT ({COL_ID}) DO UPDATE"
            f" SET {COL_VALUE} = CASE"
            f"         WHEN {qualified}.{COL_EXPIRES_AT} < %s THEN %s::text::bytea"
            f"         ELSE (COALESCE(convert_from({qualified}.{COL_VALUE}, 'UTF8')::bigint, 0) + %s)::text::bytea"
            f"     END,"
            f"     {COL_EXPIRES_AT} = CASE"
            f"         WHEN {qualified}.{COL_EXPIRES_AT} < %s THEN 'infinity'::timestamptz"
            f"         ELSE {qualified}.{COL_EXPIRES_AT}"
            f"     END"
            f" RETURNING convert_from({COL_VALUE}, 'UTF8')::bigint;"
        )

        # ------------------------------------------------------------------
        # set_lock_item
        #
        # Atomic lock acquisition using INSERT ON CONFLICT DO UPDATE.
        # Params: (key, lock_id, expires_at, now)
        # ------------------------------------------------------------------
        self.set_lock_item: str = (
            f"INSERT INTO {qualified} ({COL_ID}, {COL_VALUE}, {COL_EXPIRES_AT})"
            f" VALUES (%s, %s::bytea, %s)"
            f" ON CONFLICT ({COL_ID}) DO UPDATE"
            f" SET {COL_VALUE} = EXCLUDED.{COL_VALUE},"
            f"     {COL_EXPIRES_AT} = EXCLUDED.{COL_EXPIRES_AT}"
            f" WHERE {qualified}.{COL_EXPIRES_AT} < %s"
            f" RETURNING {COL_ID};"
        )

        # ------------------------------------------------------------------
        # unlock_item
        #
        # Atomic lock release using DELETE WHERE value matches.
        # Params: (key, lock_id)
        # ------------------------------------------------------------------
        self.unlock_item: str = (
            f"DELETE FROM {qualified} WHERE {COL_ID} = %s AND {COL_VALUE} = %s::bytea;"
        )

        # ------------------------------------------------------------------
        # is_locked_item
        #
        # Checks if a key has an active lock.
        # Params: (key, now)
        # ------------------------------------------------------------------
        self.is_locked_item: str = (
            f"SELECT 1 FROM {qualified} WHERE {COL_ID} = %s AND {COL_EXPIRES_AT} >= %s;"
        )

        # ------------------------------------------------------------------
        # advisory_lock
        #
        # Must run inside an explicit transaction (conn.autocommit = False).
        # Params: (key,)
        # ------------------------------------------------------------------
        self.advisory_lock: str = (
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0));"
        )

        # ------------------------------------------------------------------
        # get_item_after_lock double-check (after acquiring advisory lock)
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

        # ------------------------------------------------------------------
        # get_many_items
        #
        # Renews sliding expiration on read via UPDATE + RETURNING.
        # Params: (utcNow, keys_array, utcNow)
        # ------------------------------------------------------------------
        self.get_many_items: str = (
            f"UPDATE {qualified}"
            f" SET {COL_EXPIRES_AT} = CASE"
            f"     WHEN {COL_SLIDING_SECONDS} IS NOT NULL"
            f"     THEN LEAST("
            f"         %s + ({COL_SLIDING_SECONDS} * INTERVAL '1 second'),"
            f"         COALESCE({COL_ABSOLUTE_EXPIRATION}, 'infinity'::timestamptz)"
            f"     )"
            f"     ELSE {COL_EXPIRES_AT}"
            f" END"
            f" WHERE {COL_ID} = ANY(%s::varchar(449)[])"
            f"   AND {COL_EXPIRES_AT} >= %s"
            f" RETURNING {COL_ID}, {COL_VALUE};"
        )

        # ------------------------------------------------------------------
        # delete_many_items
        #
        # Params: (keys_array,)
        # ------------------------------------------------------------------
        self.delete_many_items: str = (
            f"DELETE FROM {qualified} WHERE {COL_ID} = ANY(%s::varchar(449)[]);"
        )

        # ------------------------------------------------------------------
        # get_by_pattern
        #
        # Renews sliding expiration on read via UPDATE + RETURNING.
        # Params: (utcNow, pattern, utcNow)
        # ------------------------------------------------------------------
        self.get_by_pattern: str = (
            f"UPDATE {qualified}"
            f" SET {COL_EXPIRES_AT} = CASE"
            f"     WHEN {COL_SLIDING_SECONDS} IS NOT NULL"
            f"     THEN LEAST("
            f"         %s + ({COL_SLIDING_SECONDS} * INTERVAL '1 second'),"
            f"         COALESCE({COL_ABSOLUTE_EXPIRATION}, 'infinity'::timestamptz)"
            f"     )"
            f"     ELSE {COL_EXPIRES_AT}"
            f" END"
            f" WHERE {COL_ID} LIKE %s"
            f"   AND {COL_EXPIRES_AT} >= %s"
            f" RETURNING {COL_ID}, {COL_VALUE};"
        )

        # ------------------------------------------------------------------
        # delete_by_pattern
        #
        # Params: (pattern,)
        # ------------------------------------------------------------------
        self.delete_by_pattern: str = (
            f"DELETE FROM {qualified} WHERE {COL_ID} LIKE %s;"
        )
