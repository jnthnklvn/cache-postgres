"""
Database access layer for postgres-cache.

Spec: _reversa_sdd/migration/target_architecture.md § BC-2, DA-01, DA-05, DA-06, DA-07
      _reversa_sdd/migration/target_domain_model.md § DatabaseOperations
      _reversa_sdd/migration/target_business_rules.md § BR-MIGRAR-001 to BR-MIGRAR-019
      _reversa_sdd/migration/risk_register.md § RISK-003, RISK-004, RISK-006

Critical invariants enforced here:
  - RISK-004: ALL datetimes use datetime.now(tz=timezone.utc) — NEVER naive.
  - RISK-006: pg_advisory_xact_lock MUST run inside an explicit transaction
              (conn.autocommit = False before the call).
  - BR-MIGRAR-006: DDL executed at most once per instance (double-checked locking
                   via threading.Lock + _table_created flag).
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Callable

import psycopg2
import psycopg2.extensions

from ._options import EntryOptions, PostgresCacheOptions
from ._sql import SqlQueries

logger = logging.getLogger(__name__)  # DA-07 / BR-MIGRAR-011


class DatabaseOperations:
    """All direct interactions with PostgreSQL via psycopg2.

    Instantiated internally by PostgresCache — never by library consumers.
    Manages connections, DDL, CRUD, advisory lock, and expiration scan.

    Spec: target_architecture.md § DA-01 (no DI injection)
          target_domain_model.md § Comandos
    """

    def __init__(self, options: PostgresCacheOptions, sql: SqlQueries) -> None:
        """
        Args:
            options: Validated PostgresCacheOptions.
            sql:     Pre-formatted SqlQueries for this schema/table pair.
        """
        self._options = options
        self._sql = sql

        # Double-checked locking for DDL — BR-MIGRAR-006
        self._table_created: bool = False
        self._ddl_lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Connection management — BR-MIGRAR-010
    # ------------------------------------------------------------------

    def _connect(self) -> psycopg2.extensions.connection:
        """Return a psycopg2 connection according to the configured mode.

        Mode priority (BR-MIGRAR-010):
          1. connection_factory → call it; library does NOT close the result.
          2. dsn → psycopg2.connect(dsn); caller is responsible for close().

        Returns:
            An open psycopg2 connection (autocommit state is caller's responsibility).
        """
        if self._options.connection_factory is not None:
            return self._options.connection_factory()
        return psycopg2.connect(self._options.dsn)

    def _is_owned_connection(self) -> bool:
        """True when the library created the connection (dsn mode)."""
        return self._options.connection_factory is None

    # ------------------------------------------------------------------
    # DDL — BR-MIGRAR-006 (double-checked locking)
    # ------------------------------------------------------------------

    def ensure_table_exists(self) -> None:
        """Create the cache table and index if they do not exist yet.

        Uses double-checked locking so DDL is attempted at most once per
        instance, even under concurrent calls — BR-MIGRAR-006.

        Only runs when options.create_if_not_exists is True.
        """
        if not self._options.create_if_not_exists:
            return
        if self._table_created:
            return
        with self._ddl_lock:
            if self._table_created:
                return
            conn = self._connect()
            try:
                with conn:  # auto-commit on success, rollback on error
                    with conn.cursor() as cur:
                        cur.execute(self._sql.create_schema)
                        cur.execute(self._sql.create_table)
                        cur.execute(self._sql.create_index)
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
            finally:
                if self._is_owned_connection():
                    conn.close()

    # ------------------------------------------------------------------
    # get — BR-MIGRAR-003, BR-MIGRAR-009
    # ------------------------------------------------------------------

    def get(self, key: str) -> bytes | None:
        """Retrieve a cache value by key, renewing sliding expiration atomically.

        Returns None if the key does not exist or has expired (BR-MIGRAR-004).
        Sliding expiration is recalculated in-database in the same UPDATE
        statement (BR-MIGRAR-009, single round-trip).

        RISK-004: utcNow is always datetime.now(tz=timezone.utc).

        Args:
            key: Cache key (≤ 449 characters).

        Returns:
            Cached bytes, or None on miss / expiry.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)  # RISK-004

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(self._sql.get_item, (utc_now, key, utc_now))
                row = cur.fetchone()
            conn.commit()
            return row[0] if row else None
        except Exception:
            conn.rollback()
            logger.exception("get(%r) failed.", key)
            raise
        finally:
            if self._is_owned_connection():
                conn.close()

    # ------------------------------------------------------------------
    # set — BR-MIGRAR-003, BR-MIGRAR-007
    # ------------------------------------------------------------------

    def set(
        self,
        key: str,
        value: bytes,
        options: EntryOptions | None = None,
    ) -> None:
        """Insert or update a cache entry (UPSERT via CTE ON CONFLICT).

        Computes expiration timestamps at call time using UTC now.

        RISK-004: ALL timestamps are tz-aware UTC.
        BR-MIGRAR-007: UPSERT is atomic via CTE with ON CONFLICT DO UPDATE.

        Args:
            key:     Cache key.
            value:   Raw bytes to store.
            options: Per-entry TTL options. When None, the default sliding
                     expiration from PostgresCacheOptions is applied.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)  # RISK-004

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

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    self._sql.set_item,
                    (key, value, expires_at, sliding_secs, abs_exp),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("set(%r) failed.", key)
            raise
        finally:
            if self._is_owned_connection():
                conn.close()

    # ------------------------------------------------------------------
    # refresh — BR-MIGRAR-003, BR-MIGRAR-009
    # ------------------------------------------------------------------

    def refresh(self, key: str) -> None:
        """Renew the sliding expiration of a cache entry without returning its value.

        If the entry is expired or absent, this is a no-op.

        RISK-004: utcNow is always tz-aware UTC.

        Args:
            key: Cache key.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)  # RISK-004

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(self._sql.refresh_item, (utc_now, key, utc_now))
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("refresh(%r) failed.", key)
            raise
        finally:
            if self._is_owned_connection():
                conn.close()

    # ------------------------------------------------------------------
    # remove — BR-MIGRAR-003
    # ------------------------------------------------------------------

    def remove(self, key: str) -> None:
        """Physically delete a cache entry by key.

        Args:
            key: Cache key.
        """
        self.ensure_table_exists()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(self._sql.remove_item, (key,))
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("remove(%r) failed.", key)
            raise
        finally:
            if self._is_owned_connection():
                conn.close()

    # ------------------------------------------------------------------
    # delete_expired — BR-MIGRAR-018 (background scanner)
    # ------------------------------------------------------------------

    def delete_expired(self) -> int:
        """Delete all expired entries in a single batch statement.

        Called by the background scanner thread in PostgresCache.

        RISK-004: utcNow is always tz-aware UTC.

        Returns:
            Number of rows deleted.
        """
        utc_now = datetime.now(tz=timezone.utc)  # RISK-004
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(self._sql.delete_expired, (utc_now,))
                count = cur.rowcount
            conn.commit()
            if count > 0:
                logger.debug("Deleted %d expired cache entries.", count)
            return count
        except Exception:
            conn.rollback()
            logger.exception("delete_expired() failed.")
            raise
        finally:
            if self._is_owned_connection():
                conn.close()

    # ------------------------------------------------------------------
    # get_or_create — BR-MIGRAR-002 (stampede protection via advisory lock)
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

        ⚠️ RISK-006: conn.autocommit MUST be False before pg_advisory_xact_lock.
                     The lock is transactional — it releases on COMMIT/ROLLBACK.
        ⚠️ RISK-004: All timestamps are tz-aware UTC.

        Implementation pattern:
          1. Open connection, disable autocommit (RISK-006).
          2. Acquire advisory lock on hash(key).
          3. Double-check: re-read after lock (another worker may have set it).
          4. If still miss: call factory(), write result, return it.
          5. Commit (releases advisory lock).

        Args:
            key:     Cache key.
            factory: Zero-argument callable that produces the value bytes.
            options: Per-entry TTL options.

        Returns:
            The cached or freshly computed bytes value.
        """
        self.ensure_table_exists()
        utc_now = datetime.now(tz=timezone.utc)  # RISK-004

        conn = self._connect()
        owned = self._is_owned_connection()
        try:
            # RISK-006: explicit transaction required for pg_advisory_xact_lock
            conn.autocommit = False

            with conn.cursor() as cur:
                # Step 1: acquire advisory lock (blocks until available)
                cur.execute(self._sql.advisory_lock, (key,))

                # Step 2: double-check after acquiring lock
                cur.execute(self._sql.get_item_after_lock, (key, utc_now))
                row = cur.fetchone()

            if row is not None:
                # Another worker already populated the entry
                conn.commit()
                return row[0]

            # Step 3: cache miss even after lock — call factory once
            value = factory()

            # Step 4: store the computed value
            self.set(key, value, options)

            conn.commit()
            return value

        except Exception:
            conn.rollback()
            logger.exception("get_or_create(%r) failed.", key)
            raise
        finally:
            if owned:
                conn.close()
