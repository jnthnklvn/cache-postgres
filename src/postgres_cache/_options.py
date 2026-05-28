"""
Configuration dataclasses for cache-postgres.

Spec: _reversa_sdd/migration/target_architecture.md § BC-3 (Configuration)
      _reversa_sdd/migration/target_domain_model.md § PostgresCacheOptions, EntryOptions
      _reversa_sdd/migration/target_business_rules.md § BR-MIGRAR-001, BR-MIGRAR-010

Both dataclasses are treated as immutable after construction.
Validation is performed in __post_init__ — invalid combinations raise ValueError.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Awaitable

from ._utils import parse_duration


#: Minimum allowed scanner interval — BR-MIGRAR-001
_MIN_SCAN_INTERVAL: timedelta = timedelta(minutes=5)

#: Default scanner interval — target_domain_model.md § PostgresCacheOptions
_DEFAULT_SCAN_INTERVAL: timedelta = timedelta(minutes=30)

#: Default sliding expiration — target_domain_model.md § PostgresCacheOptions
_DEFAULT_SLIDING_EXPIRATION: timedelta = timedelta(minutes=20)


@dataclass
class PostgresCacheOptions:
    """Configuration for a PostgresCache instance.

    Equivalent to PostgresCacheOptions.cs in the legacy C# code.
    All fields validated in __post_init__.

    Connection modes (BR-MIGRAR-010, in priority order):
      1. ``connection_factory`` — callable that returns a psycopg connection.
         The library does **not** close connections it did not create.
      2. ``dsn`` — connection string; library creates and manages a ConnectionPool.
      3. Neither → ValueError at construction time.

    Example::

        options = PostgresCacheOptions(
            dsn="postgresql://user:pass@localhost:5432/mydb",
            schema="public",
            table="cache",
            create_if_not_exists=True,
        )

    Spec: target_domain_model.md § Value Objects § PostgresCacheOptions
          target_business_rules.md § BR-MIGRAR-001, BR-MIGRAR-010
    """

    # ------------------------------------------------------------------
    # Connection (BR-MIGRAR-010)
    # ------------------------------------------------------------------

    dsn: str | None = None
    """PostgreSQL DSN string (e.g. ``postgresql://user:pass@host/db``).
    Used when ``connection_factory`` is None.
    """

    connection_factory: Callable[[], object] | None = None
    """Callable returning a psycopg connection.
    When provided, takes priority over ``dsn``.
    The library does **not** call ``close()`` on returned connections.
    """

    async_connection_factory: Callable[[], Awaitable[object]] | None = None
    """Callable returning an awaitable psycopg.AsyncConnection.
    Used exclusively by AsyncPostgresCache.
    """

    pool_min_size: int = 1
    """Minimum number of connections in the ConnectionPool (used with dsn)."""

    pool_max_size: int = 10
    """Maximum number of connections in the ConnectionPool (used with dsn)."""

    # ------------------------------------------------------------------
    # Table location (BR-MIGRAR-008)
    # ------------------------------------------------------------------

    schema: str = "public"
    """PostgreSQL schema that contains the cache table."""

    table: str = "cache"
    """Name of the cache table."""

    # ------------------------------------------------------------------
    # DDL (BR-MIGRAR-006, BR-MIGRAR-017)
    # ------------------------------------------------------------------

    create_if_not_exists: bool = False
    """If True, create the cache table automatically on first use (DDL)."""

    use_wal: bool = False
    """If False (default), create an UNLOGGED table (faster, not crash-safe).
    If True, create a regular WAL-logged table.
    Maps to inverted ``UseWAL`` option in the legacy C#.
    """

    # ------------------------------------------------------------------
    # Expiration scanner (BR-MIGRAR-001, BR-MIGRAR-005)
    # ------------------------------------------------------------------

    expiration_scan_interval: timedelta = field(
        default_factory=lambda: _DEFAULT_SCAN_INTERVAL
    )
    """Minimum interval between background expiration scans.
    Must be >= 5 minutes (BR-MIGRAR-001).
    Equivalent to ExpiredItemsDeletionInterval in the legacy C#.
    """

    enable_expiration_scan: bool = True
    """If True (default), start a background daemon thread to delete
    expired entries. Set to False to disable background scanning entirely.
    """

    # ------------------------------------------------------------------
    # Default entry TTL (BR-MIGRAR-003)
    # ------------------------------------------------------------------

    default_sliding_expiration: timedelta = field(
        default_factory=lambda: _DEFAULT_SLIDING_EXPIRATION
    )
    """Default sliding expiration applied when EntryOptions provides none.
    Must be > 0.
    Equivalent to DefaultSlidingExpiration in the legacy C#.
    """

    def __post_init__(self) -> None:
        """Validate all fields after construction.

        Raises:
            ValueError: On any invalid combination or out-of-range value.
        """
        self._validate_connection()
        self._validate_schema_table()
        self._validate_scan_interval()
        self._validate_default_sliding()

    # ------------------------------------------------------------------
    # Private validators
    # ------------------------------------------------------------------

    def _validate_connection(self) -> None:
        """Exactly one of dsn, connection_factory, or async_connection_factory must be provided.

        Spec: target_domain_model.md § Modos de conexão (BR-MIGRAR-010)
        """
        sources = sum(
            1 for x in (self.dsn, self.connection_factory, self.async_connection_factory) if x is not None
        )
        if sources == 0:
            raise ValueError(
                "PostgresCacheOptions requires exactly one connection source. "
                "Provide 'dsn', 'connection_factory', or 'async_connection_factory'."
            )
        if sources > 1:
            raise ValueError(
                "Provide exactly one of 'dsn', 'connection_factory', or 'async_connection_factory'."
            )

    def _validate_schema_table(self) -> None:
        """Schema and table must be non-empty strings."""
        if not self.schema or not self.schema.strip():
            raise ValueError("'schema' must be a non-empty string.")
        if not self.table or not self.table.strip():
            raise ValueError("'table' must be a non-empty string.")

    def _validate_scan_interval(self) -> None:
        """Scanner interval must be >= 5 minutes — BR-MIGRAR-001."""
        if self.expiration_scan_interval < _MIN_SCAN_INTERVAL:
            raise ValueError(
                f"'expiration_scan_interval' must be >= {_MIN_SCAN_INTERVAL} "
                f"(got {self.expiration_scan_interval}). "
                "Values smaller than 5 minutes are rejected — BR-MIGRAR-001."
            )

    def _validate_default_sliding(self) -> None:
        """Default sliding expiration must be > 0."""
        if self.default_sliding_expiration <= timedelta(0):
            raise ValueError(
                "'default_sliding_expiration' must be positive "
                f"(got {self.default_sliding_expiration})."
            )


@dataclass
class EntryOptions:
    """Per-entry TTL options passed to set() and get_or_create().

    Equivalent to DistributedCacheEntryOptions from the .NET standard library.
    All fields are optional — omitting all means the cache entry does not expire
    (uses the server-side default if any).

    ``absolute_expiration`` and ``absolute_expiration_relative`` are
    mutually exclusive — providing both raises ValueError.

    Example::

        from datetime import timedelta
        opts = EntryOptions(sliding_expiration=timedelta(minutes=20))

        from datetime import datetime, timezone
        opts = EntryOptions(
            absolute_expiration=datetime(2030, 1, 1, tzinfo=timezone.utc)
        )

    Spec: target_domain_model.md § Value Objects § EntryOptions
    """

    sliding_expiration: timedelta | str | None = None
    """Sliding window that resets on each access.
    Equivalent to SlidingExpiration in DistributedCacheEntryOptions.
    Can be a timedelta or a duration string like '10m'.
    """

    absolute_expiration: datetime | None = None
    """Hard cutoff as a timezone-aware UTC datetime.
    Equivalent to AbsoluteExpiration in DistributedCacheEntryOptions.
    RISK-004: must be tz-aware (timezone.utc). Naive datetimes raise ValueError.
    """

    absolute_expiration_relative: timedelta | str | None = None
    """Hard cutoff as a duration from now.
    Equivalent to AbsoluteExpirationRelativeToNow.
    Can be a timedelta or a duration string like '2h'.
    Converted to absolute time at the moment of the set() call.
    """

    tags: list[str] | None = None
    """List of tags for group invalidation."""

    def __post_init__(self) -> None:
        """Validate mutual exclusivity and timezone-awareness.

        Raises:
            ValueError: When both absolute fields are set, or when
                        absolute_expiration is a naive datetime (RISK-004).
        """
        if isinstance(self.sliding_expiration, str):
            self.sliding_expiration = parse_duration(self.sliding_expiration)
        if isinstance(self.absolute_expiration_relative, str):
            self.absolute_expiration_relative = parse_duration(self.absolute_expiration_relative)

        if self.absolute_expiration is not None and self.absolute_expiration_relative is not None:
            raise ValueError(
                "'absolute_expiration' and 'absolute_expiration_relative' are "
                "mutually exclusive. Provide at most one."
            )
        if self.absolute_expiration is not None:
            if self.absolute_expiration.tzinfo is None:
                raise ValueError(
                    "'absolute_expiration' must be timezone-aware (tz=timezone.utc). "
                    "Naive datetimes are rejected — RISK-004."
                )

    def resolve_expires_at(self, now: datetime | None = None) -> datetime | None:
        """Compute the absolute expiration timestamp for a set() call.

        Args:
            now: Current UTC time. Defaults to datetime.now(tz=timezone.utc).
                 Injected for testability.

        Returns:
            A tz-aware datetime representing the hard expiration ceiling,
            or None if no absolute expiration is configured.

        Spec: target_data_model.md § Mapeamento de tipos (expiresattime)
              RISK-004 — all datetimes must be tz-aware UTC.
        """
        if now is None:
            now = datetime.now(tz=timezone.utc)
        if self.absolute_expiration is not None:
            return self.absolute_expiration
        if self.absolute_expiration_relative is not None:
            return now + self.absolute_expiration_relative
        return None

    def resolve_sliding_seconds(self) -> int | None:
        """Return sliding_expiration as whole seconds, or None.

        Spec: target_data_model.md § slidingexpirationinseconds (BIGINT)
        """
        if self.sliding_expiration is None:
            return None
        return int(self.sliding_expiration.total_seconds())
