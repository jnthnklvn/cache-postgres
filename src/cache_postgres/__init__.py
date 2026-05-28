"""
cache-postgres — Distributed cache over PostgreSQL with stampede protection.

Public API:
  - PostgresCache: main cache class (context manager recommended)
  - PostgresCacheOptions: configuration dataclass
  - EntryOptions: per-entry TTL options

Example::

    from cache_postgres import PostgresCache, PostgresCacheOptions, EntryOptions
    from datetime import timedelta

    options = PostgresCacheOptions(
        dsn="postgresql://user:pass@localhost:5432/mydb",
        schema="public",
        table="cache",
        create_if_not_exists=True,
    )

    with PostgresCache(options) as cache:
        cache.set("key", b"value", EntryOptions(sliding_expiration=timedelta(minutes=20)))
        data = cache.get("key")        # bytes | None
        cache.refresh("key")
        cache.remove("key")

        # stampede-safe get-or-create (advisory lock)
        result = cache.get_or_create("key", lambda: b"expensive-value")

Spec: _reversa_sdd/migration/topology_decision.md § Implicações para a Fase 2
"""

from ._cache import PostgresCache
from ._async_cache import AsyncPostgresCache
from ._options import EntryOptions, PostgresCacheOptions

__all__ = [
    "PostgresCache",
    "AsyncPostgresCache",
    "PostgresCacheOptions",
    "EntryOptions",
]

__version__ = "0.1.0"
