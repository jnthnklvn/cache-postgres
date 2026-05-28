# postgres-cache

> Distributed cache over PostgreSQL with stampede protection — Python port of `Microsoft.Extensions.Caching.Postgres`.

## Installation

```bash
pip install postgres-cache
```

## Quick Start

```python
from postgres_cache import PostgresCache, PostgresCacheOptions

options = PostgresCacheOptions(
    dsn="postgresql://user:password@localhost:5432/mydb",
    schema="public",
    table="cache",
    create_if_not_exists=True,
)

# As a context manager (recommended — manages background scanner thread)
with PostgresCache(options) as cache:
    cache.set("my-key", b"my-value")
    value = cache.get("my-key")   # returns bytes | None
    cache.refresh("my-key")
    cache.remove("my-key")

# get_or_create with stampede protection (advisory lock)
with PostgresCache(options) as cache:
    value = cache.get_or_create("my-key", lambda: b"computed-value")
```

## Configuration

```python
from postgres_cache import PostgresCacheOptions, EntryOptions
from datetime import timedelta

options = PostgresCacheOptions(
    dsn="postgresql://...",
    schema="public",
    table="cache",
    create_if_not_exists=True,       # auto-create table on first use
    use_wal=False,                    # UNLOGGED table (faster, not crash-safe)
    expiration_scan_frequency=timedelta(minutes=5),  # background scan interval
    enable_expiration_scan=True,      # run background expiration scanner
)

entry_opts = EntryOptions(
    sliding_expiration=timedelta(minutes=20),
    absolute_expiration_relative_to_now=timedelta(hours=1),
)

with PostgresCache(options) as cache:
    cache.set("key", b"value", entry_opts)
```

## Connection modes

```python
# Mode 1: DSN string (library creates connections internally)
options = PostgresCacheOptions(dsn="postgresql://...", schema="public", table="cache")

# Mode 2: connection factory (you control the pool)
import psycopg2
options = PostgresCacheOptions(
    connection_factory=lambda: psycopg2.connect("postgresql://..."),
    schema="public",
    table="cache",
)
```

## Running tests

```bash
# Unit tests (no database required)
pytest tests/unit/

# Integration tests (requires PostgreSQL)
export PGCACHE_DSN="postgresql://user:password@localhost:5432/testdb"
pytest tests/integration/ -m integration
```

## License

MIT
