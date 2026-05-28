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

# Decorator API for synchronous functions
with PostgresCache(options) as cache:
    @cache.cached(key="user:{user_id}", ttl="10m", tags=["users"])
    def get_user(user_id: int):
        return {"id": user_id, "name": f"User {user_id}"}
        
    user = get_user(1) # Fetched and cached
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
    expiration_scan_interval=timedelta(minutes=5),  # background scan interval
    enable_expiration_scan=True,      # run background expiration scanner
)

# Use concise duration strings or standard timedeltas
entry_opts = EntryOptions(
    sliding_expiration="20m",
    absolute_expiration_relative="1h",
    tags=["org:123"]
)

with PostgresCache(options) as cache:
    cache.set("key", b"value", entry_opts)
```

## Tag-Based Invalidation

You can assign tags to cache entries and later delete them in bulk. This is highly effective for invalidating complex relationship queries.

```python
with PostgresCache(options) as cache:
    # Tag multiple entries
    cache.set("product:1", b"data", EntryOptions(tags=["products", "category:electronics"]))
    cache.set("product:2", b"data", EntryOptions(tags=["products", "category:home"]))
    
    # Invalidate all electronics instantly
    cache.delete_tags("category:electronics")
    
    # Or invalidate all products
    cache.delete_tags("products")
```

## Connection modes

```python
# Mode 1: DSN string (library creates and manages a ConnectionPool internally)
options = PostgresCacheOptions(
    dsn="postgresql://...", 
    pool_min_size=1,
    pool_max_size=10,
    schema="public", 
    table="cache"
)

# Mode 2: connection factory (you control the connection or pool)
import psycopg
options = PostgresCacheOptions(
    connection_factory=lambda: psycopg.connect("postgresql://..."),
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
