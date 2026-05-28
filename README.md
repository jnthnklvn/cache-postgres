# postgres-cache

> Distributed cache over PostgreSQL with stampede protection — Python port of `Microsoft.Extensions.Caching.Postgres`.

## Installation

```bash
pip install postgres-cache
```

## Quick Start

```python
from postgres_cache import PostgresCache, AsyncPostgresCache, PostgresCacheOptions

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

# Fully asynchronous support is also available
async def main():
    async with AsyncPostgresCache(options) as cache:
        await cache.set("my-key", b"my-value")
        value = await cache.get("my-key")

# Decorator API for synchronous functions
with PostgresCache(options) as cache:
    @cache.cached(key="user:{user_id}", ttl="10m", tags=["users"])
    def get_user(user_id: int):
        return {"id": user_id, "name": f"User {user_id}"}
        
    user = get_user(1) # Fetched and cached

# Decorator API for asynchronous functions
async def setup_async():
    async with AsyncPostgresCache(options) as cache:
        @cache.cached(key="async_user:{user_id}", ttl="10m", tags=["users"])
        async def get_user_async(user_id: int):
            return {"id": user_id, "name": f"Async User {user_id}"}
            
        user = await get_user_async(1)
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

## Bulk Operations

Performant operations to interact with multiple keys in a single database round-trip.

```python
cache.set_many({
    "key1": b"value1",
    "key2": b"value2",
})

# Missing keys are omitted from the returned dictionary
results = cache.get_many(["key1", "key2", "missing_key"])
# {"key1": b"value1", "key2": b"value2"}

cache.delete_many(["key1", "key2"])
```

## Advanced Resiliency Decorators

Postgres-Cache brings advanced patterns inspired by modern caching tools. Both synchronous (`PostgresCache`) and asynchronous (`AsyncPostgresCache`) facades support these decorators!

### `@failover`

Never fail a request just because a backend service goes down. The `@failover` decorator will catch specific exceptions and serve a **stale** cache entry (ignoring its expiration) if one is available.

```python
from postgres_cache import PostgresCache, PostgresCacheOptions
import requests

options = PostgresCacheOptions(...)
cache = PostgresCache(options)

@cache.failover("weather:{city}", ttl="1h", exceptions=(requests.RequestException,))
def get_weather(city: str) -> dict:
    # If this raises a RequestException, the cache will serve 
    # the last known data for this city!
    response = requests.get(f"https://weather.example.com/{city}")
    response.raise_for_status()
    return response.json()
```

### `@early`

Eliminate cache stampedes by proactively refreshing hot keys *before* they expire. The `@early` decorator serves the cached data immediately while spawning a background task to refresh it.

```python
# The cache entry lives for 10 minutes, but if it is requested within the last 
# 3 minutes of its life, it is refreshed in the background.
@cache.early("expensive_data:{id}", ttl="10m", early_ttl="7m")
def get_expensive_data(id: int) -> dict:
    return compute_very_expensive_data(id)
```

## Pattern Matching

Emulate Redis-like wildcard operations using standard SQL `LIKE` syntax natively.

```python
# Create multiple entries
cache.set_many({
    "user:1": b"Alice",
    "user:2": b"Bob",
    "admin:1": b"Charlie"
})

# Get all matching
users = cache.get_pattern("user:%")  # {"user:1": b"Alice", "user:2": b"Bob"}

# Delete all matching
deleted_count = cache.delete_pattern("user:%") # 2
```

## Distributed Primitives (Locks & Counters)

Postgres-Cache provides synchronization mechanisms directly built into Postgres, without requiring Redis or ZooKeeper.

### Atomic Counters

Increment a value atomically without race conditions.
> [!IMPORTANT]
> The database stores the raw integer bytes via UTF-8 serialization. If you call `cache.get("site_hits")` directly, you will receive bytes (e.g. `b'42'`). Use `int(cache.get("site_hits").decode('utf-8'))` if you need the integer form.

```python
# Returns 1 (initializes key if it doesn't exist)
hits = cache.incr("site_hits")

# Increment by 5
hits = cache.incr("site_hits", 5)  # Returns 6
```

### Distributed Locks

Coordinate tasks horizontally across processes via an atomic, distributed context manager. 
The lock requires a time-to-live to ensure no deadlocks occur if a worker crashes.

```python
with cache.lock("expensive_background_job", expire="30s"):
    # Guaranteed exclusive access!
    # If another process arrives, it will block until this completes or expires.
    process_data()
```

You can also use lower-level methods directly: `cache.set_lock`, `cache.unlock`, `cache.is_locked`.

## Thread Safety / Async Safety

```python
# Mode 1: DSN string (library creates and manages a ConnectionPool internally)
options = PostgresCacheOptions(
    dsn="postgresql://...", 
    pool_min_size=1,
    pool_max_size=10,
    schema="public", 
    table="cache"
)

# Mode 2: synchronous connection factory
import psycopg
options = PostgresCacheOptions(
    connection_factory=lambda: psycopg.connect("postgresql://..."),
    schema="public",
    table="cache",
)

# Mode 3: asynchronous connection factory (used exclusively by AsyncPostgresCache)
import psycopg
async def make_conn():
    return await psycopg.AsyncConnection.connect("postgresql://...")

options = PostgresCacheOptions(
    async_connection_factory=make_conn,
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
