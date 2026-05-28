# cache-postgres

> **Distributed cache over PostgreSQL with stampede protection** — A high-performance, feature-rich Python port of .NET's `Microsoft.Extensions.Caching.Postgres`.

[![PyPI version](https://img.shields.io/pypi/v/cache-postgres.svg?color=blue)](https://pypi.org/project/cache-postgres/)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/cache-postgres.svg)](https://pypi.org/project/cache-postgres/)
[![License](https://img.shields.io/github/license/jnthnklvn/cache-postgres.svg?color=green)](https://github.com/jnthnklvn/cache-postgres/blob/main/LICENSE)
[![Tests Status](https://img.shields.io/github/actions/workflow/status/jnthnklvn/cache-postgres/tests.yml?branch=main&label=tests)](https://github.com/jnthnklvn/cache-postgres/actions)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

---

## Table of Contents

- [Overview](#overview)
- [Why cache-postgres?](#why-cache-postgres)
- [Key Features](#key-features)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Synchronous Cache](#synchronous-cache)
  - [Asynchronous Cache](#asynchronous-cache)
  - [Decorator API](#decorator-api)
- [Configuration](#configuration)
  - [Configuration Options](#configuration-options)
  - [Entry Options (TTL)](#entry-options-ttl)
- [Core Features Walkthrough](#core-features-walkthrough)
  - [Stampede Protection](#stampede-protection)
  - [Tag-Based Invalidation](#tag-based-invalidation)
  - [Bulk Operations](#bulk-operations)
  - [Failover Decorator (`@failover`)](#failover-decorator-failover)
  - [Early Expiration Decorator (`@early`)](#early-expiration-decorator-early)
  - [Pattern Matching](#pattern-matching)
- [Distributed Primitives](#distributed-primitives)
  - [Atomic Counters](#atomic-counters)
  - [Distributed Locks](#distributed-locks)
- [Thread & Async Safety](#thread--async-safety)
- [Performance & Benchmarking](#performance--benchmarking)
- [Database Schema (DDL)](#database-schema-ddl)
- [Interactive Demo (Docker Compose)](#interactive-demo-docker-compose)
- [Development & Testing](#development--testing)
- [License](#license)

---

## Overview

`cache-postgres` is a modern, **experimental** distributed caching library for Python 3.10+ using PostgreSQL as the storage backend. Designed to be lightweight and extremely robust, it bridges the gap between simple in-memory caches and heavy infrastructure dependencies like Redis or Memcached. 

With native support for both **synchronous** and **asynchronous** paradigms, it is suitable for any modern Python web application (e.g., FastAPI, Django, Flask, Sanic).

This project is based on [Microsoft.Extensions.Caching.Postgres](https://github.com/Azure/Microsoft.Extensions.Caching.Postgres), with specifications extracted using [Reversa](https://github.com/sandeco/reversa), and draws inspiration from [Cashews](https://github.com/Krukov/cashews/).

---

## Why cache-postgres?

In many architectural designs, a PostgreSQL database is already deployed, maintained, and backed up. Introducing Redis or Memcached solely to handle caching adds operational complexity, security surfaces, additional infrastructure costs, separate backup policies, and monitoring overhead.

`cache-postgres` allows you to leverage your existing database as a fully-featured distributed cache with:

* **Zero Operational Overhead**: No new infrastructure to deploy, monitor, or pay for.
* **ACID Consistency**: Leverage PostgreSQL's strong consistency guarantees when managing cache states.
* **Cross-Technology Interoperability**: The database schema is **100% compatible** with the official C# .NET SQL caching (`Microsoft.Extensions.Caching.Postgres`). This enables Python and .NET microservices to share the exact same caching table seamlessly!
* **High Performance**: Built on `psycopg` (v3) with native connection pooling, indexing, and support for `UNLOGGED` tables (bypassing WAL writing for ephemeral speed matching in-memory performance).

---

## Key Features

* **🚀 High Performance**: Built with `psycopg3` and native connection pooling. Supports `UNLOGGED` tables for high-throughput, low-latency writes.
* **🛡️ Cache Stampede Protection**: Built-in lock-based `get_or_create` utilizing PostgreSQL Advisory Locks to guarantee that only one worker regenerates a cache entry on a miss.
* **⚡ Async & Sync Facades**: Double API layout with fully native asynchronous (`AsyncPostgresCache`) and synchronous (`PostgresCache`) classes.
* **🏷️ Tag-Based Invalidation**: Group cache entries using tag arrays and invalidate whole groups instantly (e.g., all entries tagged with `products`).
* **📦 Bulk Operations**: Highly optimized `get_many`, `set_many`, and `delete_many` to interact with multiple keys in a single database round-trip.
* **🩹 Downstream Resiliency (`@failover`)**: Protect your services. If downstream APIs or databases go down, serve stale cache data automatically rather than returning errors.
* **⏰ Proactive Hot Key Refresh (`@early`)**: Proactively refreshes hot cache keys in the background before they expire, eliminating latency spikes.
* **🔍 Wildcard Pattern Matching**: Fetch or delete keys matching SQL wildcard patterns (e.g., `user:%`).
* **🔒 Distributed Locks & Counters**: Built-in distributed locks and atomic counters, eliminating the need for ZooKeeper or Redis.
* **🧹 Automatic Expiration Scanner**: A lightweight, automatic background worker thread that continuously cleans up expired entries to keep database size optimal.

---

## Installation

Install the package via pip:

```bash
pip install cache-postgres
```

Ensure you have a PostgreSQL database available (version 13+ recommended for optimal index deduplication).

---

## Quick Start

### Synchronous Cache

Recommended as a context manager to manage background scanner threads cleanly:

```python
from cache_postgres import PostgresCache, PostgresCacheOptions

options = PostgresCacheOptions(
    dsn="postgresql://user:password@localhost:5432/mydb",
    schema="public",
    table="cache",
    create_if_not_exists=True,
)

# Using as a context manager starts and stops the background scanner
with PostgresCache(options) as cache:
    # Set and get values (keys are strings, values must be bytes)
    cache.set("my-key", b"my-value")
    value = cache.get("my-key")  # returns b"my-value" or None
    
    # Check, refresh, and remove keys
    cache.refresh("my-key")  # Updates sliding expiration TTL
    cache.remove("my-key")
```

### Asynchronous Cache

Fully native asynchronous support using Python's standard `async with` syntax:

```python
import asyncio
from cache_postgres import AsyncPostgresCache, PostgresCacheOptions

options = PostgresCacheOptions(
    dsn="postgresql://user:password@localhost:5432/mydb",
    create_if_not_exists=True,
)

async def main():
    async with AsyncPostgresCache(options) as cache:
        await cache.set("async-key", b"hello-async")
        value = await cache.get("async-key")
        print(value)  # b"hello-async"

asyncio.run(main())
```

### Decorator API

Cache the return value of functions automatically based on their arguments. Works for both sync and async functions:

```python
with PostgresCache(options) as cache:
    @cache.cached(key="user:{user_id}", ttl="10m", tags=["users"])
    def get_user(user_id: int):
        # Only runs if user_id is not cached
        return {"id": user_id, "name": f"User {user_id}"}
        
    user = get_user(42)  # Fetched and cached
    user = get_user(42)  # Served directly from cache
```

For asynchronous functions:

```python
async with AsyncPostgresCache(options) as cache:
    @cache.cached(key="async_user:{user_id}", ttl="10m", tags=["users"])
    async def get_user_async(user_id: int):
        return {"id": user_id, "name": f"Async User {user_id}"}
        
    user = await get_user_async(42)
```

---

## Configuration

### Configuration Options

The `PostgresCacheOptions` dataclass allows you to fine-tune the connection pool, thread management, and table policies:

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `dsn` | `str \| None` | `None` | PostgreSQL connection string (DSN). |
| `connection_factory` | `Callable[[], Connection] \| None` | `None` | Sync connection creator function. Ignored if `dsn` is set. |
| `async_connection_factory` | `Callable[[], Awaitable[AsyncConnection]] \| None` | `None` | Async connection creator. Ignored if `dsn` is set. |
| `schema` | `str` | `"public"` | PostgreSQL schema name. |
| `table` | `str` | `"cache"` | PostgreSQL table name. |
| `create_if_not_exists` | `bool` | `True` | Automatically create the table and indexes on first startup. |
| `use_wal` | `bool` | `False` | If `False` (default), the table is created as `UNLOGGED` (highly performant, bypassed WAL logs). Set to `True` for full durability. |
| `enable_expiration_scan` | `bool` | `True` | Spawns a background thread/task to prune expired records. |
| `expiration_scan_interval` | `timedelta` | `5 min` | Frequency of background table cleanups. |
| `pool_min_size` | `int` | `1` | Minimum connection pool size (used when `dsn` is provided). |
| `pool_max_size` | `int` | `10` | Maximum connection pool size. |

Example:

```python
from datetime import timedelta
from cache_postgres import PostgresCacheOptions

options = PostgresCacheOptions(
    dsn="postgresql://user:password@localhost:5432/mydb",
    use_wal=True,                             # Full crash durability
    enable_expiration_scan=True,
    expiration_scan_interval=timedelta(minutes=10),
    pool_max_size=20,
)
```

### Entry Options (TTL)

You can customize expiration logic per-key using `EntryOptions`. You can provide absolute expirations, sliding expirations, or strings representing intervals (e.g. `"20m"`, `"1h"`):

```python
from cache_postgres import EntryOptions
from datetime import timedelta

# Option 1: Absolute expiration
entry_opts = EntryOptions(absolute_expiration_relative="1h")

# Option 2: Sliding expiration (extends each time it is accessed)
entry_opts = EntryOptions(sliding_expiration="30m")

# Option 3: Sliding expiration with an absolute ceiling
entry_opts = EntryOptions(
    sliding_expiration=timedelta(minutes=15),
    absolute_expiration_relative=timedelta(hours=2)
)

with PostgresCache(options) as cache:
    cache.set("session:123", b"data", entry_opts)
```

---

## Core Features Walkthrough

### Stampede Protection

A **Cache Stampede** (or dog-piling) happens when a hot cache key expires, and multiple application workers concurrently try to query the backend database and rebuild the cache. Under high load, this can overwhelm and crash your database.

`cache-postgres` features automatic stampede protection using database-level advisory locks:

```python
# The lock ensures only one worker executes the lambda function. 
# Other concurrent requests block and safely read the computed value once finished.
value = cache.get_or_create(
    "hot-key",
    lambda: compute_expensive_data(),
    options=EntryOptions(absolute_expiration_relative="10m")
)
```

### Tag-Based Invalidation

You can assign tags to cache entries and later delete them in bulk. This is highly effective for invalidating relational database records or specific domains (e.g. product category pages):

```python
with PostgresCache(options) as cache:
    # Save entries with specific tag metadata
    cache.set("product:101", b"data", EntryOptions(tags=["products", "category:electronics"]))
    cache.set("product:102", b"data", EntryOptions(tags=["products", "category:books"]))
    
    # Invalidate all electronics entries instantly
    cache.delete_tags("category:electronics")
    
    # Or invalidate all products
    cache.delete_tags("products")
```

### Bulk Operations

Database round-trips are the main bottleneck in distributed caching. Fetch or insert multiple keys in a single operation:

```python
# Batch Set
cache.set_many({
    "key:1": b"Alice",
    "key:2": b"Bob",
    "key:3": b"Charlie",
})

# Batch Get (omits missing keys from the returned dict)
results = cache.get_many(["key:1", "key:2", "key:missing"])
# {"key:1": b"Alice", "key:2": b"Bob"}

# Batch Delete
cache.delete_many(["key:1", "key:2"])
```

### Failover Decorator (`@failover`)

Make your application extremely resilient to downstream outages. If your database, third-party API, or dependency raises designated exceptions, the `@failover` decorator will catch it and serve the **expired, stale** cache data if available:

```python
import requests
from cache_postgres import PostgresCache

cache = PostgresCache(options)

# If requests.RequestException is raised, the last successfully cached weather data 
# is returned to the user, ensuring zero outage user-experience.
@cache.failover("weather:{city}", ttl="30m", exceptions=(requests.RequestException,))
def get_weather(city: str) -> dict:
    response = requests.get(f"https://weather.example.com/api/{city}")
    response.raise_for_status()
    return response.json()
```

### Early Expiration Decorator (`@early`)

Proactively refresh hot cache entries before they expire. If a key is requested near its expiration date, it serves the cached data immediately to the user while spawning a background task/thread to asynchronously recalculate the cache:

```python
# The cache entry has an absolute TTL of 10 minutes.
# If requested after 7 minutes (early_ttl), the user receives the cached result instantly 
# with zero delay, while a background worker updates the cache.
@cache.early("expensive-report", ttl="10m", early_ttl="7m")
def get_report():
    return generate_heavy_report()
```

### Pattern Matching

Perform Redis-like wildcard scans natively using SQL `LIKE` syntax:

```python
# Match and fetch keys starting with 'user:'
users = cache.get_pattern("user:%")  # {"user:1": b"Alice", "user:2": b"Bob"}

# Delete keys starting with 'temp:'
deleted_count = cache.delete_pattern("temp:%")
```

---

## Distributed Primitives

### Atomic Counters

Safely increment values atomically without concurrency race conditions:

> [!NOTE]
> Values are stored in the database as UTF-8 string bytes. Reading them via `cache.get()` directly returns the bytes (e.g. `b'42'`). Use `int(cache.get("counter").decode("utf-8"))` or use the returned value of `incr()`.

```python
# Initializes the counter at 1 (if it doesn't exist)
count = cache.incr("hits")  # Returns 1

# Increment by custom offset
count = cache.incr("hits", amount=5)  # Returns 6
```

### Distributed Locks

Coordinate complex tasks across multiple background workers or machines. The lock requires an expiration time to ensure that no deadlock occurs if a worker crashes midway:

```python
# Highly robust distributed locking utilizing PostgreSQL transaction-safe locks
with cache.lock("nightly-billing-sync", expire="10m"):
    # Guaranteed exclusive access across all worker nodes!
    run_billing_sync()
```

---

## Thread & Async Safety

`cache-postgres` is fully safe to be shared across threads or async tasks. Depending on your configuration, connection management scales smoothly:

1. **DSN Mode (Recommended)**:
   The library spins up a `ConnectionPool` (`psycopg.pool.ConnectionPool` or `AsyncConnectionPool`). Connection acquisitions are thread-safe and non-blocking.
   ```python
   options = PostgresCacheOptions(dsn="postgresql://...", pool_max_size=10)
   ```

2. **Connection Factory Mode**:
   Allows you to hook custom connection builders or share your application's database pools.
   ```python
   import psycopg
   options = PostgresCacheOptions(
       connection_factory=lambda: psycopg.connect("postgresql://...")
   )
   ```

---

## Performance & Benchmarking

To validate the real-world performance characteristics and architectural efficiency of `cache-postgres`, we run comprehensive, highly-concurrent benchmarks comparing different library configurations against a standard baseline of **Valkey** (the high-performance, in-memory, Redis-compatible key-value store).

We benchmark four configurations under 20 concurrent async workers across four scenarios:

1. **Valkey Cache (Baseline)**: High-speed, in-memory single-threaded key-value caching.
2. **PG Unlogged (Pool=10)**: `cache-postgres` optimized with `use_wal=False` (unlogged table) and a 10-connection pool.
3. **PG Logged (Pool=10)**: `cache-postgres` with `use_wal=True` (regular table, full WAL durability) and a 10-connection pool.
4. **PG Bottleneck (Pool=1)**: `cache-postgres` with `use_wal=False` but restricted to a single connection to show pool contention.

### Benchmark Results

Here is the pivoted performance matrix captured under concurrent load (20 concurrent workers):

| Configuration Profile | Read-Heavy (90/10) <br> Ops/s \| Avg Latency | Write-Heavy (10/90) <br> Ops/s \| Avg Latency | Atomic Counter (INCR) <br> Ops/s \| Avg Latency | Cache Stampede <br> Avg Latency \| Factory Calls |
| :--- | :--- | :--- | :--- | :--- |
| **Valkey Cache** | 9,647 ops/s \| 1.96 ms | 11,253 ops/s \| 1.76 ms | 12,431 ops/s \| 1.59 ms | 202.00 ms \| **20 calls** |
| **PG Unlogged (Pool=10)** | 1,775 ops/s \| 11.20 ms | **2,120 ops/s** \| **9.36 ms** | **1,926 ops/s** \| **10.30 ms** | 210.85 ms \| **1 call** |
| **PG Logged (Pool=10)** | 1,771 ops/s \| 11.22 ms | 2,033 ops/s \| 9.75 ms | 1,111 ops/s \| 17.84 ms | 209.64 ms \| **1 call** |
| **PG Bottleneck (Pool=1)** | 1,531 ops/s \| 12.93 ms | 1,601 ops/s \| 12.37 ms | 1,504 ops/s \| 13.17 ms | 211.67 ms \| **1 call** |

---

### Architectural Analysis & Explanations

#### 1. Valkey (In-Memory) vs. Postgres (Relational) Performance
Valkey resides entirely in-memory and operates as a single-threaded loop, acting as the performance ceiling. By utilizing unlogged tables, indexing, and raw async psycopg pool architectures, `cache-postgres` reaches **1,700–2,100+ Ops/sec** on standard hardware, placing relational database caching in a high-throughput class suitable for most enterprise web applications.

#### 2. The Power of `UNLOGGED` Tables
When writing heavily (*Write-Heavy* workload), `PG Unlogged` clocks **2,120 Ops/s**, outperforming `PG Logged` (**2,033 Ops/s**) and reducing average write latency to just **9.36 ms**. In the *Atomic Counter* scenario, disabling WAL write sync allows raw Postgres unlogged operations to reach **1,926 Ops/s** compared to just **1,111 Ops/s** for standard WAL logging (a **73% throughput gain**). Disabling WAL cuts out expensive disk write syncs, making it perfect for volatile cache payloads.

#### 3. Connection Pool Sizing Impact
Restricting the database to a single connection pool bottleneck (`PG Bottleneck Pool=1`) severely hurts concurrent performance. Read-Heavy throughput drops from **1,775 Ops/s** to **1,531 Ops/s** and write latencies increase across the board because concurrent async tasks must block and queue up to acquire the single active database driver handle. A pool size of 10–20 is optimal for high-throughput caching.

#### 4. The Cache Stampede Advisory Lock Game-Changer
This is where **`cache-postgres` achieves a major architectural victory**:
* Under standard Redis/Valkey cache patterns, 20 concurrent requests hitting a missing key with an expensive 200ms calculation all trigger the factory concurrently. Valkey logs **20 redundant factory calls**, hammering the backend API or heavy database 20 times.
* `cache-postgres`, thanks to exclusive **Postgres Advisory Locks**, serializes the cache miss. The first worker acquires the advisory lock, computes the slow factory, writes it to the table, and releases the lock. The remaining 19 workers wait for the lock, detect the value is now populated, and return it instantly. **Postgres logs exactly 1 factory call!**
* Despite Valkey's speed, `cache-postgres` completely prevents cache stampedes, saving critical backend databases from high-load traffic collapse.

---

### Caching Profile Recommendation Matrix

| Scenario / Workload Type | Recommended Cache Approach | Rationale |
| :--- | :--- | :--- |
| **Volatile Caching** <br> (Fast web sessions, page caches, query caches) | **`cache-postgres (use_wal=False, Pool=10)`** | Offers the absolute highest write throughput and lowest latencies. Since cache data is ephemeral, skipping WAL transaction logging maximizes performance with zero durability drawbacks. |
| **High Durability Caching** <br> (Shopping carts, financial tokens, api quotas) | **`cache-postgres (use_wal=True, Pool=10)`** | Regular table with full WAL database logging guarantees full ACID persistence. If the Postgres database crashes, all cached records survive the recovery cycle. |
| **High Concurrency Hot Keys** <br> (Configuration tables, hot products, static data) | **`cache-postgres`** (with advisory lock `get_or_create`) | Database-level advisory locks protect the system against database lockups under dog-piling, guaranteeing $O(1)$ calculations where memory caching falls prey to cache stampedes. |
| **Ultra-Low Latency / Massive Load** <br> (Social media feeds, game servers) | **`Valkey / Redis`** | When sub-millisecond in-memory speeds and 10,000+ operations/second are required, a dedicated Redis or Valkey instance remains the optimal infrastructure choice. |

---

## Database Schema (DDL)

When `create_if_not_exists` is `True` (default), the following schema is created in your database automatically. 

> [!TIP]
> The database schema is fully aligned with standard Microsoft SQL caching, permitting cross-language integrations:

```sql
CREATE SCHEMA IF NOT EXISTS public;

CREATE UNLOGGED TABLE IF NOT EXISTS public.cache (
    -- Cache key using binary collation for byte-by-byte consistency
    id                          VARCHAR(449) COLLATE "C"  NOT NULL,
    -- Serialized value
    value                       BYTEA                     NOT NULL,
    -- Absolute expiration time (UTC)
    expiresattime               TIMESTAMPTZ               NOT NULL,
    -- Sliding expiration interval in seconds
    slidingexpirationinseconds  BIGINT                    NULL,
    -- Absolute ceiling of sliding expiration (UTC)
    absoluteexpiration          TIMESTAMPTZ               NULL,
    
    CONSTRAINT pk_cache PRIMARY KEY (id)
);

-- Index for cleanups by the background scanner
CREATE INDEX IF NOT EXISTS ix_expiresattime
    ON public.cache (expiresattime)
    WITH (deduplicate_items = True);
```

---

## Interactive Demo (Docker Compose)

We provide a complete, interactive FastAPI application running alongside a PostgreSQL database via Docker Compose. This allows you to explore all features (cache stampede protection, tag-based invalidations, failover resiliency, and atomic counters) in a live environment in just one command!

### Running the Demo

1. Navigate to the web demo directory:
   ```bash
   cd examples/web_demo
   ```

2. Start the services using Docker Compose:
   ```bash
   docker compose up --build
   ```

3. Open your browser and navigate to:
   ```
   http://localhost:8000
   ```

You will be greeted by a beautiful, interactive web dashboard where you can:
* **Trigger concurrent request blasts** to visually see cache stampede protection in action.
* **Test tag-based invalidations** by setting tagged keys and invalidating them.
* **Simulate downstream service outages** and watch the `@failover` decorator instantly serve stale cache instead of failing.
* **Increment page counters** atomically in PostgreSQL.

---

## Development & Testing

### Running Tests

If you are developing this package, you can run tests locally.

1. **Unit tests** (does not require a database):
   ```bash
   python -m pytest tests/unit/
   ```

2. **Integration tests** (requires a running PostgreSQL instance):
   You can easily run integration tests against the PostgreSQL instance started via Docker Compose (`examples/web_demo`).
   
   **On Windows (PowerShell):**
   ```powershell
   $env:PGCACHE_TEST_DSN="postgresql://postgres:secretpassword@localhost:5432/cache_demo"
   python -m pytest tests/integration/ -m integration
   ```
   
   **On Linux / macOS (Bash):**
   ```bash
   export PGCACHE_TEST_DSN="postgresql://postgres:secretpassword@localhost:5432/cache_demo"
   python -m pytest tests/integration/ -m integration
   ```

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
