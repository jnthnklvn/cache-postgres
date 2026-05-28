import os
import time
import random
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Dict, Any
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
import redis.asyncio as redis

from postgres_cache import AsyncPostgresCache, PostgresCacheOptions, EntryOptions

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cache_benchmark")

# Read database URL and Valkey URL from environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:secretpassword@localhost:5432/benchmark_db")
VALKEY_URL = os.getenv("VALKEY_URL", "redis://localhost:6379/0")

# Cache 1: Unlogged, pool max size 10 (High performance optimized)
opt_unlogged_10 = PostgresCacheOptions(
    dsn=DATABASE_URL,
    schema="public",
    table="cache_unlogged_10",
    create_if_not_exists=True,
    use_wal=False,
    enable_expiration_scan=False,
    pool_min_size=1,
    pool_max_size=10
)
cache_unlogged_10 = AsyncPostgresCache(opt_unlogged_10)

# Cache 2: Logged, pool max size 10 (Durability focused)
opt_logged_10 = PostgresCacheOptions(
    dsn=DATABASE_URL,
    schema="public",
    table="cache_logged_10",
    create_if_not_exists=True,
    use_wal=True,
    enable_expiration_scan=False,
    pool_min_size=1,
    pool_max_size=10
)
cache_logged_10 = AsyncPostgresCache(opt_logged_10)

# Cache 3: Unlogged, pool max size 1 (Bottleneck / high contention)
opt_pool_1 = PostgresCacheOptions(
    dsn=DATABASE_URL,
    schema="public",
    table="cache_unlogged_1",
    create_if_not_exists=True,
    use_wal=False,
    enable_expiration_scan=False,
    pool_min_size=1,
    pool_max_size=1
)
cache_pool1 = AsyncPostgresCache(opt_pool_1)

# Initialize Valkey Client
valkey_client = redis.from_url(VALKEY_URL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start background services and connection pools
    logger.info("Initializing connection pools...")
    await cache_unlogged_10.__aenter__()
    await cache_logged_10.__aenter__()
    await cache_pool1.__aenter__()
    
    # Simple check to see if database connection is alive
    try:
        await cache_unlogged_10.get("connection_test")
        logger.info("Postgres connections successfully established.")
    except Exception as e:
        logger.error(f"Postgres connection check failed: {e}")
        
    try:
        await valkey_client.ping()
        logger.info("Valkey connection successfully established.")
    except Exception as e:
        logger.error(f"Valkey connection check failed: {e}")
        
    yield
    
    # Shutdown: Close database pools and redis client
    logger.info("Closing connection pools...")
    await cache_unlogged_10.__aexit__(None, None, None)
    await cache_logged_10.__aexit__(None, None, None)
    await cache_pool1.__aexit__(None, None, None)
    await valkey_client.aclose()
    logger.info("All connection pools closed.")

app = FastAPI(
    title="Postgres Cache vs Valkey Benchmark",
    description="Interactive performance comparison suite",
    version="1.0.0",
    lifespan=lifespan
)

class BenchmarkRequest(BaseModel):
    scenario: str
    concurrency: int
    operations: int
    factory_delay: float = 0.1

def get_percentile(sorted_list: List[float], pct: float) -> float:
    """Helper to calculate latencies percentile without external numpy package."""
    if not sorted_list:
        return 0.0
    idx = int(len(sorted_list) * pct)
    idx = min(idx, len(sorted_list) - 1)
    return sorted_list[idx]

@app.get("/", response_class=HTMLResponse)
async def index():
    """Serves the dashboard index.html."""
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.post("/api/benchmark/{config_name}")
async def run_benchmark(config_name: str, req: BenchmarkRequest):
    """Executes a benchmark for a specific caching profile and workload scenario."""
    # Resolve the Cache Client
    client: Any = None
    if config_name == "postgres_unlogged_10":
        client = cache_unlogged_10
    elif config_name == "postgres_logged_10":
        client = cache_logged_10
    elif config_name == "postgres_pool1":
        client = cache_pool1
    elif config_name == "valkey":
        client = valkey_client
    else:
        raise HTTPException(status_code=400, detail=f"Invalid configuration name: {config_name}")

    scenario = req.scenario
    concurrency = req.concurrency
    operations = req.operations
    factory_delay = req.factory_delay

    logger.info(f"Running benchmark: Config={config_name}, Workload={scenario}, Concurrency={concurrency}, Ops={operations}")

    if scenario == "stampede":
        # Cache stampede benchmark is measured differently
        return await handle_stampede_benchmark(config_name, client, concurrency, factory_delay)

    # 1. Pre-population: Populate test keys to measure realistic GET operations (hits)
    keys = [f"bench_key_{i}" for i in range(100)]
    val_payload = b"x" * 1024 # 1 KB payload
    
    try:
        if config_name == "valkey":
            for key in keys:
                await valkey_client.set(key, val_payload)
        else:
            for key in keys:
                await client.set(key, val_payload, EntryOptions(absolute_expiration_relative="1h"))
    except Exception as e:
        logger.error(f"Pre-population failed for {config_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to pre-populate keys: {str(e)}")

    # 2. Benchmark Queue Setup
    queue = asyncio.Queue()
    for _ in range(operations):
        await queue.put(None)

    latencies: List[float] = []
    successes = 0
    failures = 0

    # 3. Async Workers
    async def worker():
        nonlocal successes, failures
        while not queue.empty():
            try:
                await queue.get()
            except asyncio.QueueEmpty:
                break

            op_type = "get"
            if scenario == "read_heavy":
                op_type = "get" if random.random() < 0.9 else "set"
            elif scenario == "write_heavy":
                op_type = "set" if random.random() < 0.9 else "get"
            elif scenario == "atomic_incr":
                op_type = "incr"

            key = random.choice(keys)
            start_time = time.perf_counter()

            try:
                if config_name == "valkey":
                    if op_type == "get":
                        await valkey_client.get(key)
                    elif op_type == "set":
                        await valkey_client.set(key, val_payload)
                    elif op_type == "incr":
                        await valkey_client.incr("bench_counter")
                else: # Postgres Cache Clients
                    if op_type == "get":
                        await client.get(key)
                    elif op_type == "set":
                        await client.set(key, val_payload, EntryOptions(absolute_expiration_relative="5m"))
                    elif op_type == "incr":
                        await client.incr("bench_counter")
                
                successes += 1
            except Exception as e:
                failures += 1
                logger.error(f"Error during benchmark operation for {config_name}: {e}")
            finally:
                latency = (time.perf_counter() - start_time) * 1000.0 # convert to ms
                latencies.append(latency)
                queue.task_done()

    # Run execution timer
    bench_start = time.perf_counter()
    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    await asyncio.gather(*workers)
    total_time_s = time.perf_counter() - bench_start

    # Sort latencies to compute percentiles
    sorted_latencies = sorted(latencies)
    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    p95 = get_percentile(sorted_latencies, 0.95)
    p99 = get_percentile(sorted_latencies, 0.99)
    ops_per_sec = successes / total_time_s if total_time_s > 0 else 0.0

    return {
        "config_name": config_name,
        "scenario": scenario,
        "total_ops": operations,
        "successes": successes,
        "failures": failures,
        "total_time_s": total_time_s,
        "ops_per_sec": ops_per_sec,
        "avg_latency_ms": avg_latency,
        "p95_latency_ms": p95,
        "p99_latency_ms": p99
    }

async def handle_stampede_benchmark(config_name: str, client: Any, concurrency: int, factory_delay: float) -> Dict[str, Any]:
    """Simulates cache stampede and measures factory execution count and latencies."""
    stampede_key = "benchmark_stampede_key"

    # Reset cache state by deleting key first
    try:
        if config_name == "valkey":
            await valkey_client.delete(stampede_key)
        else:
            await client.remove(stampede_key)
    except Exception:
        pass

    # Counter to track how many times the slow calculation was executed
    factory_calls = 0
    
    async def slow_factory() -> bytes:
        nonlocal factory_calls
        factory_calls += 1
        await asyncio.sleep(factory_delay)
        return b"slow_factory_payload_result"

    latencies: List[float] = []
    successes = 0
    failures = 0

    # Define concurrent work logic depending on client type
    async def stampede_worker():
        nonlocal successes, failures
        start_worker = time.perf_counter()
        try:
            if config_name == "valkey":
                # Standard Cache Stampede Scenario: Check cache -> Miss -> Slow Query -> Write
                val = await valkey_client.get(stampede_key)
                if val is None:
                    val = await slow_factory()
                    await valkey_client.set(stampede_key, val)
            else:
                # PG Cache: Protected by exclusive PG advisory locking inside get_or_create!
                await client.get_or_create(stampede_key, slow_factory, EntryOptions(absolute_expiration_relative="1m"))
            
            successes += 1
        except Exception as e:
            failures += 1
            logger.error(f"Stampede worker error: {e}")
        finally:
            latency = (time.perf_counter() - start_worker) * 1000.0 # ms
            latencies.append(latency)

    # Launch all workers concurrently to trigger the cache miss simultaneously
    bench_start = time.perf_counter()
    tasks = [asyncio.create_task(stampede_worker()) for _ in range(concurrency)]
    await asyncio.gather(*tasks)
    total_time_s = time.perf_counter() - bench_start

    sorted_latencies = sorted(latencies)
    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    p95 = get_percentile(sorted_latencies, 0.95)
    p99 = get_percentile(sorted_latencies, 0.99)
    ops_per_sec = successes / total_time_s if total_time_s > 0 else 0.0

    return {
        "config_name": config_name,
        "scenario": "stampede",
        "total_ops": concurrency,
        "successes": successes,
        "failures": failures,
        "total_time_s": total_time_s,
        "ops_per_sec": ops_per_sec,
        "avg_latency_ms": avg_latency,
        "p95_latency_ms": p95,
        "p99_latency_ms": p99,
        "factory_calls": factory_calls # Critical metric showing stampede avoidance!
    }
