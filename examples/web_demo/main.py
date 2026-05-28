import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from postgres_cache import AsyncPostgresCache, PostgresCacheOptions, EntryOptions

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cache_demo")

# Get database URL from environment or default to local docker setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:secretpassword@localhost:5432/cache_demo")

# Configure postgres-cache options
options = PostgresCacheOptions(
    dsn=DATABASE_URL,
    schema="public",
    table="cache",
    create_if_not_exists=True,
    use_wal=False,  # UNLOGGED table for high performance
    enable_expiration_scan=True,
    expiration_scan_interval=timedelta(minutes=5),  # 5 minutes minimum scan interval
)

# Initialize global cache object
cache = AsyncPostgresCache(options)

# Setup FastAPI lifespan manager
async def lifespan(app: FastAPI):
    # Startup: Initialize the cache connection pool and background scanner
    logger.info("Starting up database cache scanner and connection pool...")
    async with cache:
        yield
    # Shutdown: Close the scanner and connection pool
    logger.info("Shutting down database cache scanner and connection pool...")

app = FastAPI(
    title="Postgres Cache Demo",
    description="Interactive dashboard to showcase stampede protection, failover, tag invalidation, and atomic counters.",
    version="1.0.0",
    lifespan=lifespan
)

# Request schemas
class SetTagRequest(BaseModel):
    key: str
    value: str
    tags: List[str]

class InvalidateTagRequest(BaseModel):
    tag: str

# 1. HTML Dashboard Endpoint
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Postgres Cache - Interactive Demo</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
        <style>
            body {
                font-family: 'Plus Jakarta Sans', sans-serif;
                background-color: #0f172a;
            }
            .glass {
                background: rgba(30, 41, 59, 0.7);
                backdrop-filter: blur(12px);
                border: 1px solid rgba(255, 255, 255, 0.05);
            }
        </style>
    </head>
    <body class="text-slate-100 min-h-screen pb-12">
        <div class="max-w-6xl mx-auto px-4 py-8">
            <!-- Header -->
            <div class="flex flex-col md:flex-row md:items-center md:justify-between border-b border-slate-800 pb-6 mb-8">
                <div>
                    <h1 class="text-3xl font-bold bg-gradient-to-r from-blue-400 via-indigo-400 to-purple-500 bg-clip-text text-transparent">
                        Postgres Cache Demo
                    </h1>
                    <p class="text-slate-400 mt-1 text-sm md:text-base">
                        Real-time interactive dashboard demonstrating distributed caching capabilities over PostgreSQL.
                    </p>
                </div>
                <div class="mt-4 md:mt-0 flex items-center gap-3">
                    <span class="px-3 py-1 bg-green-500/10 border border-green-500/20 text-green-400 rounded-full text-xs font-semibold flex items-center gap-2">
                        <span class="w-2 h-2 rounded-full bg-green-500 animate-ping"></span>
                        Active Connection Pool
                    </span>
                    <span class="px-3 py-1 bg-blue-500/10 border border-blue-500/20 text-blue-400 rounded-full text-xs font-semibold">
                        UNLOGGED Table enabled
                    </span>
                </div>
            </div>

            <!-- Dashboard Grid -->
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
                
                <!-- Left Panel: Stats & Primitives -->
                <div class="lg:col-span-1 space-y-6">
                    <!-- Stats Card -->
                    <div class="glass p-6 rounded-2xl">
                        <h2 class="text-lg font-semibold text-slate-200 mb-4 flex items-center gap-2">
                            📈 Live Statistics
                        </h2>
                        <div class="space-y-4">
                            <div class="flex justify-between items-center bg-slate-900/50 p-3 rounded-lg border border-slate-800">
                                <span class="text-sm text-slate-400">Total Page Hits (Atomic)</span>
                                <span id="stat-hits" class="text-xl font-bold text-blue-400">Loading...</span>
                            </div>
                            <button onclick="incrementHits()" class="w-full py-2 bg-blue-600 hover:bg-blue-500 text-white rounded-lg transition font-medium text-sm flex items-center justify-center gap-2 shadow-lg shadow-blue-500/20">
                                ➕ Increment Counter (Atomic)
                            </button>
                        </div>
                    </div>

                    <!-- Connection Pool details -->
                    <div class="glass p-6 rounded-2xl">
                        <h2 class="text-lg font-semibold text-slate-200 mb-4 flex items-center gap-2">
                            ⚙️ Configuration
                        </h2>
                        <div class="space-y-2 text-sm text-slate-400">
                            <div class="flex justify-between border-b border-slate-800/50 py-2">
                                <span>Schema / Table</span>
                                <span class="text-slate-200 font-mono">public.cache</span>
                            </div>
                            <div class="flex justify-between border-b border-slate-800/50 py-2">
                                <span>WAL Mode</span>
                                <span class="text-yellow-500 font-semibold">Disabled (UNLOGGED)</span>
                            </div>
                            <div class="flex justify-between border-b border-slate-800/50 py-2">
                                <span>Auto-scan Expired</span>
                                <span class="text-green-500 font-semibold">Active (Every 10s)</span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Right/Middle: Interactive Feature Demos -->
                <div class="lg:col-span-2 space-y-6">
                    
                    <!-- 1. Cache Stampede Protection Demo -->
                    <div class="glass p-6 rounded-2xl">
                        <div class="flex justify-between items-start mb-2">
                            <h2 class="text-lg font-semibold text-slate-200 flex items-center gap-2">
                                🛡️ Stampede Protection (Advisory Locks)
                            </h2>
                            <span class="text-xs bg-indigo-500/10 border border-indigo-500/20 text-indigo-400 px-2 py-0.5 rounded font-mono">get_or_create</span>
                        </div>
                        <p class="text-xs text-slate-400 mb-4 leading-relaxed">
                            Simulates an expensive database query (taking <strong>2.0 seconds</strong>). When clicking "Concurrent Request Blast", multiple HTTP requests are sent simultaneously. Thanks to advisory locks, only one request will execute the calculation; others wait and serve the computed cache key instantly.
                        </p>
                        
                        <div class="flex flex-wrap gap-3 mb-4">
                            <button onclick="testStampede(1)" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-sm font-medium rounded-lg border border-slate-700 transition">
                                Single Slow Request
                            </button>
                            <button onclick="testStampede(5)" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-medium rounded-lg transition shadow-lg shadow-indigo-500/20">
                                ⚡ Concurrent Request Blast (5x)
                            </button>
                            <button onclick="clearStampedeKey()" class="px-3 py-2 bg-slate-900/50 border border-red-500/20 text-red-400 hover:bg-red-500/10 text-xs font-semibold rounded-lg transition ml-auto">
                                Clear Cache Key
                            </button>
                        </div>

                        <!-- Results display -->
                        <div id="stampede-log" class="bg-slate-950 p-4 rounded-xl font-mono text-xs border border-slate-800 h-40 overflow-y-auto space-y-1">
                            <div class="text-slate-500">// Results will be output here in real-time...</div>
                        </div>
                    </div>

                    <!-- 2. Tag-Based Invalidation Demo -->
                    <div class="glass p-6 rounded-2xl">
                        <div class="flex justify-between items-start mb-2">
                            <h2 class="text-lg font-semibold text-slate-200 flex items-center gap-2">
                                🏷️ Tag-Based Invalidation
                            </h2>
                            <span class="text-xs bg-purple-500/10 border border-purple-500/20 text-purple-400 px-2 py-0.5 rounded font-mono">delete_tags</span>
                        </div>
                        <p class="text-xs text-slate-400 mb-4 leading-relaxed">
                            Associate entries with tag tags. You can invalidate multiple caches at once by calling delete_tags.
                        </p>
                        
                        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                            <div>
                                <label class="block text-xs font-semibold text-slate-400 mb-1">Cache Key</label>
                                <input id="tag-key" type="text" value="product:101" class="w-full bg-slate-900 border border-slate-800 rounded px-3 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500">
                            </div>
                            <div>
                                <label class="block text-xs font-semibold text-slate-400 mb-1">Value (Content)</label>
                                <input id="tag-value" type="text" value="Smartphone Max" class="w-full bg-slate-900 border border-slate-800 rounded px-3 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500">
                            </div>
                            <div>
                                <label class="block text-xs font-semibold text-slate-400 mb-1">Tags (Comma-separated)</label>
                                <input id="tag-tags" type="text" value="products, electronics" class="w-full bg-slate-900 border border-slate-800 rounded px-3 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500">
                            </div>
                        </div>

                        <div class="flex gap-3 mb-4">
                            <button onclick="setTaggedValue()" class="px-4 py-2 bg-purple-600 hover:bg-purple-500 text-white text-sm font-medium rounded-lg transition">
                                Set Tagged Cache
                            </button>
                            <button onclick="readTaggedValue()" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-sm font-medium rounded-lg border border-slate-700 transition">
                                Read Cache Key
                            </button>
                            <div class="flex gap-2 ml-auto items-center">
                                <input id="invalidate-tag" type="text" value="electronics" placeholder="tag" class="bg-slate-900 border border-red-500/20 focus:border-red-500 rounded px-2.5 py-1 text-xs text-slate-200 focus:outline-none w-24">
                                <button onclick="invalidateTag()" class="px-3 py-1.5 bg-red-600/20 hover:bg-red-600/30 border border-red-500/30 text-red-300 text-xs font-semibold rounded-lg transition">
                                    Invalidate Tag
                                </button>
                            </div>
                        </div>

                        <div id="tag-log" class="bg-slate-950 p-3 rounded-xl font-mono text-xs border border-slate-800 h-24 overflow-y-auto">
                            <div class="text-slate-500">// Tag logs...</div>
                        </div>
                    </div>

                    <!-- 3. Failover Outage Resiliency Demo -->
                    <div class="glass p-6 rounded-2xl">
                        <div class="flex justify-between items-start mb-2">
                            <h2 class="text-lg font-semibold text-slate-200 flex items-center gap-2">
                                🩹 Outage Resiliency (@failover Decorator)
                            </h2>
                            <span class="text-xs bg-yellow-500/10 border border-yellow-500/20 text-yellow-400 px-2 py-0.5 rounded font-mono">@failover</span>
                        </div>
                        <p class="text-xs text-slate-400 mb-4 leading-relaxed">
                            Serves expired cache data when external dependencies fail. Execute a call to successfully cache the data first, then check "Simulate API Outage" and trigger it again. Instead of throwing a 500 error, it returns the stale cache!
                        </p>

                        <div class="flex items-center gap-4 mb-4">
                            <label class="flex items-center gap-2 cursor-pointer bg-slate-900/60 p-3 rounded-lg border border-slate-800">
                                <input id="failover-simulate" type="checkbox" class="w-4 h-4 text-blue-600 bg-gray-700 border-gray-600 rounded focus:ring-blue-500 focus:ring-2">
                                <span class="text-sm font-semibold text-red-400">Simulate API Outage</span>
                            </label>
                            
                            <button onclick="fetchFailover()" class="px-4 py-3 bg-yellow-600 hover:bg-yellow-500 text-white text-sm font-semibold rounded-lg transition shadow-lg shadow-yellow-500/15">
                                Trigger Resilient Call
                            </button>
                            
                            <button onclick="clearFailoverKey()" class="px-3 py-1.5 bg-slate-900 border border-red-500/20 text-red-400 hover:bg-red-500/10 text-xs font-semibold rounded-lg transition ml-auto">
                                Clear Cache
                            </button>
                        </div>

                        <div id="failover-log" class="bg-slate-950 p-3 rounded-xl font-mono text-xs border border-slate-800 h-28 overflow-y-auto">
                            <div class="text-slate-500">// Outage log outputs...</div>
                        </div>
                    </div>

                </div>
            </div>
        </div>

        <script>
            // Log outputs
            function appendLog(elementId, text, type = 'info') {
                const log = document.getElementById(elementId);
                const color = type === 'error' ? 'text-red-400' : type === 'success' ? 'text-green-400' : type === 'warning' ? 'text-yellow-400' : 'text-slate-300';
                const time = new Date().toLocaleTimeString();
                log.innerHTML += `<div class="${color}">[${time}] ${text}</div>`;
                log.scrollTop = log.scrollHeight;
            }

            // Stats
            async function updateStats() {
                try {
                    const res = await fetch('/api/stats');
                    const data = await res.json();
                    document.getElementById('stat-hits').innerText = data.page_hits || 0;
                } catch(e) {
                    console.error("Failed to load statistics", e);
                }
            }

            async function incrementHits() {
                try {
                    const res = await fetch('/api/incr', { method: 'POST' });
                    const data = await res.json();
                    document.getElementById('stat-hits').innerText = data.page_hits;
                } catch(e) {
                    alert("Error incrementing counter");
                }
            }

            // Stampede Test
            async function testStampede(count) {
                const item = Math.floor(Math.random() * 100) + 1;
                appendLog('stampede-log', `Blasting ${count} concurrent requests for slow-data endpoint (item ID: ${item})...`, 'info');
                
                const promises = [];
                for(let i = 0; i < count; i++) {
                    promises.push((async (index) => {
                        const start = performance.now();
                        try {
                            const res = await fetch(`/api/slow-data/${item}`);
                            const data = await res.json();
                            const duration = ((performance.now() - start) / 1000).toFixed(2);
                            appendLog('stampede-log', `Req #${index+1} finished in ${duration}s -> Data: ${data.data} (Cached: ${data.cached})`, data.cached ? 'success' : 'warning');
                        } catch(e) {
                            appendLog('stampede-log', `Req #${index+1} failed!`, 'error');
                        }
                    })(i));
                }
                await Promise.all(promises);
                updateStats();
            }

            async function clearStampedeKey() {
                try {
                    await fetch('/api/clear-key?key=slow-data-demo');
                    appendLog('stampede-log', 'Cache key cleared!', 'success');
                } catch(e) {
                    appendLog('stampede-log', 'Failed to clear key', 'error');
                }
            }

            // Tags Test
            async function setTaggedValue() {
                const key = document.getElementById('tag-key').value;
                const value = document.getElementById('tag-value').value;
                const tagsStr = document.getElementById('tag-tags').value;
                const tags = tagsStr.split(',').map(t => t.trim());

                try {
                    const res = await fetch('/api/tags/set', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ key, value, tags })
                    });
                    const data = await res.json();
                    appendLog('tag-log', `SUCCESS: Key '${key}' set with tags: [${tags.join(', ')}]`, 'success');
                } catch(e) {
                    appendLog('tag-log', 'Error saving tagged cache', 'error');
                }
            }

            async function readTaggedValue() {
                const key = document.getElementById('tag-key').value;
                try {
                    const res = await fetch(`/api/get-key?key=${key}`);
                    if(res.status === 404) {
                        appendLog('tag-log', `MISS: Cache key '${key}' not found (expired or deleted by tag).`, 'warning');
                        return;
                    }
                    const data = await res.json();
                    appendLog('tag-log', `HIT: Value for '${key}' is '${data.value}'`, 'success');
                } catch(e) {
                    appendLog('tag-log', 'Error reading cache', 'error');
                }
            }

            async function invalidateTag() {
                const tag = document.getElementById('invalidate-tag').value;
                try {
                    const res = await fetch('/api/tags/invalidate', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ tag })
                    });
                    appendLog('tag-log', `INVALIDATED tag: '${tag}'. All related cache keys are now gone!`, 'error');
                } catch(e) {
                    appendLog('tag-log', 'Failed to invalidate tag', 'error');
                }
            }

            // Failover Test
            async function fetchFailover() {
                const simulate = document.getElementById('failover-simulate').checked;
                appendLog('failover-log', `Fetching resilient API (outage: ${simulate})...`, 'info');
                
                try {
                    const res = await fetch(`/api/failover-data?simulate_failure=${simulate}`);
                    if (!res.ok) {
                        throw new Error("Outage and no stale cache available");
                    }
                    const data = await res.json();
                    if (data.stale) {
                        appendLog('failover-log', `WARNING: Downstream API failed, served STALE cache -> Temp: ${data.temperature}°C, City: ${data.city}`, 'warning');
                    } else {
                        appendLog('failover-log', `SUCCESS: API succeeded, served fresh data -> Temp: ${data.temperature}°C, City: ${data.city}`, 'success');
                    }
                } catch(e) {
                    appendLog('failover-log', `ERROR: API failed and no cached data was found! Make a successful call first.`, 'error');
                }
            }

            async function clearFailoverKey() {
                try {
                    await fetch('/api/clear-key?key=failover-weather');
                    appendLog('failover-log', 'Failover cache cleared!', 'success');
                } catch(e) {
                    appendLog('failover-log', 'Failed to clear', 'error');
                }
            }

            // Init
            updateStats();
            setInterval(updateStats, 5000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# 2. Page Counter API using Atomic Counter
@app.get("/api/stats")
async def get_stats():
    # Read the counter (if none exists, returns 0)
    counter_bytes = await cache.get("page_hits")
    hits = int(counter_bytes.decode('utf-8')) if counter_bytes else 0
    return {"page_hits": hits}

@app.post("/api/incr")
async def increment_counter():
    # Atomically increment in the database
    new_value = await cache.incr("page_hits")
    return {"page_hits": new_value}

# 3. Cache Stampede Protection (get_or_create with database advisory locks)
@app.get("/api/slow-data/{item_id}")
async def get_slow_data(item_id: int):
    # This track variable allows detecting if the factory actually runs (Cache Miss) or not (Cache Hit)
    factory_executed = False

    async def expensive_computation():
        nonlocal factory_executed
        factory_executed = True
        logger.info(f"Cache miss for slow-data! Computing heavy database values for item {item_id}...")
        # Simulate heavy load
        await asyncio.sleep(2.0)
        now_str = datetime.now(timezone.utc).isoformat()
        return f"Item {item_id} data generated at {now_str}".encode('utf-8')

    # The cache.get_or_create utilizes advisory locks internally so that only ONE concurrent
    # request triggers the expensive_computation while others wait on it.
    cached_bytes = await cache.get_or_create(
        "slow-data-demo",
        expensive_computation,
        EntryOptions(absolute_expiration_relative="15s")  # 15s TTL
    )

    return {
        "data": cached_bytes.decode('utf-8'),
        "cached": not factory_executed
    }

# 4. Tag-based Invalidation
@app.post("/api/tags/set")
async def set_tagged(req: SetTagRequest):
    value_bytes = req.value.encode('utf-8')
    await cache.set(
        req.key, 
        value_bytes, 
        EntryOptions(
            absolute_expiration_relative="5m", 
            tags=req.tags
        )
    )
    return {"status": "success", "key": req.key, "tags": req.tags}

@app.post("/api/tags/invalidate")
async def invalidate_tag(req: InvalidateTagRequest):
    await cache.delete_tags(req.tag)
    return {"status": "success", "invalidated_tag": req.tag}

# Helper to read raw key
@app.get("/api/get-key")
async def get_key(key: str):
    val = await cache.get(key)
    if val is None:
        raise HTTPException(status_code=404, detail="Key not found or expired")
    return {"key": key, "value": val.decode('utf-8')}

# Helper to clear keys
@app.get("/api/clear-key")
async def clear_key(key: str):
    await cache.remove(key)
    return {"status": "success", "cleared": key}

# 5. Outage Resiliency using @failover decorator
# We set a short TTL of 15 seconds. If the API is simulated to fail, it will serve the stale
# data for up to 10 minutes if available.
@cache.failover("failover-weather", ttl="15s", exceptions=(RuntimeError,))
async def get_weather_data(simulate_failure: bool):
    if simulate_failure:
        logger.warning("Outage! Simulating downstream API failure...")
        raise RuntimeError("External Weather API Outage! Service is unavailable.")
        
    logger.info("Fresh call: Downstream API succeeded.")
    # Return serializable dict (failover decorator handles pickling/unpickling automatically!)
    return {
        "city": "São Paulo",
        "temperature": 22.5,
        "humidity": "65%",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stale": False
    }

@app.get("/api/failover-data")
async def api_failover(simulate_failure: bool = Query(False)):
    try:
        data = await get_weather_data(simulate_failure)
        # If the returned data contains a timestamp that is not current but the API was simulated
        # to fail, then we served stale data.
        if simulate_failure:
            data = data.copy()
            data["stale"] = True
        return data
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
