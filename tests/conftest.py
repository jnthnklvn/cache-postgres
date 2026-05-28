import sys
import asyncio

if sys.platform == "win32":
    # Required by psycopg3 in Windows to avoid ProactorEventLoop incompatibility
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
