FROM python:3.11-slim

WORKDIR /app

# Install git or compile tools if needed (psycopg binary might be used, but since we use standard psycopg we're good)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the core library code to install it
COPY . /cache_postgres_src

# Install the cache-postgres library from source
RUN pip install --no-cache-dir -e /cache_postgres_src

# Copy and install demo-specific requirements
COPY examples/web_demo/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the application
COPY examples/web_demo/main.py /app/main.py

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
