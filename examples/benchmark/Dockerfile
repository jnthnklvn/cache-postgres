FROM python:3.11-slim

WORKDIR /app

# Install compilation tools and postgres client libraries for psycopg
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the core library code to install it
COPY . /postgres_cache_src

# Install the postgres-cache library from source in editable mode
RUN pip install --no-cache-dir -e /postgres_cache_src

# Copy and install benchmark-specific requirements
COPY examples/benchmark/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the benchmark application
COPY examples/benchmark/main.py /app/main.py
COPY examples/benchmark/index.html /app/index.html

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
