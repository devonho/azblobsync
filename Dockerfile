# Dockerfile for azblobsync
# Builds a minimal image that installs dependencies from requirements.txt
# and runs the sync script at container start.

FROM python:3.12-slim

LABEL maintainer="devonho"

# Ensure Python output is unbuffered (helpful for logs)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install CA certificates and any minimal system deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Default command: run the main script
ENTRYPOINT ["python", "-u", "src/main.py"]
