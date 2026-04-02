FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY evogateway.py .
COPY evogateway/ evogateway/

# Config and logs are mounted at runtime — create mount points
RUN mkdir -p config logs

CMD ["python", "evogateway.py", "--config", "config/evogateway.cfg"]
