FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory
RUN mkdir -p data

# Set environment variables
ENV PYTHONPATH=/app
ENV ENABLE_SPARK=true
ENV ENABLE_KAFKA=true
ENV ENABLE_SCHEDULER=true

# Command to run the application with all features
CMD ["python", "etl/main_spark.py"]