FROM python:3.9

WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY etl/ /app/etl/

# Set environment variables
ENV PYTHONPATH=/app
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=stream-data

# Run producer
CMD ["python", "etl/stream_producer.py"]
