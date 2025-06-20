#!/bin/bash

echo "Starting Kafka, Spark, and Airflow services..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first."
    exit 1
fi

# Set environment variables
export USE_MONGODB=true
export MONGODB_HOST=mongodb
export MONGODB_PORT=27017
export MONGODB_DATABASE=market_data

# Start the services
docker-compose up -d

echo
echo "Services are starting up. Please wait..."
sleep 10

echo
echo "Access the services at:"
echo "- Kafka UI: http://localhost:8080"
echo "- Spark Master UI: http://localhost:8090"
echo "- Airflow UI: http://localhost:8081 (username: airflow, password: airflow)"
echo "- MongoDB UI: http://localhost:8082"
echo "- Dashboard UI: http://localhost:8501"
echo
echo "To stop the services, run: docker-compose down"
echo