@echo off
echo Starting Kafka, Spark, and Airflow services...

REM Check if Docker is running
docker info > nul 2>&1
if %errorlevel% neq 0 (
    echo Docker is not running. Please start Docker Desktop first.
    exit /b 1
)

REM Set environment variables
set USE_MONGODB=true
set MONGODB_HOST=mongodb
set MONGODB_PORT=27017
set MONGODB_DATABASE=market_data

REM Start the services
docker-compose up -d zookeeper kafka kafka-ui mongodb mongo-express spark-master spark-worker postgres airflow-webserver airflow-scheduler airflow-init kafka-producer kafka-consumer dashboard

echo.
echo Services are starting up. Please wait...
timeout /t 10 /nobreak > nul

echo.
echo Access the services at:
echo - Kafka UI: http://localhost:8080
echo - Spark Master UI: http://localhost:8090
echo - Airflow UI: http://localhost:8081 (username: airflow, password: airflow)
echo - MongoDB UI: http://localhost:8082
echo - Dashboard UI: http://localhost:8501
echo.
echo To stop the services, run: docker-compose down
echo.
