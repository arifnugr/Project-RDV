@echo off
echo Starting Project-RDV with all components (Kafka, Spark, Scheduler)...

REM Check if Docker is running
docker info > nul 2>&1
if %errorlevel% neq 0 (
    echo Docker is not running. Please start Docker and try again.
    exit /b 1
)

REM Start the containers
docker-compose up -d

echo.
echo All services are starting up...
echo.
echo Access Spark UI at: http://localhost:8080
echo.
echo To view logs:
echo docker-compose logs -f app
echo.
echo To view Kafka producer logs:
echo docker-compose logs -f kafka-producer
echo.
echo To view Kafka consumer logs:
echo docker-compose logs -f kafka-consumer
echo.
echo To view scheduler logs:
echo docker-compose logs -f scheduler
echo.
echo To stop all services:
echo docker-compose down
echo.

pause