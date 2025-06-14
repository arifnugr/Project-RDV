# Docker Setup for Kafka, Spark, and Airflow

This document explains how to use the Docker setup for running Apache Kafka, Apache Spark, and Apache Airflow on Windows.

## Prerequisites

- Docker Desktop for Windows installed and running
- WSL2 backend recommended for better performance

## Services Included

### Apache Kafka
- Kafka Broker: localhost:29092 (from host), kafka:9092 (from containers)
- Zookeeper: localhost:2181
- Kafka UI: http://localhost:8080

### Apache Spark
- Spark Master: spark://localhost:7077
- Spark Master UI: http://localhost:8090
- Spark Worker with 2G memory and 2 cores

### Apache Airflow
- Airflow Webserver: http://localhost:8081
- Default login: username: airflow, password: airflow
- PostgreSQL database for Airflow

## Getting Started

1. Open Command Prompt and navigate to the project directory:
   ```
   cd "c:\Perkuliahan\SEM 6\Rekayasa Data dan Visualisasi\Project-RDV"
   ```

2. Start the services using the provided batch file:
   ```
   start.bat
   ```

3. Access the services through your browser:
   - Kafka UI: http://localhost:8080
   - Spark Master UI: http://localhost:8090
   - Airflow UI: http://localhost:8081 (username: airflow, password: airflow)

4. To stop the services:
   ```
   stop.bat
   ```

## File Structure

- `docker-compose.yml`: Main configuration file for all services
- `airflow.Dockerfile`: Custom Dockerfile for Airflow with Spark and Kafka integration
- `spark.Dockerfile`: Custom Dockerfile for Spark with Python dependencies
- `requirements.txt`: Python dependencies for data processing
- `requirements-airflow.txt`: Airflow-specific dependencies
- `.env`: Environment variables for all services
- `start.bat`: Windows batch file to start services
- `stop.bat`: Windows batch file to stop services

## Troubleshooting

- If containers fail to start, check Docker logs:
  ```
  docker-compose logs [service_name]
  ```

- To restart a specific service:
  ```
  docker-compose restart [service_name]
  ```

- To rebuild a service:
  ```
  docker-compose build --no-cache [service_name]
  docker-compose up -d [service_name]
  ```

- If you encounter permission issues with volumes, try:
  ```
  docker-compose down -v
  docker-compose up -d
  ```