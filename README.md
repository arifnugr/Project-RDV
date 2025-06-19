# Data Engineering Project with Kafka, Spark, and Airflow

This project sets up a complete data engineering pipeline using Apache Kafka for streaming, Apache Spark for data processing, and Apache Airflow for workflow orchestration.

## Prerequisites

- Docker and Docker Compose installed on Windows
- Git (optional)

## Project Structure

```
Project-RDV/
├── dags/                  # Airflow DAG files
├── data/                  # Data files
├── etl/                   # ETL scripts
├── docker-compose.yml     # Docker Compose configuration
├── airflow.Dockerfile     # Airflow custom Dockerfile
├── spark.Dockerfile       # Spark custom Dockerfile
├── requirements.txt       # Python dependencies
├── requirements-airflow.txt # Airflow-specific dependencies
└── .env                   # Environment variables
```

## Getting Started

1. Clone or download this repository

2. Start the Docker containers:
   ```
   docker-compose up -d
   ```

3. Access the services:
   - Kafka UI: http://localhost:8080
   - Spark Master UI: http://localhost:8090
   - Airflow UI: http://localhost:8081 (username: airflow, password: airflow)
   - MongoDB UI: http://localhost:8082
   - Dashboard UI: http://localhost:8501

4. To stop the containers:
   ```
   docker-compose down
   ```

## Services

### Apache Kafka

- Kafka Broker: localhost:29092 (from host), kafka:9092 (from containers)
- Zookeeper: localhost:2181
- Kafka UI: http://localhost:8080

### Apache Spark

- Spark Master: spark://localhost:7077
- Spark Master UI: http://localhost:8090
- Spark Worker

### Apache Airflow

- Airflow Webserver: http://localhost:8081
- Default login: username: airflow, password: airflow

## Running the Pipeline

1. Start the services using Docker Compose
2. Access the Airflow UI and enable the DAGs
3. Monitor the execution through the Airflow UI

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

## Notes for Windows Users

- Docker Desktop for Windows must be installed and running
- WSL2 backend is recommended for better performance
- File paths in Docker volumes use forward slashes even on Windows
