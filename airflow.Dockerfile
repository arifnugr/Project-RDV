FROM apache/airflow:2.6.3-python3.9

USER airflow

# Copy requirements files
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install additional packages for Kafka integration
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-kafka==1.0.0 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.6
