FROM apache/airflow:2.6.3-python3.9

USER root
# Install Java & Spark client
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless wget && \
    wget -qO- https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
      | tar xz -C /opt && \
    ln -s /opt/spark-3.3.2-bin-hadoop3 /opt/spark

# Set SPARK_HOME & PATH
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY requirements-airflow.txt /tmp/
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt