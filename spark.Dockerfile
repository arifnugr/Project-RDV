FROM bitnami/spark:3.3.2

USER root

# Install Python and pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic links for python
RUN ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# Install necessary tools
RUN apt-get update && apt-get install -y \
    procps \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download SQLite JDBC driver directly
RUN mkdir -p /opt/bitnami/spark/jars/ && \
    wget -q -O /opt/bitnami/spark/jars/sqlite-jdbc-3.34.0.jar \
    https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.34.0/sqlite-jdbc-3.34.0.jar

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set environment variables
ENV PYTHONPATH=$PYTHONPATH:/app

# Create app directory
RUN mkdir -p /app
WORKDIR /app

# Set permissions
RUN chmod -R 777 /app