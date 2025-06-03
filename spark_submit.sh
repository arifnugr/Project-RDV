#!/bin/bash

# Submit Spark job to the cluster
docker exec -it project-rdv_spark-master_1 spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    /opt/spark-apps/spark_processor.py