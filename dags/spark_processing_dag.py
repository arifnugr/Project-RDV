"""
Spark Processing DAG for Apache Airflow
This DAG orchestrates the Spark streaming processing of cryptocurrency market data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_stream_processing',
    default_args=default_args,
    description='Spark streaming processing for cryptocurrency market data',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent multiple concurrent runs
    tags=['crypto', 'spark', 'processing'],
)

# Define the path to the project directory
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def run_spark_stream(**kwargs):
    """Run the Spark streaming job reading directly from Kafka"""
    import os
    import sys
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, window, avg, max, min, from_json, to_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from pymongo import MongoClient
    from datetime import datetime
    
    try:
        # Create a SparkSession with Kafka packages
        spark = SparkSession.builder \
            .appName("KafkaStreamProcessing") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .master("local[*]") \
            .getOrCreate()
        
        print(f"Apache Spark Version: {spark.version}")
        print(f"Spark Application ID: {spark.sparkContext.applicationId}")
        print(f"Spark Master: {spark.sparkContext.master}")
        
        # Define schema for the market data
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("last_price", DoubleType(), True),
            StructField("bid_price", DoubleType(), True),
            StructField("ask_price", DoubleType(), True),
            StructField("bid_volume", DoubleType(), True),
            StructField("ask_volume", DoubleType(), True),
            StructField("volume_24h", DoubleType(), True),
            StructField("high_24h", DoubleType(), True),
            StructField("low_24h", DoubleType(), True),
            StructField("change_24h", DoubleType(), True),
            StructField("price_change_24h", DoubleType(), True),
            StructField("bid_ask_spread", DoubleType(), True),
            StructField("change_24h_normalized", DoubleType(), True),
            StructField("spread_percentage", DoubleType(), True),
            StructField("trend", StringType(), True)
        ])
        
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "stream-data") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data from Kafka
        parsed_df = kafka_df \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        
        # Convert timestamp and calculate metrics
        processed_df = parsed_df \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("price_range", col("high_24h") - col("low_24h")) \
            .withColumn("volume_price_ratio", col("volume_24h") / col("last_price")) \
            .withColumn("market_impact", col("bid_volume") * col("ask_volume") / col("volume_24h"))
        
        # Window aggregation
        windowed_df = processed_df \
            .withWatermark("timestamp", "1 minute") \
            .groupBy(window(col("timestamp"), "1 minute")) \
            .agg(
                avg("last_price").alias("avg_price"),
                max("last_price").alias("max_price"),
                min("last_price").alias("min_price"),
                avg("volume_24h").alias("avg_volume"),
                avg("price_range").alias("avg_price_range"),
                avg("volume_price_ratio").alias("avg_volume_price_ratio"),
                avg("market_impact").alias("avg_market_impact")
            )
        
        # Write to MongoDB
        def write_data(batch_df, batch_id):
            # Convert to pandas for MongoDB
            if not batch_df.isEmpty():
                result_pd = batch_df.toPandas()
                
                # Connect to MongoDB
                client = MongoClient("mongodb://mongodb:27017/")
                db = client["market_data"]
                stream_collection = db["market_analysis_stream"]
                
                # Convert window to string for MongoDB storage
                records = []
                for index, row in result_pd.iterrows():
                    record = {
                        "window_start": str(row["window"].start),
                        "window_end": str(row["window"].end),
                        "avg_price": float(row["avg_price"]),
                        "max_price": float(row["max_price"]),
                        "min_price": float(row["min_price"]),
                        "avg_volume": float(row["avg_volume"]),
                        "avg_price_range": float(row["avg_price_range"]),
                        "avg_volume_price_ratio": float(row["avg_volume_price_ratio"]),
                        "avg_market_impact": float(row["avg_market_impact"]),
                        "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    records.append(record)
                
                if records:
                    stream_collection.insert_many(records)
                    print(f"Inserted {len(records)} records into market_analysis_stream")
        
        # Start the streaming query
        query = windowed_df.writeStream \
            .foreachBatch(write_data) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .trigger(processingTime="1 minute") \
            .start()
        
        # Wait for the query to terminate
        query.awaitTermination(timeout=270)  # Run for 4.5 minutes
        
        # Stop the SparkSession
        spark.stop()
        
        return "Kafka stream processing completed successfully"
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        print(f"Error in Kafka stream processing: {e}\n{error_msg}")
        return f"Kafka stream processing failed: {str(e)}"

# Define task
spark_stream_task = PythonOperator(
    task_id='run_spark_stream',
    python_callable=run_spark_stream,
    dag=dag,
    execution_timeout=timedelta(minutes=10)  # Give more time for stream processing
)