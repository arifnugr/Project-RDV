import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class SparkProcessor:
    def __init__(self, app_name="Market Data Processor"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Define schema for market data
        self.schema = StructType([
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

    def process_batch(self, csv_path="data/market_data.csv", output_path="data/spark_output"):
        """Process data in batch mode from CSV file"""
        try:
            # Read CSV
            df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
            
            # Convert timestamp
            df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
            
            # Perform analysis
            result = df.groupBy("trend").count()
            
            # Save results
            result.write.mode("overwrite").csv(output_path)
            
            print(f"Batch processing complete. Results saved to {output_path}")
            return True
        except Exception as e:
            print(f"Error in batch processing: {e}")
            return False

    def process_stream(self, kafka_servers="kafka:9092", topic="stream-data", output_path="data/spark_streaming_output"):
        """Process data in streaming mode from Kafka"""
        try:
            # Read from Kafka
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .load()
            
            # Parse JSON
            parsed_df = df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), self.schema).alias("data")) \
                .select("data.*")
            
            # Convert timestamp
            parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))
            
            # Process with windowing
            from pyspark.sql.functions import window, avg, max, min
            windowed_df = parsed_df \
                .withWatermark("timestamp", "1 minute") \
                .groupBy(window(col("timestamp"), "5 minutes")) \
                .agg(
                    avg("last_price").alias("avg_price"),
                    max("last_price").alias("max_price"),
                    min("last_price").alias("min_price"),
                    avg("volume_24h").alias("avg_volume")
                )
            
            # Output to console for debugging
            query = windowed_df.writeStream \
                .outputMode("complete") \
                .format("console") \
                .option("truncate", "false") \
                .start()
            
            # Write to CSV files
            csv_query = windowed_df.writeStream \
                .outputMode("append") \
                .format("csv") \
                .option("path", output_path) \
                .option("checkpointLocation", f"{output_path}/checkpoint") \
                .start()
            
            # Wait for termination
            query.awaitTermination()
            csv_query.awaitTermination()
            
            return True
        except Exception as e:
            print(f"Error in stream processing: {e}")
            return False

    def stop(self):
        """Stop Spark session"""
        self.spark.stop()


if __name__ == "__main__":
    # Example usage
    processor = SparkProcessor()
    
    # Choose either batch or streaming mode
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "stream":
        print("Starting stream processing...")
        processor.process_stream()
    else:
        print("Starting batch processing...")
        processor.process_batch()