#!/usr/bin/env python3
"""
Kafka consumer for food orders
Anik Das - 2025EM1100026

Reads from kafka, cleans data, writes parquet to datalake
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import yaml

def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Food Orders Stream Consumer')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FoodOrdersStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Consumer for topic: {config['kafka']['topic']}")
    print(f"Output path: {config['datalake']['path']}")
    
    # Define schema for incoming JSON events (exact format from assignment)
    order_schema = StructType([
        StructField("order_id", IntegerType(), False),  # Required field
        StructField("customer_name", StringType(), True),
        StructField("restaurant_name", StringType(), True),
        StructField("item", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("order_status", StringType(), True),
        StructField("created_at", StringType(), True)  # ISO format timestamp as string
    ])
    
    try:
        # Create streaming DataFrame from Kafka
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config['kafka']['brokers']) \
            .option("subscribe", config['kafka']['topic']) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print(f"Connected to kafka at {config['kafka']['brokers']}")
        
        # Parse JSON data from Kafka value column
        parsed_df = kafka_stream \
            .select(from_json(col("value").cast("string"), order_schema).alias("order_data")) \
            .select("order_data.*")
        
        # clean data - remove nulls and bad amounts
        from pyspark.sql.functions import to_timestamp
        cleaned_df = parsed_df \
            .filter(col("order_id").isNotNull()) \
            .filter(col("amount") > 0) \
            .withColumn("created_at_timestamp", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
            .withColumn("date", to_date(col("created_at_timestamp")))
        
        print("Applied data cleaning (null order_id, negative amounts filtered)")
        
        # Create directories if they don't exist
        os.makedirs(config['datalake']['path'], exist_ok=True)
        os.makedirs(config['streaming']['checkpoint_location'], exist_ok=True)
        
        # Write cleaned data to Data Lake
        query = cleaned_df.writeStream \
            .outputMode("append") \
            .format(config['datalake']['format']) \
            .option("path", config['datalake']['path']) \
            .option("checkpointLocation", config['streaming']['checkpoint_location']) \
            .partitionBy("date") \
            .trigger(processingTime='5 seconds')  # Match producer polling interval

        # Start the streaming query
        streaming_query = query.start()

        print(f"Writing to {config['datalake']['path']}")
        print("Consumer running... Ctrl+C to stop")

        # Wait for termination
        streaming_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("Stream Consumer stopped by user")
    except Exception as e:
        print(f"Error in stream consumer: {e}")
        raise
    finally:
        spark.stop()
        print("Stream Consumer shutdown complete")

if __name__ == "__main__":
    main()