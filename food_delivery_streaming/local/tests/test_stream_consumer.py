#!/usr/bin/env python3
"""
Test suite for Stream Consumer
Assignment 2 - Data Stores & Pipelines
Student: Anik Das (2025EM1100026)
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
from consumers.orders_stream_consumer import load_config

class TestStreamConsumer(unittest.TestCase):
    """Test cases for Stream Consumer functionality"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests"""
        cls.spark = SparkSession.builder \
            .appName("TestStreamConsumer") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()

    def test_config_loading(self):
        """Test configuration file loading"""
        config = load_config("configs/orders_stream.yml")

        # Verify all required sections exist
        self.assertIn("postgres", config)
        self.assertIn("kafka", config)
        self.assertIn("datalake", config)
        self.assertIn("streaming", config)

        # Verify specific required fields
        self.assertIn("jdbc_url", config["postgres"])
        self.assertIn("brokers", config["kafka"])
        self.assertIn("topic", config["kafka"])
        self.assertIn("path", config["datalake"])

    def test_schema_validation(self):
        """Test schema validation for incoming data"""
        # Define test schema (same as in consumer)
        test_schema = StructType([
            StructField("order_id", IntegerType(), False),
            StructField("customer_name", StringType(), True),
            StructField("restaurant_name", StringType(), True),
            StructField("item", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("order_status", StringType(), True),
            StructField("created_at", StringType(), True)  # ISO format as string
        ])

        # Create test data
        test_data = [
            (101, "John Doe", "Burger Junction", "Veg Burger", 220.0, "PLACED", "2025-11-18T12:24:00Z"),
            (102, "Jane Smith", "Pizza Palace", "Margherita Pizza", 450.0, "PREPARING", "2025-11-18T12:25:00Z"),
            (103, None, "KFC Express", "Zinger Burger", -50.0, "DELIVERED", "2025-11-18T12:26:00Z")  # Invalid data
        ]

        # Create DataFrame
        df = self.spark.createDataFrame(test_data, schema=test_schema)

        # Test data cleaning logic
        cleaned_df = df.filter(col("order_id").isNotNull()) \
                       .filter(col("amount") > 0)

        # Verify cleaning worked
        self.assertEqual(cleaned_df.count(), 2)  # Should remove the invalid record
        self.assertEqual(cleaned_df.filter(col("order_id") == 103).count(), 0)  # Invalid record removed

    def test_timestamp_parsing(self):
        """Test timestamp parsing and date extraction"""
        from pyspark.sql.functions import to_timestamp, to_date

        test_schema = StructType([
            StructField("order_id", IntegerType(), False),
            StructField("created_at", StringType(), True)
        ])

        test_data = [
            (101, "2025-11-18T12:24:00Z"),
            (102, "2025-11-19T15:30:00Z")
        ]

        df = self.spark.createDataFrame(test_data, schema=test_schema)

        # Apply same transformation as in consumer
        result_df = df.withColumn("created_at_timestamp", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                      .withColumn("date", to_date(col("created_at_timestamp")))

        # Verify date extraction
        dates = [row.date for row in result_df.select("date").collect()]
        self.assertEqual(len(dates), 2)
        self.assertIsNotNone(dates[0])

    def test_data_validation(self):
        """Test data validation rules"""
        test_schema = StructType([
            StructField("order_id", IntegerType(), False),
            StructField("amount", DoubleType(), True),
            StructField("order_status", StringType(), True)
        ])

        # Test data with various validation issues
        test_data = [
            (101, 220.0, "PLACED"),        # Valid
            (102, -10.0, "PREPARING"),     # Invalid amount
            (None, 300.0, "DELIVERED"),    # Invalid order_id
            (103, 0.0, "CANCELLED"),       # Zero amount (should be filtered)
            (104, 450.0, "INVALID_STATUS") # Invalid status (but not filtered by current rules)
        ]

        df = self.spark.createDataFrame(test_data, schema=test_schema)

        # Apply validation rules
        validated_df = df.filter(col("order_id").isNotNull()) \
                         .filter(col("amount") > 0)

        # Should have 3 valid records (101, 103, 104)
        self.assertEqual(validated_df.count(), 3)

        # Verify specific records
        valid_ids = [row.order_id for row in validated_df.select("order_id").collect()]
        self.assertIn(101, valid_ids)
        self.assertIn(103, valid_ids)
        self.assertIn(104, valid_ids)
        self.assertNotIn(102, valid_ids)  # Negative amount removed
        self.assertNotIn(None, valid_ids) # Null order_id removed

if __name__ == "__main__":
    unittest.main()