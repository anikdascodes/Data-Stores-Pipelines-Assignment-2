#!/usr/bin/env python3
"""
Simplified Food Delivery Orders Stream Consumer
Assignment 2 - Data Stores & Pipelines
Student: Anik Das (2025EM1100026)

This simplified version works with local PySpark and processes data in batches.
"""

import argparse
import os
import json
from datetime import datetime
from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

def load_config(config_path):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def process_batch(messages, config):
    """Process a batch of messages and save to Parquet"""
    if not messages:
        return 0
    
    # Convert messages to DataFrame
    df = pd.DataFrame(messages)
    
    # Data cleaning as per assignment requirements:
    # 1. Remove records with null order_id
    # 2. Remove records with negative amounts
    df = df[df['order_id'].notna()]
    df = df[df['amount'] > 0]
    
    if len(df) == 0:
        print("No valid records after cleaning")
        return 0
    
    # Add date column for partitioning
    df['date'] = pd.to_datetime(df['created_at']).dt.date
    
    # Save to Parquet with partitioning by date
    output_path = config['datalake']['path']
    os.makedirs(output_path, exist_ok=True)
    
    for date in df['date'].unique():
        date_str = str(date)
        date_path = os.path.join(output_path, f"date={date_str}")
        os.makedirs(date_path, exist_ok=True)
        
        # Filter data for this date
        date_df = df[df['date'] == date]
        
        # Remove date column for Parquet file
        date_df = date_df.drop('date', axis=1)
        
        # Save to Parquet
        parquet_file = os.path.join(date_path, f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        date_df.to_parquet(parquet_file, index=False)
        print(f"Saved {len(date_df)} records to {parquet_file}")
    
    return len(df)

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Food Orders Stream Consumer')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    print(f"Starting Simplified Stream Consumer for topic: {config['kafka']['topic']}")
    print(f"Data Lake path: {config['datalake']['path']}")
    
    # Create directories if they don't exist
    os.makedirs(config['datalake']['path'], exist_ok=True)
    
    # Connect to Kafka
    try:
        consumer = KafkaConsumer(
            config['kafka']['topic'],
            bootstrap_servers=config['kafka']['brokers'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='food-orders-consumer-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("✅ Connected to Kafka")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return
    
    print("Stream Consumer is running... Press Ctrl+C to stop")
    
    try:
        batch = []
        batch_size = 0
        
        for message in consumer:
            try:
                # Add message to batch
                batch.append(message.value)
                batch_size += 1
                
                print(f"Received order {message.value.get('order_id', 'unknown')} from Kafka")
                
                # Process batch when we have 5 messages or every 10 seconds
                if batch_size >= 5:
                    print(f"Processing batch of {batch_size} messages...")
                    processed_count = process_batch(batch, config)
                    print(f"✅ Processed {processed_count} messages")
                    
                    # Reset batch
                    batch = []
                    batch_size = 0
                    
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        print("Stream Consumer stopped by user")
        # Process remaining messages
        if batch:
            print(f"Processing final batch of {len(batch)} messages...")
            processed_count = process_batch(batch, config)
            print(f"✅ Processed {processed_count} final messages")
    except Exception as e:
        print(f"Error in stream consumer: {e}")
    finally:
        consumer.close()
        print("Stream Consumer shutdown complete")

if __name__ == "__main__":
    main()