# Food Delivery Streaming Pipeline
Assignment 2 - Data Stores & Pipelines  
Anik Das (2025EM1100026)

## Quick Start

```bash
chmod +x run_pipeline.sh && ./run_pipeline.sh
```

This will:
1. Build Docker image with dependencies
2. Start PostgreSQL, Kafka, Zookeeper, Spark
3. Create database with 10 sample records
4. Setup Data Lake directories

## Running the Pipeline

### Terminal 1 - Start CDC Producer
```bash
docker exec -it spark_food_delivery bash -c 'cd /opt/spark/work-dir && \
  /opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  --master local[*] \
  producers/orders_cdc_producer.py --config configs/orders_stream.yml'
```

### Terminal 2 - Start Stream Consumer
```bash
docker exec -it spark_food_delivery bash -c 'cd /opt/spark/work-dir && \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  --master local[*] \
  consumers/orders_stream_consumer.py --config configs/orders_stream.yml'
```

### Terminal 3 - Insert Test Records
```bash
docker exec postgres_food_delivery psql -U student -d food_delivery_db -c "
INSERT INTO orders_2025EM1100026 (customer_name, restaurant_name, item, amount, order_status) VALUES
('Test User 1', 'Restaurant A', 'Item A', 150.00, 'PLACED'),
('Test User 2', 'Restaurant B', 'Item B', 250.00, 'PREPARING'),
('Test User 3', 'Restaurant C', 'Item C', 350.00, 'DELIVERED'),
('Test User 4', 'Restaurant D', 'Item D', 450.00, 'PLACED'),
('Test User 5', 'Restaurant E', 'Item E', 550.00, 'CANCELLED');
"
```

### Check Data Lake Output
```bash
docker exec spark_food_delivery ls -la /datalake/output/orders/

docker exec spark_food_delivery python3 -c "
import pandas as pd
df = pd.read_parquet('/datalake/output/orders/')
print(f'Total records: {len(df)}')
print(df[['order_id', 'customer_name', 'amount', 'order_status', 'date']].to_string())
"
```

## How it Works

```
PostgreSQL (orders_2025EM1100026)
       |
       v
CDC Producer (polls every 5 sec)
       |
       v
Kafka (2025EM1100026_food_orders_raw)
       |
       v
Spark Consumer (cleans data)
       |
       v
Data Lake (parquet partitioned by date)
```

## Requirements

- Docker 20.10+
- Docker Compose 1.29+
- Around 4GB RAM

## Files

```
local/
  db/orders.sql            - table schema and sample data
  producers/orders_cdc_producer.py
  consumers/orders_stream_consumer.py
  configs/orders_stream.yml
  Dockerfile
  docker-compose.yml
  run_pipeline.sh
```

## What Gets Tested

- Initial 10 records load from postgres
- Incremental insert (add 5 more, only new ones go to kafka)
- Data cleaning (null order_id and negative amounts filtered)

## Cleanup

```bash
docker-compose down -v
```

## Debug Commands

```bash
# see kafka messages
docker exec kafka_food_delivery kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic 2025EM1100026_food_orders_raw \
  --from-beginning

# count postgres records
docker exec postgres_food_delivery psql -U student -d food_delivery_db \
  -c "SELECT COUNT(*) FROM orders_2025EM1100026;"
```
