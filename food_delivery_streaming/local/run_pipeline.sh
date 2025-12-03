#!/bin/bash
# setup script for the streaming pipeline
# Anik Das - 2025EM1100026

set -e

echo "Setting up pipeline..."
echo ""

# cleanup old containers
echo "Cleaning up..."
docker-compose down -v 2>/dev/null || true

# build spark image
echo "Building spark image..."
docker-compose build --no-cache spark

# start everything
echo "Starting services..."
docker-compose up -d

# wait for them to come up
echo "Waiting for services (30s)..."
sleep 30

# check postgres
RECORD_COUNT=$(docker exec postgres_food_delivery psql -U student -d food_delivery_db -t -c "SELECT COUNT(*) FROM orders_2025EM1100026;" 2>/dev/null | tr -d ' ')
if [ "$RECORD_COUNT" -ge "10" ]; then
    echo "Postgres has $RECORD_COUNT records - good"
else
    echo "Error: postgres doesnt have expected records"
    exit 1
fi

# create datalake dirs
docker exec spark_food_delivery mkdir -p /datalake/output/orders /datalake/checkpoints/orders /datalake/lastprocess/orders
docker exec spark_food_delivery chmod -R 777 /datalake

echo ""
echo "Done! Now run:"
echo ""
echo "Terminal 1 (producer):"
echo "docker exec -it spark_food_delivery bash -c 'cd /opt/spark/work-dir && /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 --master local[*] producers/orders_cdc_producer.py --config configs/orders_stream.yml'"
echo ""
echo "Terminal 2 (consumer):"
echo "docker exec -it spark_food_delivery bash -c 'cd /opt/spark/work-dir && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 --master local[*] consumers/orders_stream_consumer.py --config configs/orders_stream.yml'"
echo ""
