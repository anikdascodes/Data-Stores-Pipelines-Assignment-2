#!/bin/bash
# run consumer

/opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --master local[*] \
    consumers/orders_stream_consumer.py \
    --config configs/orders_stream.yml
