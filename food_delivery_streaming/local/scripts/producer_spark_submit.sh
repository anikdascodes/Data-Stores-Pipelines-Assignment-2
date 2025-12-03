#!/bin/bash
# run producer

/opt/spark/bin/spark-submit \
    --packages org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --master local[*] \
    producers/orders_cdc_producer.py \
    --config configs/orders_stream.yml
