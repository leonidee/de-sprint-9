#!/bin/bash

 yc managed-kafka topic create stg-service-orders \
    --cluster-name de-kafka-01 \
    --partitions 1 \
    --replication-factor 1 \
    --compression-type gzip