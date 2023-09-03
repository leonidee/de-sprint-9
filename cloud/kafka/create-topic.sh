#!/usr/bin/env bash

 yc managed-kafka topic create dds-collector-app-orders \
    --cluster-name de-kafka-01 \
    --partitions 1 \
    --replication-factor 1 \
    --compression-type gzip