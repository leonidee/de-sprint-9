

# Apps

## Test app

```shell
docker compose up stg-collector-app --build
```


## Build image and push to registry


```shell
source .env
```

```shell
export APP_VERSION=v20230826-r1.1
```

```shell
docker build -t cr.yandex/$YC_REGISTRY_ID/stg-collector-app:$APP_VERSION -f ./apps/stg-collector-app/Dockerfile .
```

```shell
docker images | grep "stg-collector-app" 
```

Push image to registry:

```shell
docker push cr.yandex/$YC_REGISTRY_ID/stg-collector-app:$APP_VERSION
```

## Deploy helm chart to kubernetes cluster

Prepare k8s config file and add path to environment variable:

```shell
export KUBECONFIG=$HOME/.kube/config
```

```shell
cd /apps/stg-collector-app/helm
```

```shell
helm upgrade --install --atomic stg-collector-app . -n c12-leonid-grishenkov 
```


---


```shell
source .env && docker build -t cr.yandex/$YC_REGISTRY_ID/stg-collector-app:v20230909-r1.0 -f ./apps/stg-collector-app/Dockerfile .
source .env && docker build -t cr.yandex/$YC_REGISTRY_ID/dds-collector-app:v20230909-r1.0 -f ./apps/dds-collector-app/Dockerfile .
source .env && docker build -t cr.yandex/$YC_REGISTRY_ID/cdm-collector-app:v20230909-r1.0 -f ./apps/cdm-collector-app/Dockerfile .

docker push cr.yandex/$YC_REGISTRY_ID/cdm-collector-app:v20230909-r1.0

helm upgrade --install --atomic cdm-collector-app . -n c12-leonid-grishenkov 
```

```shell
docker build -t mongodb:v0.1 -f ./apps/mongodb/Dockerfile .
```

# Services

## Postgresql

```shell
docker compose up postgres
```

Connect to cluster with `pgcli`:

```shell
pgcli -h localhost -U postgres  -p 5432 
```

Create databases for test and prod environment:

```shell
create database test;

create database prod;
```

Grant all priviliges to main user:

```shell
grant all privileges on database test to de_etl;

grant all privileges on database prod to de_etl;
```

Reconnect with main user:

```shell
pgcli -h localhost -U de_etl -d test -p 5432 
```

And execute DWH DDL:

```shell
\i scripts/sql/dwh-ddl.sql 
```


## Kafka 

```shell
source .env
```

Create topics:

```shell
yc managed-kafka topic create order-app-orders \
    --cluster-name $YC_KAFKA_NAME \
    --partitions 3 \
    --replication-factor 1 \
    --compression-type gzip \
    --cleanup-policy delete

yc managed-kafka topic create stg-collector-app-orders \
    --cluster-name $YC_KAFKA_NAME \
    --partitions 3 \
    --replication-factor 1 \
    --compression-type gzip \
    --cleanup-policy delete

yc managed-kafka topic create dds-collector-app-orders \
    --cluster-name $YC_KAFKA_NAME \
    --partitions 3 \
    --replication-factor 1 \
    --compression-type gzip \
    --cleanup-policy delete
```

```shell
yc managed-kafka user create $YC_KAFKA_USERNAME \
    --cluster-name $YC_KAFKA_NAME \
    --password $YC_KAFKA_PASSWORD \
    --permission topic=order-app-orders,role=producer,role=consumer \
    --permission topic=stg-collector-app-orders,role=producer,role=consumer \
    --permission topic=dds-collector-app-orders,role=producer,role=consumer
```




