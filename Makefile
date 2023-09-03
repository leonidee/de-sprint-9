include .env
export

# kafkacat commands
run-producer:
	kafkacat -b $$YC_KAFKA_BOOTSTRAP_SERVERS \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=$$YC_KAFKA_USERNAME \
		-X sasl.password=$$YC_KAFKA_PASSWORD \
		-X ssl.ca.location=./CA.pem \
		-P -t $(topic)

run-consumer:
	kafkacat -b $$YC_KAFKA_BOOTSTRAP_SERVERS \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=$$YC_KAFKA_USERNAME \
		-X sasl.password=$$YC_KAFKA_PASSWORD \
		-X ssl.ca.location=./CA.pem \
		-C -o beginning -t $(topic) \
		-f 'Key: %k\nValue: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'

# yandex-cloud commands
# redis
up-redis:
	yc managed-redis cluster start $$YC_REDIS_NAME

check-redis:
	yc managed-redis cluster get $$YC_REDIS_NAME | grep -i "status"

down-redis:
	yc managed-redis cluster stop $$YC_REDIS_NAME

# kafka
up-kafka:
	yc managed-kafka cluster start $$YC_KAFKA_NAME

check-kafka:
	yc managed-kafka cluster get $$YC_KAFKA_NAME | grep -i "status"

down-kafka:
	yc managed-kafka cluster stop $$YC_KAFKA_NAME

create-kafka-topic:
	yc managed-kafka topic create $(topic) --cluster-name $$YC_KAFKA_NAME --partitions 1 --replication-factor 1 --compression-type gzip

list-kafka-topics:
	yc managed-kafka topic list --cluster-name $$YC_KAFKA_NAME

# postgresql
up-pg:
	yc managed-postgresql cluster start $$YC_POSTGRE_NAME

check-pg:
	yc managed-postgresql cluster get $$YC_POSTGRE_NAME | grep -i "status"

down-pg:
	yc managed-postgresql cluster stop $$YC_POSTGRE_NAME


