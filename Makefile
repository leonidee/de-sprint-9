include .env
export

# kafkacat commands
run-producer:
	kafkacat -b $$KAFKA_BOOTSTRAP_SERVERS \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=$$KAFKA_USERNAME \
		-X sasl.password=$$KAFKA_PASSWORD \
		-X ssl.ca.location=./CA.pem \
		-P -t $(topic) -K:

run-consumer:
	kafkacat -b $$KAFKA_BOOTSTRAP_SERVERS \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=$$KAFKA_USERNAME \
		-X sasl.password=$$KAFKA_PASSWORD \
		-X ssl.ca.location=./CA.pem \
		-C -o end -t $(topic) \
		-f 'Key: %k\nValue: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'

do-test:
	@echo $$YC_REDIS_NAME

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

list-kafka-topics:
	yc managed-kafka topic list --cluster-name $$YC_KAFKA_NAME

