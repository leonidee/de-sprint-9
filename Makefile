include .env
export

# kafkacat commands
run-producer:
	kafkacat -b "$$YC_KAFKA_BOOTSTRAP_SERVER:$$YC_KAFKA_BOOTSTRAP_SERVER_PORT" \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=$$YC_KAFKA_USERNAME \
		-X sasl.password=$$YC_KAFKA_PASSWORD \
		-X ssl.ca.location=./CA.pem \
		-P -t $(topic)

run-consumer:
	kafkacat -b "$$YC_KAFKA_BOOTSTRAP_SERVER:$$YC_KAFKA_BOOTSTRAP_SERVER_PORT" \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=$$YC_KAFKA_USERNAME \
		-X sasl.password=$$YC_KAFKA_PASSWORD \
		-X ssl.ca.location=./CA.pem \
		-C -o beginning -t $(topic) \
		-f 'Key: %k\nValue: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'




