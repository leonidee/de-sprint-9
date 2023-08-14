

# kafkacat commands
run-producer:
	kcat -b rc1a-j1ulkqntckv14qrd.mdb.yandexcloud.net:9091 \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=producer_consumer \
		-X sasl.password="4kkDxgdnwuagovH7JuQy" \
		-X ssl.ca.location=./CA.pem \
		-P -t order-service_orders -K:

run-consumer:
	kcat -b rc1a-j1ulkqntckv14qrd.mdb.yandexcloud.net:9091,rc1b-9rnkj4ds3r9g1bpl.mdb.yandexcloud.net:9091 \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=producer_consumer \
		-X sasl.password="4kkDxgdnwuagovH7JuQy" \
		-X ssl.ca.location=/Users/leonidgrisenkov/Code/de-sprint-9/CA.pem \
		-C -o beginning -t order-service_orders \
		-f 'Key: %k\nValue: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'


# yandex-cloud commands
up-redis:
	yc managed-redis cluster start de-redis-04

check-redis:
	yc managed-redis cluster get de-redis-04 | rg "status"

down-redis:
	yc managed-redis cluster stop de-redis-04
