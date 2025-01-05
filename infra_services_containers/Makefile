container_name := pyspark-notebook
packages := org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4,org.apache.kafka:kafka-clients:3.2.0,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2
NOTEBOOK_FILE := $(notebook)


# .PHONY: submit

submit:
	rm -R _spark_metadata/checkpoint_sink_stream_into_clickhouse
	mkdir _spark_metadata/checkpoint_sink_stream_into_clickhouse
	docker exec $(container_name) spark-submit --packages $(packages) --deploy-mode client ./work/$(app)

submit-dev:
	rm -R _spark_metadata/checkpoint_sink_stream_into_clickhouse
	mkdir _spark_metadata/checkpoint_sink_stream_into_clickhouse
	docker exec pyspark-notebook spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4,org.apache.kafka:kafka-clients:3.2.0,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2 --deploy-mode client ./work/$(app)

build:
	docker-compose build

run:
	make down && docker-compose up -d

down:
	docker-compose down --volumes --remove-orphans