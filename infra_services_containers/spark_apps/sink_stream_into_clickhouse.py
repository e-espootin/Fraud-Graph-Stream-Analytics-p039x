from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def write_to_clickhouse(batch_df, batch_id):
    try:
        print(f"Writing batch {batch_id} to ClickHouse")
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/testdb1") \
            .option("dbtable", "testdb1.fin_trans_table") \
            .option("user", "click") \
            .option("password", "click") \
            .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} written successfully")
    except Exception as e:
        print(f"Error writing batch {batch_id} to ClickHouse: {e}")
        raise e


def sink_stream_into_clickhouse():
    spark = SparkSession.builder \
        .appName("StreamToTable") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4,org.apache.kafka:kafka-clients:3.2.0,org.postgresql:postgresql:42.7.4,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2") \
        .getOrCreate()

    print("Spark version:", spark.version)
    print("Spark JARs:", spark.sparkContext._jsc.sc().jars())
    scala_version = spark.sparkContext._gateway.jvm.scala.util.Properties.versionString()
    print("Scala Version:", scala_version)

    df = spark\
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "fin_trans_topic") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Select and cast key and value columns to string
    query_df = df.selectExpr(
        "CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS kafka_value")

    query_df.printSchema()

    # Write the streaming data to PostgreSQL using foreachBatch
    query = query_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", "/home/jovyan/work/_spark_metadata/checkpoint_sink_stream_into_clickhouse/") \
        .start()

    query.awaitTermination()


def main():
    sink_stream_into_clickhouse()


if __name__ == "__main__":
    main()
