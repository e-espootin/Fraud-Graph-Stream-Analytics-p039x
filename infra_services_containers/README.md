## pyspark custom
- build custome pyspark image with correct versions

## how to add postgressql
- find from ref : https://mvnrepository.com/artifact/org.postgresql/postgresql/42.7.4
- add > .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4,org.apache.kafka:kafka-clients:3.2.0,org.postgresql:postgresql:42.7.4") \
-  restart kernel

## how connect to scylla
- https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.12/3.3.0
- required > scalaVersion 2.12