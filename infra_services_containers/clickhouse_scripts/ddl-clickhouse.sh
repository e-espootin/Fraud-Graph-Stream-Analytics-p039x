#!/bin/sh
until curl -s http://clickhouse:8123/ping; do
    echo "Waiting for ClickHouse..."
    sleep 5
done

echo "ClickHouse is ready!"

db="testdb1"
echo 'creating database'
curl -X POST http://clickhouse:8123/ --user click:click --data "CREATE DATABASE IF NOT EXISTS ${db};"
echo 'database created'

echo 'creating table A'
query_tblA="CREATE TABLE IF NOT EXISTS ${db}.fin_trans_table (event_time DateTime DEFAULT now(), \
                event_type String DEFAULT 'stream', kafka_key String, kafka_value String \
                    ) ENGINE = MergeTree() ORDER BY kafka_key;"
echo $query_tblA

curl -X POST http://clickhouse:8123/ --user click:click --data "$query_tblA"
echo 'table A created'

echo 'creating table B'
query_tblB="CREATE TABLE IF NOT EXISTS ${db}.fin_transactions (transaction_id Int64, t_datetime String, \
                    amount Float64, merchant_name String, merchant_id Int64, customer_name String, \
                    customer_id Int64, location_id Int64, payment_method String, terminal_id Int64, \
                        card_type String, card_brand String, transaction_type String, transaction_status String, \
                            transaction_category String, transaction_channel String, merchant_bank String, \
                            customer_bank String) ENGINE = MergeTree() ORDER BY transaction_id;"
echo $query_tblB

curl -X POST http://clickhouse:8123/ --user click:click --data "$query_tblB"
echo 'table B created'

echo 'alter table'
query_ttl="ALTER TABLE ${db}.fin_trans_table MODIFY TTL event_time + INTERVAL 15 MINUTE;"
echo $query_ttl

curl -X POST http://clickhouse:8123/ --user click:click --data "$query_tblB"
echo 'table altered'
