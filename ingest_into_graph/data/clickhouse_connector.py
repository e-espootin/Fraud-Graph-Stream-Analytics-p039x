from datetime import datetime
from clickhouse_driver import Client
from config.clickhouse_config import CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB


class ClickHouseConnector:
    def __init__(self, database: str):
        self.database = database
        self.client = Client(
            host=CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=database,
        )

    def fetch_transactions(self, query):
        return self.client.execute(query)

    def get_last_transaction_id(self, *, table: str):
        query = f"SELECT max(transaction_id) as transaction_id FROM {
            self.database}.{table};"
        print(f"Query is: {query}")
        res = self.client.execute(query)[0][0]
        print(f"Last transaction id is: {res}")
        return int(res)

    def get_last_transaction_datetime(self, *, table: str) -> datetime:
        query = f"SELECT if(max(t_datetime) IS NOT NULL, max(t_datetime) , '2025-01-01 17:34:37') AS event_time  FROM {
            self.database}.{table};"
        # print(f"Query is: {query}")
        res = self.client.execute(query)[0][0]
        # print(f"Last transaction datetime is: {res}")
        return res
