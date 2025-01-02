from datetime import datetime
from clickhouse_driver import Client


class ClickHouseConnector:
    def __init__(self, database: str):
        self.database = database
        self.client = Client(
            host='localhost',
            user='click',
            password='click',
            database='testdb1',
        )

    def get_last_transaction_id(self, *, table: str):
        query = f"SELECT max(transaction_id) as transaction_id FROM {
            self.database}.{table};"
        # print(f"Query is: {query}")
        res = self.client.execute(query)[0][0]
        # print(f"Last transaction id is: {res}")
        return int(res)
