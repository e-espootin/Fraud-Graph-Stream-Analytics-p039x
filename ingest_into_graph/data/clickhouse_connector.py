from clickhouse_driver import Client
from config.clickhouse_config import CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD


class ClickHouseConnector:
    def __init__(self, database):
        self.client = Client(
            host=CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=database,
        )

    def fetch_transactions(self, query):
        return self.client.execute(query)
