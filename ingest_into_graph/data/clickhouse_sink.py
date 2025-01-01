from abc import ABC, abstractmethod
from clickhouse_driver import Client
from config.clickhouse_config import CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB
from models.transaction import Transaction


class AbstractClickhouseSink(ABC):
    @abstractmethod
    def sink_data(self, data):
        pass


class ClickhouseSink(AbstractClickhouseSink):
    def __init__(self, *, database: str, table: str):
        self.client = Client(
            host=CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=database,
        )
        self.table = table

    def sink_data(self, data: Transaction):
        try:
            # TODO , looking for better and more dynamic way to sink data

            # Get all attributes of the Transaction object
            # columns = [attr for attr in dir(data) if not callable(
            #     getattr(data, attr)) and not attr.startswith("__")]

            # transaction_id: int, t_datetime: str, amount: float, merchant_name: str, merchant_id: int,
            #      customer_name: str, customer_id: int, location_id: int, payment_method: int, terminal_id: int,
            #      card_type: str, card_brand: str, transaction_type: str, transaction_status: str,
            #      transaction_category: str, transaction_channel: str, merchant_bank: str,
            #      customer_bank: str
            columns = ['transaction_id', 't_datetime', 'amount', 'merchant_name', 'merchant_id', 'customer_name',
                       'customer_id', 'location_id', 'payment_method', 'terminal_id', 'card_type', 'card_brand',
                       'transaction_type', 'transaction_status', 'transaction_category', 'transaction_channel',
                       'merchant_bank', 'customer_bank']

            values = [getattr(data, attr) for attr in columns]

            # Construct the query string
            columns_str = ", ".join(columns)
            # placeholders = "%d, '%s', %.2f, '%s', %d, '%s', %d, %d, %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'"
            placeholders = "%d, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
            query = f"INSERT INTO {self.table} ({columns_str}) VALUES ({data.terminal_id}, \
                '{data.t_datetime}', \
                {data.amount}, \
                '{data.merchant_name}', \
                {data.merchant_id}, \
                '{data.customer_name}', \
                {data.customer_id}, \
                {data.location_id}, \
                '{data.payment_method}', \
                {data.terminal_id}, \
                '{data.card_type}', \
                '{data.card_brand}', \
                '{data.transaction_type}', \
                '{data.transaction_status}', \
                '{data.transaction_category}', \
                '{data.transaction_channel}', \
                '{data.merchant_bank}', \
                '{data.customer_bank}')"

            # print(f"query: {query}")
            # print(f"values: {values}")

            # Execute the query
            self.client.execute(query)
            # self.client.execute(query, values)
        except Exception as e:
            print(f"Error sinking data in sink_data: {e}")
            return False

    # create table
    def create_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {self.table} (transaction_id Int64, t_datetime String, amount Float64, merchant_name String, merchant_id Int64, customer_name String, customer_id Int64, location_id Int64, payment_method String, terminal_id Int64, card_type String, card_brand String, transaction_type String, transaction_status String, transaction_category String, transaction_channel String, merchant_bank String, customer_bank String) ENGINE = MergeTree() ORDER BY transaction_id"
        self.client.execute(query)
        print(f"Table {self.table} created successfully.")
        return True