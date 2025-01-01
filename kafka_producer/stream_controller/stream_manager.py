import time
# from kafka_producer.fin_trans._2dl_sensor import ISensor
# from fin_trans.pydantic_data import Transaction_reg
from stream_controller.transactions import *


class StreamManager:
    def __init__(self):
        self.transactions = []
        self.stream_handler = None

    def add_sensor(self, transaction: ITransactions):
        self.transactions.append(transaction)

    def set_stream_handler(self, handler):
        self.stream_handler = handler

    def start_streaming(self, interval: float = 1.0):
        """Continuously read data from transactions and send it."""
        if not self.stream_handler:
            raise ValueError("StreamHandler is not set.")

        print("Starting data streaming...")
        while True:
            for transaction in self.transactions:
                print(f"Transaction ID: {transaction.transaction_id}")

                if transaction.transaction_id % 100 == 0:
                    data = transaction.read_fraud_location()
                elif transaction.transaction_id % 150 == 0:
                    # small transaction amount
                    data = transaction.read_fraud_small_amount()
                    self.stream_handler.send(
                        topic=transaction.topic, data=data)
                    #
                    data = transaction.read_fraud_large_amount()
                else:
                    data = transaction.read_data()

                print(f"topic: {transaction.topic}")
                self.stream_handler.send(
                    topic=transaction.topic, data=data)
            time.sleep(interval)
