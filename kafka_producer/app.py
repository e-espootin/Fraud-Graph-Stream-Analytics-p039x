from time import sleep
import os
# from fin_trans.pydantic_data import Transaction_reg
from stream_controller.transactions import *
from stream_controller.stream_manager import StreamManager
from stream_controller.kafka_stream_handler import StreamHandler
from stream_controller.transactions import Transactions
from data.clickhouse_connector import ClickHouseConnector
import argparse


def main():
    pass


if __name__ == "__main__":
    try:
        # Parse the arguments
        parser = argparse.ArgumentParser(
            description="Kafka Producer for Financial Transactions")
        parser.add_argument('--interval_sec', type=int, default=10,
                            help='Interval in seconds between streaming data')
        args = parser.parse_args()
        interval_sec = args.interval_sec

        # get counter from clickhouse
        click = ClickHouseConnector(database='testdb1')
        trans_id_counter = click.get_last_transaction_id(
            table='fin_transactions')
        if not trans_id_counter:
            trans_id_counter = os.getenv('counter', 100001)

        # Initialize gen transaction data
        trans_reg = Transactions(
            transaction_id=trans_id_counter, topic_name='fin-trans')

        # Create the stream manager
        stream_manager = StreamManager()
        stream_manager.add_sensor(trans_reg)

        # Set up the stream handler
        stream_handler = StreamHandler()
        stream_manager.set_stream_handler(stream_handler)

        # Start streaming data
        stream_manager.start_streaming(
            interval=interval_sec)  # Stream every n seconds

    except KeyboardInterrupt as e:
        print("\n KeyboardInterrupt : Streaming stopped.")
        os.environ['counter'] = str(trans_reg.get_id())
        raise e
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e
