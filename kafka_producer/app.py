from time import sleep
import os
# from fin_trans.pydantic_data import Transaction_reg
from stream_controller.transactions import *
from stream_controller.stream_manager import StreamManager
from stream_controller.kafka_stream_handler import StreamHandler
from stream_controller.transactions import Transactions


def main():
    pass


if __name__ == "__main__":
    try:
        # TODO , get counter from file or last item in db
        trans_id_counter = os.getenv('counter', 100001)
        # Initialize gen transaction data
        trans_reg = Transactions(
            transaction_id=trans_id_counter, topic_name='fin-trans')
        # trans_reg = [Transactions(
        #     transaction_id=trans_id_counter, topic_name='transaction').read_data() for _ in range(2)]

        # Create the stream manager
        stream_manager = StreamManager()
        stream_manager.add_sensor(trans_reg)

        # Set up the stream handler
        stream_handler = StreamHandler()
        stream_manager.set_stream_handler(stream_handler)

        # Start streaming data
        stream_manager.start_streaming(
            interval=3.0)  # Stream every 10 seconds

    except KeyboardInterrupt as e:
        print("\n KeyboardInterrupt : Streaming stopped.")
        os.environ['counter'] = str(trans_reg.get_id())
        raise e
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e
