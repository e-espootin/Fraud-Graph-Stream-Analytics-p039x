import asyncio
import nest_asyncio
import json
from data.clickhouse_connector import ClickHouseConnector
from data.clickhouse_sink import ClickhouseSink
from config.clickhouse_config import STREAM_TABLE, TRANSACTION_TABLE, CLICKHOUSE_DB
from graph.janusgraph_client import JanusGraphClient
from models.transaction import Transaction
from utils.logger import setup_logger

logger = setup_logger()


async def main():
    logger.info("Starting the application")

    # fetch last transaction id that processed
    clickhouse = ClickHouseConnector(database=CLICKHOUSE_DB)
    last_processed_transaction_datetime = clickhouse.get_last_transaction_datetime(
        table=TRANSACTION_TABLE)

    # Update with your query
    query = f"SELECT * FROM {STREAM_TABLE} where event_time > '{
        last_processed_transaction_datetime}';"
    raw_transactions = clickhouse.fetch_transactions(query)
    logger.info(f"Fetched {len(raw_transactions)
                           } transactions from ClickHouse.")

    # ClickhouseSink
    sink = ClickhouseSink(database=CLICKHOUSE_DB, table=TRANSACTION_TABLE)

    # Process transactions
    janus_client = JanusGraphClient()
    try:
        for raw in raw_transactions:
            # print(f" raw record is: >>> {raw}")
            transaction_data = json.loads(raw[3])
            # print(f" raw record is: >>> {transaction_data}")
            transaction = Transaction(**transaction_data)
            logger.info(f"Processing transaction {
                transaction.transaction_id}")

            try:
                # write transaction into clickhouse or scylla
                sink.sink_data(transaction)
                #
                janus_client.add_transaction(transaction)
            except Exception as e:
                logger.error(f"Error processing transaction: {e}")
                continue

            logger.info(f"transaction {transaction.transaction_id} processed.")

        logger.info("Processing complete.")
    except Exception as e:
        logger.error(f"Error processing transaction: {e}")
    finally:
        janus_client.close_connection()

if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(main())
