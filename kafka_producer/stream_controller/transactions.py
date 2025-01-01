from fin_trans.pydantic_data import *
from fin_trans.fraud_transactions import Transaction_Fraud
from abc import ABC, abstractmethod


class ITransactions(ABC):
    @abstractmethod
    def read_data(self) -> dict:
        pass

    @abstractmethod
    def get_id(self) -> str:
        pass

    @abstractmethod
    def get_topic(self) -> str:
        pass

    def read_fraud_location(self) -> dict:
        pass

    def read_fraud_large_amount(self) -> dict:
        pass

    def read_fraud_small_amount(self) -> dict:
        pass


class Transactions(ITransactions):
    def __init__(self, *, transaction_id: int, topic_name: str):
        self.transaction_id: int = int(transaction_id)
        self.topic = topic_name
        # Create an instance of Transaction_reg
        self.transaction_reg = Transaction_reg()
        self.transaction_fraud = Transaction_Fraud()

    def read_data(self) -> dict:
        self.transaction_id += 1  # TODO
        return self.transaction_reg.generate_data(transaction_id=self.transaction_id)

    def get_id(self) -> int:
        return self.transaction_id

    def get_topic(self) -> str:
        return self.topic

    def read_fraud_location(self) -> dict:
        self.transaction_id += 1  # TODO
        return self.transaction_fraud.gen_transaction_fraud_location(transaction_id=self.transaction_id)

    def read_fraud_large_amount(self) -> dict:
        self.transaction_id += 1  # TODO
        return self.transaction_fraud.gen_transaction_fraud_large_amount(transaction_id=self.transaction_id)

    def read_fraud_small_amount(self) -> dict:
        self.transaction_id += 1  # TODO
        return self.transaction_fraud.gen_transaction_fraud_small_amount(transaction_id=self.transaction_id)
