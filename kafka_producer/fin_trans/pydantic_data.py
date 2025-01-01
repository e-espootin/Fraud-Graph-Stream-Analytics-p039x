from pydantic import BaseModel, Field
from faker import Faker
from random import uniform
from typing import Dict, Optional
from datetime import datetime


class Transaction_reg(BaseModel):
    transaction_id: int = 1
    t_datetime: str = '2021-01-01 00:00:00'
    amount: Optional[float] = None
    merchant_name: str = "Merchant"
    merchant_id: int = 100
    customer_name: str = "Customer"
    customer_id: int = 500
    location_id: int = 200
    payment_method: str = "Credit_Card"
    terminal_id: int = 300
    card_type: str = "Credit"
    card_brand: str = "Visa"
    transaction_type: str = "Purchase"
    transaction_status: str = "Success"
    transaction_category: str = "Retail"
    transaction_channel: str = "POS"
    merchant_bank: str = "Bank of America"
    customer_bank: str = "Chase"

    @staticmethod
    def generate_data(*, transaction_id: int) -> Dict:
        fake = Faker()

        m = Transaction_reg(
            transaction_id=transaction_id,  # fake.uuid4(),
            t_datetime=str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
            amount=round(uniform(10.0, 1000000.0), 2),
            merchant_name=fake.company(),
            merchant_id=int(uniform(100, 200)),
            customer_name=fake.name(),
            customer_id=int(uniform(500, 600)),
            location_id=int(uniform(200, 300)),
            payment_method=fake.credit_card_provider(),
            terminal_id=int(uniform(300, 400)),
            card_type=fake.random_element(elements=("Credit", "Debit")),
            card_brand=fake.random_element(
                elements=["Visa", "MasterCard", "American Express", "Discover"]),
            transaction_type=fake.random_element(
                elements=("Purchase", "Refund")),
            transaction_status=fake.random_element(
                elements=("Success", "Failed", "Pending")),
            transaction_category=fake.random_element(
                elements=("Retail", "Grocery", "Electronics")),
            transaction_channel=fake.random_element(
                elements=("POS", "Online", "Mobile")),
            merchant_bank=fake.random_element(
                elements=("Bank of America", "Wells Fargo", "Citi", "Chase", "US Bank", "Capital One")),
            customer_bank=fake.random_element(
                elements=("Chase", "Bank of America", "Wells Fargo", "Citi", "US Bank", "Capital One")),
        )
        # print(m.model_dump())
        return m.model_dump()
