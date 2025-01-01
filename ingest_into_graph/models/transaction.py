class Transaction:
    def __init__(self, transaction_id: int, t_datetime: str, amount: float, merchant_name: str, merchant_id: int,
                 customer_name: str, customer_id: int, location_id: int, payment_method: str, terminal_id: int,
                 card_type: str, card_brand: str, transaction_type: str, transaction_status: str,
                 transaction_category: str, transaction_channel: str, merchant_bank: str,
                 customer_bank: str):
        self.transaction_id: int = transaction_id
        self.t_datetime: str = t_datetime
        self.amount: float = amount
        self.merchant_name: str = merchant_name
        self.merchant_id: int = merchant_id
        self.customer_name: str = customer_name
        self.customer_id: int = customer_id
        self.location_id: int = location_id
        self.payment_method: str = payment_method
        self.terminal_id: int = terminal_id
        self.card_type: str = card_type
        self.card_brand: str = card_brand
        self.transaction_type: str = transaction_type
        self.transaction_status: str = transaction_status
        self.transaction_category: str = transaction_category
        self.transaction_channel: str = transaction_channel
        self.merchant_bank: str = merchant_bank
        self.customer_bank: str = customer_bank
