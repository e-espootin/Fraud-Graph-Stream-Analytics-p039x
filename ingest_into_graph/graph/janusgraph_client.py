from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

from config.janusgraph_config import JANUSGRAPH_URI, JANUSGRAPH_GRAPH_NAME


class JanusGraphClient:
    def __init__(self):
        self.graph = Graph()
        self.connection = DriverRemoteConnection(
            JANUSGRAPH_URI, 'g')
        self.g = traversal().withRemote(self.connection)

    def close_connection(self):
        self.connection.close()

    # submit a script to gremlin server
    def go(self):
        with DriverRemoteConnection('ws://localhost:8182/gremlin', 'g') as remote_connection:
            g = traversal().withRemote(remote_connection)
            vertices = g.V().toList()
            return vertices

    def user_exists(self, user_id):
        user = self.g.V().has('User', 'user_id', user_id).toList()
        return len(user) > 0

    def user_exists2(self, user_id):
        hercules_age = self.g.V().has('id', '4216').values('label').iterate()
        print(f'Hercules is {hercules_age} years old.')

    def example1(self):
        try:
            self.g.addV('Transaction').property(
                'transaction_id', '123').iterate()
        finally:
            # Close connection
            self.connection.close()

    def add_transaction(self, transaction):
        try:
            exists = self.g.V().has('Transaction', 'transaction_id',
                                    int(transaction.transaction_id)).toList()
            # exists = self.g.V().has('transaction_id', 100050).valueMap() # not worked
            if exists:
                print(f"Transaction {
                      transaction.transaction_id} already exists. Skipping...")
                return

            # Add vertices
            self.g.addV('Transaction').property('transaction_id', transaction.transaction_id)\
                .property('datetime', transaction.t_datetime).property('amount', transaction.amount).iterate()
            self.g.addV('Merchant').property('merchant_id', transaction.merchant_id)\
                .property('name', transaction.merchant_name).iterate()
            self.g.addV('Customer').property('customer_id', transaction.customer_id)\
                .property('name', transaction.customer_name).iterate()
            self.g.addV('Location').property(
                'location_id', transaction.location_id).iterate()
            self.g.addV('PaymentMethod').property(
                'method', transaction.payment_method).iterate()
            self.g.addV('Terminal').property(
                'terminal_id', transaction.terminal_id).iterate()
            self.g.addV('CardType').property(
                'type', transaction.card_type).iterate()
            self.g.addV('CardBrand').property(
                'brand', transaction.card_brand).iterate()
            self.g.addV('TransactionType').property(
                'type', transaction.transaction_type).iterate()
            self.g.addV('TransactionStatus').property(
                'status', transaction.transaction_status).iterate()
            self.g.addV('TransactionCategory').property(
                'category', transaction.transaction_category).iterate()
            self.g.addV('TransactionChannel').property(
                'channel', transaction.transaction_channel).iterate()
            # self.g.addV('MerchantBank').property('bank_id', transaction.merchant_bank).iterate()
            # self.g.addV('CustomerBank').property('bank_id', transaction.customer_bank).iterate()
            self.g.addV('Bank').property(
                'bank_id', transaction.merchant_bank).iterate()

            # Add edges
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('PROCESSED_BY').to(__.V().has('Merchant', 'merchant_id', transaction.merchant_id)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('MADE_BY').to(__.V().has('Customer', 'customer_id', transaction.customer_id)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('AT').to(__.V().has('Location', 'location_id', transaction.location_id)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('USING').to(__.V().has('PaymentMethod', 'method', transaction.payment_method)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('ON_TERMINAL').to(__.V().has('Terminal', 'terminal_id', transaction.terminal_id)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('HAS_CARD_TYPE').to(__.V().has('CardType', 'type', transaction.card_type)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('HAS_CARD_BRAND').to(__.V().has('CardBrand', 'brand', transaction.card_brand)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('HAS_TRANSACTION_TYPE').to(__.V().has('TransactionType', 'type', transaction.transaction_type)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('HAS_TRANSACTION_STATUS').to(__.V().has('TransactionStatus', 'status', transaction.transaction_status)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('HAS_TRANSACTION_CATEGORY').to(__.V().has('TransactionCategory', 'category', transaction.transaction_category)).iterate()
            self.g.V().has('Transaction', 'transaction_id', transaction.transaction_id)\
                .addE('HAS_TRANSACTION_CHANNEL').to(__.V().has('TransactionChannel', 'channel', transaction.transaction_channel)).iterate()
            #
            self.g.V().has('Merchant', 'merchant_id', transaction.merchant_id)\
                .addE('USES_BANKING_SERVICES').to(__.V().has('Bank', 'bank_id', transaction.merchant_bank)).iterate()
            self.g.V().has('Customer', 'customer_id', transaction.customer_id)\
                .addE('USES_BANKING_SERVICES').to(__.V().has('Bank', 'bank_id', transaction.customer_bank)).iterate()

        except Exception as e:
            print(f"Error adding transaction: {e}")
            raise e
    '''
    # Define schema
    def define_schema(self):
        mgmt = self.graph.openManagement()

        # Define property keys

        transaction_id = mgmt.makePropertyKey(
            'transaction_id').dataType(str).make()
        datetime = mgmt.makePropertyKey(
            'datetime').dataType(datetime.datetime).make()
        amount = mgmt.makePropertyKey('amount').dataType(float).make()
        merchant_id = mgmt.makePropertyKey('merchant_id').dataType(str).make()
        customer_id = mgmt.makePropertyKey('customer_id').dataType(str).make()
        location_id = mgmt.makePropertyKey('location_id').dataType(str).make()
        method = mgmt.makePropertyKey('method').dataType(str).make()
        terminal_id = mgmt.makePropertyKey('terminal_id').dataType(str).make()
        type = mgmt.makePropertyKey('type').dataType(str).make()
        brand = mgmt.makePropertyKey('brand').dataType(str).make()
        status = mgmt.makePropertyKey('status').dataType(str).make()
        category = mgmt.makePropertyKey('category').dataType(str).make()
        channel = mgmt.makePropertyKey('channel').dataType(str).make()
        bank_id = mgmt.makePropertyKey('bank_id').dataType(str).make()

        # Define vertex labels
        transaction = mgmt.makeVertexLabel('Transaction').make()
        merchant = mgmt.makeVertexLabel('Merchant').make()
        customer = mgmt.makeVertexLabel('Customer').make()
        location = mgmt.makeVertexLabel('Location').make()
        payment_method = mgmt.makeVertexLabel('PaymentMethod').make()
        terminal = mgmt.makeVertexLabel('Terminal').make()
        card_type = mgmt.makeVertexLabel('CardType').make()
        card_brand = mgmt.makeVertexLabel('CardBrand').make()
        transaction_type = mgmt.makeVertexLabel('TransactionType').make()
        transaction_status = mgmt.makeVertexLabel('TransactionStatus').make()
        transaction_category = mgmt.makeVertexLabel(
            'TransactionCategory').make()
        transaction_channel = mgmt.makeVertexLabel('TransactionChannel').make()
        bank = mgmt.makeVertexLabel('Bank').make()

        # Define edge labels
        processed_by = mgmt.makeEdgeLabel('PROCESSED_BY').make()
        made_by = mgmt.makeEdgeLabel('MADE_BY').make()
        at = mgmt.makeEdgeLabel('AT').make()
        using = mgmt.makeEdgeLabel('USING').make()
        on_terminal = mgmt.makeEdgeLabel('ON_TERMINAL').make()
        has_card_type = mgmt.makeEdgeLabel('HAS_CARD_TYPE').make()
        has_card_brand = mgmt.makeEdgeLabel('HAS_CARD_BRAND').make()
        has_transaction_type = mgmt.makeEdgeLabel(
            'HAS_TRANSACTION_TYPE').make()
        has_transaction_status = mgmt.makeEdgeLabel(
            'HAS_TRANSACTION_STATUS').make()
        has_transaction_category = mgmt.makeEdgeLabel(
            'HAS_TRANSACTION_CATEGORY').make()
        has_transaction_channel = mgmt.makeEdgeLabel(
            'HAS_TRANSACTION_CHANNEL').make()
        uses_banking_services = mgmt.makeEdgeLabel(
            'USES_BANKING_SERVICES').make()

        mgmt.commit()
    '''
