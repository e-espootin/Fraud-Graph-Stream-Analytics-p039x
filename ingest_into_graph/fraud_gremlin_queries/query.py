'''

# Query 1: Find all the transactions with amount greater than 9,000,000
g.V().hasLabel('Transaction').has('amount', gt(9000000)).toList()

# Query 2: Find all the transactions with amount less than 10,000
g.V().hasLabel('Transaction').has('amount', __.lt(10000)).toList()

# query 3: find all transaction that has locastion between 900 and 910
g.V().hasLabel('Transaction').has('location_id', between(900, 910)).toList()


'''
