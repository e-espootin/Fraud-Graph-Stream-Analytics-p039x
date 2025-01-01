### fully clear graph database
g.V().drop().iterate()

g.V().count() // Should return 0
g.E().count() // Should return 0
# search for a vertice
g.V().has('transaction_id', '100050').valueMap()
# TODO async