### fully clear graph database
g.V().drop().iterate()

g.V().count() // Should return 0
g.E().count() // Should return 0

# TODO async