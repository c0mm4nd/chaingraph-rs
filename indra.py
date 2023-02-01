# %%
import indradb
import uuid
import csv
# Connect to the server and make sure it's up
client = indradb.Client("127.0.0.1:27615")
client.ping()

# %%
class UUID:
    def __init__(self, bytes):
        self.bytes = bytes

# Create a couple of vertices
# with open("nodes_nodup_0-16200000.csv") as f:
#     r = csv.DictReader(f)
    
#     # inserter = indradb.BulkInserter()
#     for row in r:
#         addr = row["addr:ID"]
#         print(addr)

#         id = uuid.uuid4()
#         v = indradb.Vertex(id, "Address")
#         # v_addr = indradb.NamedProperty("addr", addr)
#         # inserter.vertex(v).vertex_property(id, "addr", addr).execute(client)
#         client.create_vertex(v)
#         # client.set_vertex_properties(indradb.SpecificVertexQuery(id), v_addr)

# Add an edge between the vertices
# edge = indradb.Edge(out_v.id, "bar", in_v.id)
# client.create_edge(edge)

# Query for the edge
# results = list(client.get(indradb.SpecificEdgeQuery(edge))
# print(results)


out_v = indradb.Vertex(uuid.uuid4(), "person")
in_v = indradb.Vertex(uuid.uuid4(), "movie")
client.create_vertex(out_v)
client.create_vertex(in_v)