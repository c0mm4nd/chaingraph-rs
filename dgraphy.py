# %%
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
session = driver.session()

# %%
# load csv from "/data/txs_0-16200000.out" NO HEADER AS row
load csv from "/data/nodes_nodup_0-16200000.csv" WITH HEADER AS row 
CREATE (:Address {addr: row["addr:ID"]})