from collections import defaultdict

view_clusters = defaultdict(lambda: 0)
buy_clusters = defaultdict(lambda: 0)

# CREATE VIEW RELATIONSIPS
f1 = open('Downloads/views.csv', 'r')
cnter = 0
current_cluster = []
current_cluster_id = ''
for line in f1:
    cnter += 1
    if cnter %1000 == 0:
        print(cnter)

    line = line.strip().split(',')
    if line[0] == current_cluster_id:
        current_cluster.append(int(line[1]))
    else:
        current_cluster = list(set(current_cluster))
        current_cluster.sort()
        for k1 in range(len(current_cluster)):
            for k2 in range(k1+1, len(current_cluster)):
                view_clusters[(current_cluster[k1], current_cluster[k2])] += 1
                # create_view_link(tx, current_cluster[k1], current_cluster[k2])
                # session.write_transaction(create_view_link, current_cluster[k1], current_cluster[k2])
        current_cluster = []
        current_cluster_id = line[0]
        current_cluster.append(int(line[1]))

cnter = 0
# CREATE BUY RELATIONSHIPS
f1 = open('Downloads/buys.csv', 'r')
current_cluster = []
current_cluster_id = ''
for line in f1:
    cnter += 1
    if cnter %1000 == 0:
        print(cnter)
    line = line.strip().split(',')
    if line[0] == current_cluster_id:
        current_cluster.append(int(line[1]))
    else:
        current_cluster = list(set(current_cluster))
        current_cluster.sort()
        for k1 in range(len(current_cluster)):
            for k2 in range(k1+1, len(current_cluster)):
                buy_clusters[(current_cluster[k1], current_cluster[k2])] += 1
                # create_buy_link(tx, current_cluster[k1], current_cluster[k2])
                # session.write_transaction(create_buy_link, current_cluster[k1], current_cluster[k2])
        current_cluster = []
        current_cluster_id = line[0]
        current_cluster.append(int(line[1]))

import pickle

pickle.dump(dict(view_clusters), open('view_graph.p', 'wb'))
pickle.dump(dict(buy_clusters), open('buy_graph.p', 'wb'))

#Nodes and links creation for Neo4j

from neo4j.v1 import GraphDatabase,basic_auth
import pickle

driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=basic_auth("neo4j", "neo4j"))

def create_node(tx, sku_id, sku_name,sku_category,price,drug_form,is_otc):
    tx.run("CREATE (n: Med {sku_id: $sku_id, sku_name: $sku_name,sku_category: $sku_category,price: $price,drug_form: $drug_form,is_otc: $is_otc })", sku_id=sku_id, sku_name=sku_name,sku_category=sku_category,price=price,drug_form=drug_form,is_otc=is_otc)

def create_view_link(tx, sku_id1, sku_id2):
    tx.run("""
    MATCH (a), (b)
    WHERE a.sku_id = $sku_id1 AND b.sku_id = $sku_id2
    CREATE UNIQUE (a)-[r:VIEW]->(b)
    SET r.count = coalesce(r.count, 0) + 1
        """, sku_id1=sku_id1, sku_id2=sku_id2)

def create_buy_link(tx, sku_id1, sku_id2):
    tx.run("""
    MATCH (a), (b)
    WHERE a.sku_id = $sku_id1 AND b.sku_id = $sku_id2
    CREATE UNIQUE (a)-[r:BUY]->(b)
    SET r.count = coalesce(r.count, 0) + 1
        """, sku_id1=sku_id1, sku_id2=sku_id2)

def create_link(tx, sku_id1, sku_id2, weight, link_type):
    tx.run("""
    MATCH (sku1:Med {sku_id:$sku_id1}), (sku2:Med {sku_id: $sku_id2})
    CREATE (sku1)-[r:"""+link_type+"""]->(sku2)
    SET r.count = $weight
        """, sku_id1=sku_id1, sku_id2=sku_id2, weight=weight)

    # tx.run("""
    # MATCH (a), (b)
    # WHERE a.sku_id = $sku_id1 AND b.sku_id = $sku_id2
    # CREATE UNIQUE (a)-[r:"""+link_type+"""]->(b)
    # SET r.count = $weight
    #     """, sku_id1=sku_id1, sku_id2=sku_id2, weight=weight)

with driver.session() as session:
    # CREATE NODES
    #nodes = pickle.load(open('result.p', 'rb'))
    nodes = open('Documents/joined.csv', 'r')
    cnter = 0
    tx = session.begin_transaction()
    for line in nodes:
        cnter += 1
        if cnter %1000 == 0:
            print(cnter)
            tx.commit()
            tx = session.begin_transaction()
        line = line.strip().split(',')
        create_node(tx, int(line[1]),line[2],line[3],line[4],line[5],line[6])
        # session.write_transaction(create_node, int(k), v.title())
    del(nodes)
    cnter = 0
    tx.commit()
    tx = session.begin_transaction()

    tx.run("""CREATE INDEX ON :Med(sku_id)""")
    tx.commit()
    tx = session.begin_transaction()

    view_graph = pickle.load(open('view_graph.p', 'rb'))
    for k, v in view_graph.items():
        cnter += 1
        if cnter % 1000 == 0:
            print(cnter)
            tx.commit()
            tx = session.begin_transaction()
        create_link(tx, k[0], k[1], v, 'VIEW')
    del(view_graph)

    buy_graph = pickle.load(open('buy_graph.p', 'rb'))
    for k, v in buy_graph.items():
        cnter += 1
        if cnter % 1000 == 0:
            print(cnter)
            tx.commit()
            tx = session.begin_transaction()
        create_link(tx, k[0], k[1], v, 'BUY')
    del(buy_graph)
