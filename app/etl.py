import pandas as pd
import psycopg
from neo4j import GraphDatabase

# ----- Postgres connection (from docker-compose) -----
PG_DSN = "postgresql://app:app@postgres:5432/shop"

# ----- Neo4j connection (from docker-compose) -----
NEO_URI = "bolt://neo4j:7687"
NEO_AUTH = ("neo4j", "password")

def load_tables():
    with psycopg.connect(PG_DSN) as conn:
        customers   = pd.read_sql("SELECT * FROM customers", conn)
        categories  = pd.read_sql("SELECT * FROM categories", conn)
        products    = pd.read_sql("SELECT * FROM products", conn)
        orders      = pd.read_sql("SELECT * FROM orders", conn)
        order_items = pd.read_sql("SELECT * FROM order_items", conn)
        events      = pd.read_sql("SELECT * FROM events", conn)
    return customers, categories, products, orders, order_items, events

def run_cypher(tx, query, parameters=None):
    tx.run(query, parameters or {})

def main():
    customers, categories, products, orders, order_items, events = load_tables()

    driver = GraphDatabase.driver(NEO_URI, auth=NEO_AUTH)
    with driver.session() as ses:
        # Nodes
        ses.execute_write(run_cypher, """
        UNWIND $rows AS r
        MERGE (c:Category {id:r.id})
        SET   c.name = r.name
        """, {"rows": categories.to_dict("records")})

        ses.execute_write(run_cypher, """
        UNWIND $rows AS r
        MERGE (p:Product {id:r.id})
        SET   p.name = r.name, p.price = toFloat(r.price)
        """, {"rows": products.to_dict("records")})

        ses.execute_write(run_cypher, """
        UNWIND $rows AS r
        MERGE (u:Customer {id:r.id})
        SET   u.name = r.name, u.join_date = date(r.join_date)
        """, {"rows": customers.to_dict("records")})

        ses.execute_write(run_cypher, """
        UNWIND $rows AS r
        MERGE (o:Order {id:r.id})
        SET   o.ts = datetime(r.ts)
        """, {"rows": orders.to_dict("records")})

        # Relationships
        ses.execute_write(run_cypher, """
        UNWIND $rows AS r
        MATCH (p:Product {id:r.id}), (c:Category {id:r.category_id})
        MERGE (p)-[:IN_CATEGORY]->(c)
        """, {"rows": products.to_dict("records")})

        ses.execute_write(run_cypher, """
        UNWIND $rows AS r
        MATCH (o:Order {id:r.order_id}), (p:Product {id:r.product_id})
        MERGE (o)-[li:LINE_ITEM]->(p)
        SET li.quantity = toInteger(r.quantity)
        """, {"rows": order_items.to_dict("records")})

        ses.execute_write(run_cypher, """
        UNWIND $rows AS r
        MATCH (o:Order {id:r.id}), (u:Customer {id:r.customer_id})
        MERGE (u)-[:PLACED]->(o)
        """, {"rows": orders.to_dict("records")})

        ses.execute_write(run_cypher, """
        UNWIND $rows AS r
        MATCH (u:Customer {id:r.customer_id}), (p:Product {id:r.product_id})
        MERGE (u)-[e:EVENT {id:r.id}]->(p)
        SET e.type = r.event_type, e.ts = datetime(r.ts)
        """, {"rows": events.to_dict("records")})

    driver.close()
    print("ETL complete.")

if __name__ == "__main__":
    main()
