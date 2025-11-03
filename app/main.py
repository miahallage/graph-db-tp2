from fastapi import FastAPI, HTTPException
from typing import List, Dict, Any
from neo4j import GraphDatabase
from pydantic import BaseModel

app = FastAPI(title="Graph TP2 API", version="1.0")

# ---- Neo4j connection (matches docker-compose) ----
NEO_URI = "bolt://neo4j:7687"
NEO_AUTH = ("neo4j", "password")
driver = GraphDatabase.driver(NEO_URI, auth=NEO_AUTH)

@app.get("/health")
def health():
    # basic check that the app is alive
    return {"ok": True}

class Rec(BaseModel):
    product_id: str
    product_name: str
    score: int

@app.get("/recommendations/{customer_id}", response_model=list[Rec])
def recommend(customer_id: str, limit: int = 5):
    coop = """
    MATCH (u:Customer {id:$cid})-[:PLACED]->(:Order)-[:LINE_ITEM]->(p:Product)
    WITH u, collect(DISTINCT p) AS myProducts
    MATCH (u)-[:PLACED]->(:Order)-[:LINE_ITEM]->(pCommon:Product)
    MATCH (:Customer)-[:PLACED]->(:Order)-[:LINE_ITEM]->(pCommon)
          <-[:LINE_ITEM]-(:Order)-[:LINE_ITEM]->(rec:Product)
    WHERE NOT rec IN myProducts
    RETURN rec.id AS product_id, rec.name AS product_name, count(*) AS score
    ORDER BY score DESC, product_name ASC
    LIMIT $limit
    """
    pop = """
    MATCH (u:Customer {id:$cid})
    OPTIONAL MATCH (u)-[:PLACED]->(:Order)-[:LINE_ITEM]->(pBought:Product)
    WITH u, collect(DISTINCT pBought) AS myProducts
    MATCH (:Order)-[li:LINE_ITEM]->(p:Product)
    WHERE NOT p IN myProducts
    RETURN p.id AS product_id, p.name AS product_name, count(li) AS score
    ORDER BY score DESC, product_name ASC
    LIMIT $limit
    """
    with driver.session() as s:
        rows = s.run(coop, {"cid": customer_id, "limit": limit}).data()
        if rows:
            return rows
        # fallback to popularity
        return s.run(pop, {"cid": customer_id, "limit": limit}).data()
    
@app.get("/also-viewed/{product_id}")
def also_viewed(product_id: str, limit: int = 5):
    cypher = """
    MATCH (:Product {id:$pid})<-[e1:EVENT]-(:Customer)-[e2:EVENT]->(p:Product)
    WHERE p.id <> $pid
    RETURN p.id AS product_id, p.name AS product_name, count(e2) AS score
    ORDER BY score DESC, product_name ASC
    LIMIT $limit
    """
    with driver.session() as s:
        return s.run(cypher, {"pid": product_id, "limit": limit}).data()
