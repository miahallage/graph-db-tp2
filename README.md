In this project I implemented a mini recommendation system using PostgreSQL, Neo4j and FastAPI, all in docker containers.
It is my graphdb tp2 

Project stucture:
graph-tp2/
─ app/                     
   ─ main.py          
   ─ etl.py             
   ─ queries.cypher     
   ─ requirements.txt     

─ postgres/init/           
   ─ 01_schema.sql  
   ─ 02_schema.sql  

─ neo4j/               
─ docker-compose.yml       

It imports customer orders and products and generates recommendations using purchase similarity or popularity of item           

