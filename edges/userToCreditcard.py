from neo4j import GraphDatabase
import json
 
# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "neo4j123"

def load_relationships(jsonl_file):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))
 
    def insert_relationship(tx, rel):
        query = """
        MATCH (a:User {user_id: $from_id}), (b:CreditCard {creditcard_number: $to_id})
        MERGE (a)-[r:TEST_2]->(b)
        """
        tx.run(query, from_id=rel["from_id"], to_id= int(rel["to_id"]))
 
    with driver.session() as session:
        with open(jsonl_file, "r") as file:
            for line in file:
                rel = json.loads(line.strip())
                # print(rel["from_id"])
                session.execute_write(insert_relationship, rel)
 
    driver.close()
    print("Relationships loaded successfully!")
 