from neo4j import GraphDatabase
import json
 
# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "neo4j123"
 
def load_nodes(jsonl_file):
    try:
        print("Adding CreditCard node")
        # Initialize Neo4j driver
        driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))
        
        def insert_credit_card(tx, node):
            query = """
            MERGE (cc:CreditCard {creditcard_number: $CreditCardNumber})
            """
            tx.run(
                query,
                CreditCardNumber=node["attributes"]["CreditCardNumber"]
            )
        
        with driver.session() as session:
            # Read JSONL file and process each line
            with open(jsonl_file, "r") as file:
                for line in file:
                    node = json.loads(line.strip())
                    # Validate attributes before insertion
                    if "attributes" not in node or not all(k in node["attributes"] for k in ["CreditCardNumber", "ML_embedding", "ML_degCen", "ML_batch_no"]):
                        print(f"Skipping invalid node: {node}")
                        continue
                    session.execute_write(insert_credit_card, node)
        
        print("CreditCard nodes loaded successfully!")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        driver.close()

# load_nodes(r"D:\neo4jCreditcard\datasets\Creditcard.jsonl")