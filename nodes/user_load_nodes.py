from neo4j import GraphDatabase
import json
 
# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "neo4j123"
 
def load_nodes(jsonl_file):
    try:
        # Initialize Neo4j driver
        print("Adding User node")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))
        
        def insert_node(tx, node):
            # Query to merge user nodes with their attributes
            query = """
            MERGE (n:User {user_id: $id})
            SET n.user_first_name = $First_Name,
                n.user_last_name = $Last_Name,
                n.user_gender = $Gender,
                n.user_dob = datetime(replace($DOB,' ','T'))
            """
            tx.run(
                query,
                id=node["attributes"]["SSA_No"],  # Use SSA_No as the unique identifier
                First_Name=node["attributes"]["First_Name"],
                Last_Name=node["attributes"]["Last_Name"],
                Gender=node["attributes"]["Gender"],
                DOB=node["attributes"]["DOB"]
            )
        
        with driver.session() as session:
            # Read JSONL file and process each line
            with open(jsonl_file, "r") as file:
                for line in file:
                    node = json.loads(line.strip())
                    session.execute_write(insert_node, node)
        
        # Close the driver
        driver.close()
        print("User nodes loaded successfully!")
    
    except Exception as e:
        print(f"Error: {e}")

# load_nodes(r"D:\neo4jCreditcard\datasets\User.jsonl")