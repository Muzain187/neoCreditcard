from neo4j import GraphDatabase
import json

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "neo4j123"

BATCH_SIZE = 1000  # Number of records to process in each batch

def load_nodes(jsonl_file):
    """
    Loads transactions into Neo4j in batches without threading.
    """
    try:
        print("Adding Transaction nodes in batch...")
        
        # Initialize Neo4j driver
        driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))
        
        def insert_transactions_batch(tx, transactions):
            # Create a batch insert query with UNWIND for bulk insertion
            query = """
            UNWIND $transactions AS transaction
            MERGE (t:Transaction {transaction_id: transaction.Transaction_id})
            SET t.transaction_amount = transaction.Amount,
                t.transaction_fraud_hist = transaction.Fraud,
                t.transaction_datetime = datetime(replace(transaction.Transaction_Datetime,' ','T'))
            """
            tx.run(query, transactions=transactions)
        
        with driver.session() as session:
            transactions_batch = []  # Temporary storage for batch
            
            # Read the JSONL file line by line
            with open(jsonl_file, "r") as file:
                for line in file:
                    # Parse the JSON object
                    transaction = json.loads(line.strip())
                    transactions_batch.append(transaction)
                    
                    # If the batch size is reached, process the batch
                    if len(transactions_batch) >= BATCH_SIZE:
                        session.execute_write(insert_transactions_batch, transactions_batch)
                        transactions_batch = []  # Clear the batch
            
            # Process any remaining transactions
            if transactions_batch:
                session.execute_write(insert_transactions_batch, transactions_batch)
        
        driver.close()
        print("Transaction nodes loaded successfully!")
    
    except Exception as e:
        print(f"Error: {e}")


