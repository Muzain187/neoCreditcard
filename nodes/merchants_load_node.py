from neo4j import GraphDatabase
import json
import threading

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "neo4j123"

BATCH_SIZE = 10000  # Adjust the batch size as necessary for optimal performance
NUM_THREADS = 8  # Number of threads for concurrent processing

def load_nodes(jsonl_file):
    try:
        print("Adding Merchant nodes in batch with multi-threading")
        
        # Initialize Neo4j driver
        driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))

        def insert_merchant_batch(tx, merchants):
            # Create a batch insert query with UNWIND for bulk insertion
            query = """
            UNWIND $merchants AS merchant
            MERGE (m:Merchant {merchant_id: merchant.id})
            SET m.merchant_name = merchant.Merchant_name,
                m.ml_ctid = merchant.ML_ctid,
                m.ml_ctlid = merchant.ML_ctlid,
                m.ml_degCen = merchant.ML_degCen,
                m.ml_risk_score = merchant.ML_Risk_Score,
                m.ml_batch_no = merchant.ML_batch_no
            """
            tx.run(query, merchants=merchants)

        def process_batch(start_line, end_line, thread_id):
            merchants_batch = []
            with open(jsonl_file, "r") as file:
                for idx, line in enumerate(file):
                    if idx < start_line:
                        continue
                    if idx >= end_line:
                        break
                    merchant = json.loads(line.strip())
                    merchants_batch.append({
                        "id": merchant["v_id"],  # Use v_id as the unique identifier
                        "Merchant_name": merchant["attributes"]["Merchant_name"],
                        "ML_ctid": merchant["attributes"]["ML_ctid"],
                        "ML_ctlid": merchant["attributes"]["ML_ctlid"],
                        "ML_degCen": merchant["attributes"]["ML_degCen"],
                        "ML_Risk_Score": merchant["attributes"]["ML_Risk_Score"],
                        "ML_batch_no": merchant["attributes"]["ML_batch_no"]
                    })
                    
                    # Process the batch after reaching the batch size
                    if len(merchants_batch) >= BATCH_SIZE:
                        with driver.session() as session:
                            session.execute_write(insert_merchant_batch, merchants_batch)
                        merchants_batch = []  # Reset the batch

                # Process any remaining merchants in the batch
                if merchants_batch:
                    with driver.session() as session:
                        session.execute_write(insert_merchant_batch, merchants_batch)
            
            print(f"Thread-{thread_id} finished processing lines {start_line} to {end_line}")

        # Split dataset into chunks for parallel processing
        with open(jsonl_file, "r") as file:
            total_lines = sum(1 for line in file)
        
        # Split the total lines across threads
        lines_per_thread = total_lines // NUM_THREADS
        threads = []

        for i in range(NUM_THREADS):
            start_line = i * lines_per_thread
            end_line = (i + 1) * lines_per_thread if i != NUM_THREADS - 1 else total_lines
            thread = threading.Thread(target=process_batch, args=(start_line, end_line, i))
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        # Close the driver
        driver.close()
        print("Merchant nodes loaded successfully with multi-threading!")
    
    except Exception as e:
        print(f"Error: {e}")

# Call the function with the file path
# load_merchants(r"D:\neo4jCreditcard\datasets\Merchants.jsonl")
