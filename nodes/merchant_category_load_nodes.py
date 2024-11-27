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
        print("Adding Merchant Category nodes in batch with multi-threading")
        
        # Initialize Neo4j driver
        driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))

        def insert_category_batch(tx, categories):
            # Create a batch insert query with UNWIND for bulk insertion
            query = """
            UNWIND $categories AS category
            MERGE (c:Merchant_Category {category_id: category.id})
            SET c.category_name = category.Category,
                c.ml_ctid = category.ML_ctid,
                c.ml_degCen = category.ML_degCen,
                c.ml_batch_no = category.ML_batch_no
            """
            tx.run(query, categories=categories)

        def process_batch(start_line, end_line, thread_id):
            categories_batch = []
            with open(jsonl_file, "r") as file:
                for idx, line in enumerate(file):
                    if idx < start_line:
                        continue
                    if idx >= end_line:
                        break
                    category = json.loads(line.strip())
                    categories_batch.append({
                        "id": category["v_id"],  # Use v_id as the unique identifier
                        "Category": category["attributes"]["Category"],
                        "ML_ctid": category["attributes"]["ML_ctid"],
                        "ML_degCen": category["attributes"]["ML_degCen"],
                        "ML_batch_no": category["attributes"]["ML_batch_no"]
                    })
                    
                    # Process the batch after reaching the batch size
                    if len(categories_batch) >= BATCH_SIZE:
                        with driver.session() as session:
                            session.execute_write(insert_category_batch, categories_batch)
                        categories_batch = []  # Reset the batch

                # Process any remaining categories in the batch
                if categories_batch:
                    with driver.session() as session:
                        session.execute_write(insert_category_batch, categories_batch)
            
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
        print("Merchant Category nodes loaded successfully with multi-threading!")
    
    except Exception as e:
        print(f"Error: {e}")

# Call the function with the file path
