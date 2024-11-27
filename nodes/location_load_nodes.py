from neo4j import GraphDatabase
import json
import threading

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "neo4j123"

BATCH_SIZE = 500  # Adjust the batch size as necessary for optimal performance
NUM_THREADS = 8  # Number of threads for concurrent processing

def load_nodes(jsonl_file):
    try:
        print("Adding Location nodes in batch with multi-threading")

        # Initialize Neo4j driver
        driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))

        def insert_nodes_batch(tx, nodes):
            # Create a batch insert query with UNWIND for bulk insertion
            query = """
            UNWIND $nodes AS node
            MERGE (l:Location {loc_id: node.id})
            SET l.loc_lat = node.Lat,
                l.loc_lon = node.Lon
            """
            tx.run(query, nodes=nodes)

        def process_batch(start_line, end_line, thread_id):
            nodes_batch = []
            with open(jsonl_file, "r") as file:
                for idx, line in enumerate(file):
                    if idx < start_line:
                        continue
                    if idx >= end_line:
                        break
                    node = json.loads(line.strip())
                    nodes_batch.append({
                        "id": node["Loc_id"],  # Loc_id as the unique identifier
                        "Lat": node["Lat"],
                        "Lon": node["Lon"]
                    })
                    
                    # Process the batch after reaching the batch size
                    if len(nodes_batch) >= BATCH_SIZE:
                        with driver.session() as session:
                            session.execute_write(insert_nodes_batch, nodes_batch)
                        nodes_batch = []  # Reset the batch

                # Process any remaining nodes in the batch
                if nodes_batch:
                    with driver.session() as session:
                        session.execute_write(insert_nodes_batch, nodes_batch)

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
        print("Location nodes loaded successfully with multi-threading!")

    except Exception as e:
        print(f"Error: {e}")

# Call the function with the file path
