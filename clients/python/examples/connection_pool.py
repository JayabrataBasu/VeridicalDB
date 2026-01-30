"""
Connection pooling example for VeridicalDB Python driver.
"""

import time
import threading
from veridicaldb import ConnectionPool


def worker(pool, worker_id, num_queries):
    """Worker thread that executes queries using pooled connections."""
    print(f"Worker {worker_id} starting...")
    
    for i in range(num_queries):
        # Acquire connection from pool
        with pool.acquire() as conn:
            cursor = conn.cursor()
            
            # Execute query
            cursor.execute("SELECT ?, ?", (worker_id, i))
            result = cursor.fetchone()
            
            print(f"Worker {worker_id} query {i}: {result}")
            
            # Simulate some work
            time.sleep(0.1)
    
    print(f"Worker {worker_id} finished")


def main():
    print("Creating connection pool...")
    
    # Create connection pool with 2-5 connections
    pool = ConnectionPool(
        min_size=2,
        max_size=5,
        host='localhost',
        port=5432,
        database='default',
        user='admin',
        password=''
    )
    
    print(f"Pool created: {pool.size} connections available")
    print(f"Total connections: {pool.total_connections}")
    
    # Create worker threads
    num_workers = 10
    queries_per_worker = 5
    
    print(f"\nStarting {num_workers} workers with {queries_per_worker} queries each...")
    
    threads = []
    start_time = time.time()
    
    for i in range(num_workers):
        thread = threading.Thread(
            target=worker,
            args=(pool, i, queries_per_worker)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all workers to complete
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    
    print(f"\nAll workers completed in {end_time - start_time:.2f} seconds")
    print(f"Final pool state:")
    print(f"  Available connections: {pool.available_connections}")
    print(f"  Total connections: {pool.total_connections}")
    
    # Close pool
    pool.close()
    print("Pool closed")


if __name__ == '__main__':
    main()
