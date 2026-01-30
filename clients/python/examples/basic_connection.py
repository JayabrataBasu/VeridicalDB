"""
Basic connection and query example for VeridicalDB Python driver.
"""

import veridicaldb


def main():
    print("Connecting to VeridicalDB...")
    
    # Connect to VeridicalDB
    conn = veridicaldb.connect(
        host='localhost',
        port=5432,
        database='default',
        user='admin',
        password=''
    )
    
    print(f"Connected! Server version: {conn.get_server_version()}")
    
    try:
        # Create cursor
        cursor = conn.cursor()
        
        # Create table
        print("\nCreating table 'users'...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INT
            )
        """)
        
        # Insert data
        print("Inserting data...")
        users = [
            (1, 'Alice Smith', 'alice@example.com', 30),
            (2, 'Bob Jones', 'bob@example.com', 25),
            (3, 'Carol White', 'carol@example.com', 35),
            (4, 'David Brown', 'david@example.com', 28),
        ]
        
        for user in users:
            cursor.execute(
                "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
                user
            )
        
        conn.commit()
        print(f"Inserted {len(users)} users")
        
        # Query data
        print("\nQuerying users older than 26...")
        cursor.execute("SELECT * FROM users WHERE age > ?", (26,))
        
        # Print column names
        columns = [desc[0] for desc in cursor.description]
        print(f"Columns: {', '.join(columns)}")
        
        # Fetch and print results
        rows = cursor.fetchall()
        print(f"Found {len(rows)} users:")
        for row in rows:
            print(f"  ID: {row[0]}, Name: {row[1]}, Email: {row[2]}, Age: {row[3]}")
        
        # Aggregate query
        print("\nCalculating average age...")
        cursor.execute("SELECT COUNT(*), AVG(age) FROM users")
        count, avg_age = cursor.fetchone()
        print(f"Total users: {count}, Average age: {avg_age:.1f}")
        
        # Clean up
        print("\nDropping table...")
        cursor.execute("DROP TABLE users")
        conn.commit()
        
        print("\nSuccess!")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    
    finally:
        # Close connection
        conn.close()
        print("Connection closed")


if __name__ == '__main__':
    main()
