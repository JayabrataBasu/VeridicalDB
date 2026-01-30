"""
Async/await example for VeridicalDB Python driver.
"""

import asyncio
import veridicaldb


async def fetch_users(conn, min_age):
    """Fetch users asynchronously."""
    cursor = await conn.cursor()
    await cursor.execute("SELECT * FROM users WHERE age >= ?", (min_age,))
    rows = await cursor.fetchall()
    return rows


async def insert_user(conn, user_data):
    """Insert user asynchronously."""
    cursor = await conn.cursor()
    await cursor.execute(
        "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
        user_data
    )


async def main():
    print("Connecting to VeridicalDB asynchronously...")
    
    # Create async connection
    async with veridicaldb.connect_async(
        host='localhost',
        port=5432,
        database='default',
        user='admin',
        password=''
    ) as conn:
        print("Connected!")
        
        # Create table
        cursor = await conn.cursor()
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                name TEXT,
                age INT
            )
        """)
        
        # Insert multiple users concurrently
        print("\nInserting users concurrently...")
        users = [
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Carol', 35),
            (4, 'David', 28),
            (5, 'Eve', 32),
        ]
        
        # Execute inserts in parallel
        insert_tasks = [insert_user(conn, user) for user in users]
        await asyncio.gather(*insert_tasks)
        
        print(f"Inserted {len(users)} users")
        
        # Query users with different age filters concurrently
        print("\nQuerying users concurrently with different filters...")
        
        fetch_tasks = [
            fetch_users(conn, 25),
            fetch_users(conn, 30),
            fetch_users(conn, 35),
        ]
        
        results = await asyncio.gather(*fetch_tasks)
        
        for i, (min_age, rows) in enumerate(zip([25, 30, 35], results)):
            print(f"\nUsers aged >= {min_age}: {len(rows)} found")
            for row in rows:
                print(f"  ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")
        
        # Clean up
        await cursor.execute("DROP TABLE users")
        
        print("\nSuccess!")


if __name__ == '__main__':
    asyncio.run(main())
