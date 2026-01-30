"""
Type mapping example for VeridicalDB Python driver.
"""

import veridicaldb
from veridicaldb import Int32, Int64, Float64, Text, Boolean, Timestamp
from datetime import datetime


def main():
    print("Type mapping example...")
    
    with veridicaldb.connect(host='localhost', database='default') as conn:
        cursor = conn.cursor()
        
        # Create table with various types
        print("\nCreating table with various data types...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS type_test (
                id INT PRIMARY KEY,
                int32_val INT,
                int64_val BIGINT,
                float_val FLOAT,
                text_val TEXT,
                bool_val BOOLEAN,
                timestamp_val TIMESTAMP
            )
        """)
        
        # Insert data using explicit types
        print("Inserting data with explicit types...")
        cursor.execute("""
            INSERT INTO type_test VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            Int32(1),
            Int32(42),
            Int64(9223372036854775807),  # Max int64
            Float64(3.14159),
            Text("Hello, VeridicalDB!"),
            Boolean(True),
            Timestamp(datetime.now())
        ))
        
        # Insert data using Python native types
        print("Inserting data with Python native types...")
        cursor.execute("""
            INSERT INTO type_test VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            2,
            100,
            1000000000000,
            2.71828,
            "Python native types",
            False,
            datetime(2024, 1, 15, 10, 30, 0)
        ))
        
        # Insert NULL values
        print("Inserting NULL values...")
        cursor.execute("""
            INSERT INTO type_test (id, text_val) VALUES (?, ?)
        """, (3, "Only ID and text"))
        
        conn.commit()
        
        # Query and display data
        print("\nQuerying data...")
        cursor.execute("SELECT * FROM type_test ORDER BY id")
        
        print("\nColumn descriptions:")
        for desc in cursor.description:
            print(f"  {desc[0]}: type_oid={desc[1]}, size={desc[3]}")
        
        print("\nData rows:")
        for row in cursor.fetchall():
            print(f"\nRow ID {row[0]}:")
            print(f"  int32_val: {row[1]} (type: {type(row[1]).__name__})")
            print(f"  int64_val: {row[2]} (type: {type(row[2]).__name__})")
            print(f"  float_val: {row[3]} (type: {type(row[3]).__name__})")
            print(f"  text_val: {row[4]} (type: {type(row[4]).__name__})")
            print(f"  bool_val: {row[5]} (type: {type(row[5]).__name__})")
            print(f"  timestamp_val: {row[6]} (type: {type(row[6]).__name__})")
        
        # Test type conversions
        print("\n\nTesting type conversions...")
        
        # Integer overflow handling
        print("\n1. Integer overflow:")
        try:
            cursor.execute("INSERT INTO type_test (id, int32_val) VALUES (?, ?)", (10, 2**31))
        except Exception as e:
            print(f"  Expected error: {e}")
        
        # Boolean operations
        print("\n2. Boolean queries:")
        cursor.execute("SELECT id, text_val FROM type_test WHERE bool_val = ?", (True,))
        true_rows = cursor.fetchall()
        print(f"  Rows with bool_val=TRUE: {len(true_rows)}")
        for row in true_rows:
            print(f"    ID {row[0]}: {row[1]}")
        
        # String operations
        print("\n3. String matching:")
        cursor.execute("SELECT id, text_val FROM type_test WHERE text_val LIKE ?", ('%Python%',))
        matching = cursor.fetchall()
        print(f"  Rows matching 'Python': {len(matching)}")
        
        # Numeric comparisons
        print("\n4. Numeric comparisons:")
        cursor.execute("SELECT id, float_val FROM type_test WHERE float_val > ?", (3.0,))
        numeric = cursor.fetchall()
        print(f"  Rows with float_val > 3.0: {len(numeric)}")
        
        # NULL handling
        print("\n5. NULL handling:")
        cursor.execute("SELECT COUNT(*) FROM type_test WHERE int32_val IS NULL")
        null_count = cursor.fetchone()[0]
        print(f"  Rows with NULL int32_val: {null_count}")
        
        # Clean up
        cursor.execute("DROP TABLE type_test")
        conn.commit()
        
        print("\n\nSuccess!")


if __name__ == '__main__':
    main()
