"""
Transaction management example for VeridicalDB Python driver.
"""

import veridicaldb


def transfer_money(conn, from_account, to_account, amount):
    """
    Transfer money between accounts using a transaction.
    
    Demonstrates ACID properties with automatic rollback on error.
    """
    cursor = conn.cursor()
    
    try:
        # Start transaction (implicit)
        print(f"\nTransferring ${amount} from account {from_account} to {to_account}...")
        
        # Check source account balance
        cursor.execute("SELECT balance FROM accounts WHERE id = ?", (from_account,))
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Account {from_account} not found")
        
        balance = row[0]
        if balance < amount:
            raise ValueError(f"Insufficient funds: ${balance} < ${amount}")
        
        # Deduct from source account
        cursor.execute(
            "UPDATE accounts SET balance = balance - ? WHERE id = ?",
            (amount, from_account)
        )
        
        # Add to destination account
        cursor.execute(
            "UPDATE accounts SET balance = balance + ? WHERE id = ?",
            (amount, to_account)
        )
        
        # Commit transaction
        conn.commit()
        print(f"Transfer successful!")
        
        return True
        
    except Exception as e:
        # Rollback on error
        print(f"Transfer failed: {e}")
        conn.rollback()
        return False


def main():
    print("Transaction example...")
    
    with veridicaldb.connect(host='localhost', database='default') as conn:
        cursor = conn.cursor()
        
        # Create accounts table
        print("Creating accounts table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INT PRIMARY KEY,
                name TEXT,
                balance FLOAT
            )
        """)
        
        # Insert test accounts
        print("Setting up test accounts...")
        accounts = [
            (1, 'Alice', 1000.0),
            (2, 'Bob', 500.0),
            (3, 'Carol', 750.0),
        ]
        
        for account in accounts:
            cursor.execute(
                "INSERT INTO accounts (id, name, balance) VALUES (?, ?, ?)",
                account
            )
        
        conn.commit()
        
        # Print initial balances
        print("\nInitial balances:")
        cursor.execute("SELECT id, name, balance FROM accounts ORDER BY id")
        for row in cursor.fetchall():
            print(f"  Account {row[0]} ({row[1]}): ${row[2]:.2f}")
        
        # Successful transfer
        transfer_money(conn, from_account=1, to_account=2, amount=200.0)
        
        # Failed transfer (insufficient funds)
        transfer_money(conn, from_account=2, to_account=3, amount=1000.0)
        
        # Another successful transfer
        transfer_money(conn, from_account=3, to_account=1, amount=100.0)
        
        # Print final balances
        print("\nFinal balances:")
        cursor.execute("SELECT id, name, balance FROM accounts ORDER BY id")
        for row in cursor.fetchall():
            print(f"  Account {row[0]} ({row[1]}): ${row[2]:.2f}")
        
        # Clean up
        cursor.execute("DROP TABLE accounts")
        conn.commit()
        
        print("\nSuccess!")


if __name__ == '__main__':
    main()
