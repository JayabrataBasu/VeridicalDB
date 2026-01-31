package com.veridicaldb.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Example demonstrating transaction management with VeridicalDB JDBC driver.
 * Shows commit, rollback, and auto-commit control.
 */
public class TransactionExample {
    
    public static void main(String[] args) {
        String url = "jdbc:veridicaldb://localhost:5432/mydb";
        String user = "admin";
        String password = "";
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected successfully");
            
            // Create accounts table
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS accounts (id INT, name TEXT, balance INT)");
                stmt.executeUpdate("DELETE FROM accounts"); // Clear existing data
                
                // Insert initial balances
                stmt.executeUpdate("INSERT INTO accounts VALUES (1, 'Alice', 1000)");
                stmt.executeUpdate("INSERT INTO accounts VALUES (2, 'Bob', 1000)");
                System.out.println("Initial balances created");
            }
            
            // Successful transaction: Transfer $100 from Alice to Bob
            System.out.println("\nPerforming transfer: Alice -> Bob ($100)");
            conn.setAutoCommit(false);
            
            try (Statement stmt = conn.createStatement()) {
                int rows1 = stmt.executeUpdate("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
                int rows2 = stmt.executeUpdate("UPDATE accounts SET balance = balance + 100 WHERE id = 2");
                
                if (rows1 == 1 && rows2 == 1) {
                    conn.commit();
                    System.out.println("Transaction committed successfully");
                } else {
                    conn.rollback();
                    System.out.println("Transaction rolled back - invalid row counts");
                }
            } catch (SQLException e) {
                conn.rollback();
                System.err.println("Transaction rolled back due to error: " + e.getMessage());
            }
            
            // Show balances after successful transfer
            showBalances(conn);
            
            // Failed transaction: Attempt to overdraw Alice's account
            System.out.println("\nAttempting invalid transfer: Alice -> Bob ($2000)");
            conn.setAutoCommit(false);
            
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("UPDATE accounts SET balance = balance - 2000 WHERE id = 1");
                
                // Check for negative balance
                ResultSet rs = stmt.executeQuery("SELECT balance FROM accounts WHERE id = 1");
                if (rs.next() && rs.getInt(1) < 0) {
                    throw new SQLException("Insufficient funds - balance would be negative");
                }
                
                stmt.executeUpdate("UPDATE accounts SET balance = balance + 2000 WHERE id = 2");
                conn.commit();
                System.out.println("Transaction committed");
                
            } catch (SQLException e) {
                conn.rollback();
                System.out.println("Transaction rolled back: " + e.getMessage());
            }
            
            // Show balances after rollback
            showBalances(conn);
            
            // Restore auto-commit
            conn.setAutoCommit(true);
            System.out.println("\nAuto-commit restored");
            
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void showBalances(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, name, balance FROM accounts ORDER BY id")) {
            
            System.out.println("\nCurrent balances:");
            System.out.println("ID\tName\tBalance");
            System.out.println("--\t----\t-------");
            
            while (rs.next()) {
                System.out.printf("%d\t%s\t$%d%n",
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("balance")
                );
            }
        }
    }
}
