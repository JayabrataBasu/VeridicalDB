package com.veridicaldb.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Example demonstrating batch operations with VeridicalDB JDBC driver.
 * Shows efficient bulk inserts using batch execution.
 */
public class BatchExample {
    
    public static void main(String[] args) {
        String url = "jdbc:veridicaldb://localhost:5432/mydb";
        String user = "admin";
        String password = "";
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected successfully");
            
            // Create table
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS products (id INT, name TEXT, price REAL)");
                stmt.executeUpdate("DELETE FROM products");
                System.out.println("Table prepared");
            }
            
            // Batch insert using prepared statement
            String insertSQL = "INSERT INTO products (id, name, price) VALUES (?, ?, ?)";
            long startTime = System.currentTimeMillis();
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                conn.setAutoCommit(false);
                
                // Insert 1000 products in batches of 100
                int batchSize = 100;
                int totalRecords = 1000;
                
                for (int i = 1; i <= totalRecords; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "Product " + i);
                    pstmt.setDouble(3, 10.0 + (i % 100));
                    pstmt.addBatch();
                    
                    // Execute batch every 100 records
                    if (i % batchSize == 0) {
                        int[] results = pstmt.executeBatch();
                        System.out.printf("Batch %d: inserted %d records%n", 
                            i / batchSize, results.length);
                    }
                }
                
                // Execute remaining records
                if (totalRecords % batchSize != 0) {
                    int[] results = pstmt.executeBatch();
                    System.out.printf("Final batch: inserted %d records%n", results.length);
                }
                
                conn.commit();
                long endTime = System.currentTimeMillis();
                
                System.out.printf("\nInserted %d records in %d ms%n", 
                    totalRecords, (endTime - startTime));
            }
            
            // Verify data
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM products")) {
                
                if (rs.next()) {
                    int count = rs.getInt(1);
                    System.out.printf("Total products in database: %d%n", count);
                }
            }
            
            // Show sample products
            try (Statement stmt = conn.createStatement()) {
                stmt.setMaxRows(10); // Limit to first 10
                
                ResultSet rs = stmt.executeQuery(
                    "SELECT id, name, price FROM products ORDER BY id LIMIT 10");
                
                System.out.println("\nSample products:");
                System.out.println("ID\tName\t\tPrice");
                System.out.println("--\t----\t\t-----");
                
                while (rs.next()) {
                    System.out.printf("%d\t%s\t$%.2f%n",
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getFloat("price")
                    );
                }
            }
            
            conn.setAutoCommit(true);
            
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
