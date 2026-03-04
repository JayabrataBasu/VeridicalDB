package com.veridicaldb.examples;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Basic example demonstrating VeridicalDB JDBC driver usage.
 * Shows connection, query execution, and result processing.
 */
public class BasicConnection {
    
    public static void main(String[] args) {
        String url = "jdbc:veridicaldb://localhost:5432/mydb";
        String user = "admin";
        String password = "";
        
        // Establish connection, ah yes, the try-with-resources pattern
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to VeridicalDB successfully!");
            
            // Create table
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS users (id INT, name TEXT, email TEXT)");
                System.out.println("Table created");
            }
            
            // Insert data
            String insertSQL = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "Alice");
                pstmt.setString(3, "alice@example.com");
                pstmt.executeUpdate();
                
                pstmt.setInt(1, 2);
                pstmt.setString(2, "Bob");
                pstmt.setString(3, "bob@example.com");
                pstmt.executeUpdate();
                
                System.out.println("Data inserted");
            }
            
            // Query data, thank you for teaching me tey catching resources properly
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, name, email FROM users")) {
                
                System.out.println("\nUsers:");
                System.out.println("ID\tName\tEmail");
                System.out.println("--\t----\t-----");
                
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    String email = rs.getString("email");
                    System.out.printf("%d\t%s\t%s%n", id, name, email);
                }
            }
            
            // Database metadata, I need to write a script to verify this, it seems fishy 
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("\nDatabase info:");
            System.out.println("  Product: " + meta.getDatabaseProductName());
            System.out.println("  Version: " + meta.getDatabaseProductVersion());
            System.out.println("  Driver: " + meta.getDriverName());
            System.out.println("  Driver Version: " + meta.getDriverVersion());
            
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("SQL State: " + e.getSQLState());
            e.printStackTrace();
        }
    }
}
