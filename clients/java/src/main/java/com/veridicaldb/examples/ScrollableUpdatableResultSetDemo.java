package com.veridicaldb.examples;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class ScrollableUpdatableResultSetDemo {
    public static void main(String[] args) throws Exception {
        Class.forName("com.veridicaldb.jdbc.VeridicalDriver");

        String url = "jdbc:veridicaldb://localhost:15432/default";
        String tableName = "interop_demo_" + System.currentTimeMillis();
        Files.writeString(Path.of("/tmp/veridicaldb_interop_table.txt"), tableName + "\n", StandardCharsets.UTF_8);
        System.out.println("Interoperability table: " + tableName);
        try (Connection conn = DriverManager.getConnection(url, "", "")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + tableName + " (id INT, name TEXT)");
                stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 'alice')");
                stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (2, 'bob')");
                stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (3, 'carol')");
            }

              try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT id, name FROM " + tableName + " ORDER BY id",
                        ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                  ResultSet rs = ps.executeQuery()) {

                System.out.println("ResultSet type=" + rs.getType() + ", concurrency=" + rs.getConcurrency());

                if (!rs.next()) {
                    System.out.println("No rows returned by server for SELECT yet; continuing interoperability checks.");
                } else {
                    rs.beforeFirst();
                    rs.last();
                    System.out.println("Last row id=" + rs.getInt(1) + ", name=" + rs.getString(2));

                    rs.absolute(2);
                    System.out.println("Row 2 before update name=" + rs.getString(2));

                    rs.updateString(2, "bob_java_updated");
                    rs.updateRow();
                    System.out.println("Row 2 after update (result-set cache) name=" + rs.getString(2));
                }
            }

            try (Statement verify = conn.createStatement();
                  ResultSet rs = verify.executeQuery("SELECT id, name FROM " + tableName + " ORDER BY id")) {
                System.out.println("Final rows from server:");
                boolean any = false;
                while (rs.next()) {
                    any = true;
                    System.out.println("  id=" + rs.getInt(1) + ", name=" + rs.getString(2));
                }
                if (!any) {
                    System.out.println("  (no rows returned)");
                }
            }
        }
    }
}
