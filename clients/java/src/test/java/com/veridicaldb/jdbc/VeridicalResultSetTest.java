package com.veridicaldb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

/**
 * Standalone test class for VeridicalResultSet behavior.
 * (No JUnit dependency; tests can be called manually or via reflection)
 */
public class VeridicalResultSetTest {
    
    private static final String PASS = "✓";
    private static final String FAIL = "✗";
    
    public static void main(String[] args) {
        VeridicalResultSetTest test = new VeridicalResultSetTest();
        
        try {
            test.scrollInsensitiveNavigationWorks();
            System.out.println(PASS + " scrollInsensitiveNavigationWorks");
        } catch (Exception e) {
            System.out.println(FAIL + " scrollInsensitiveNavigationWorks: " + e.getMessage());
        }
        
        try {
            test.forwardOnlyRejectsScrollableOperations();
            System.out.println(PASS + " forwardOnlyRejectsScrollableOperations");
        } catch (Exception e) {
            System.out.println(FAIL + " forwardOnlyRejectsScrollableOperations: " + e.getMessage());
        }
        
        try {
            test.updatableResultSetStagesAndAppliesChanges();
            System.out.println(PASS + " updatableResultSetStagesAndAppliesChanges");
        } catch (Exception e) {
            System.out.println(FAIL + " updatableResultSetStagesAndAppliesChanges: " + e.getMessage());
        }
        
        try {
            test.readOnlyResultSetRejectsUpdates();
            System.out.println(PASS + " readOnlyResultSetRejectsUpdates");
        } catch (Exception e) {
            System.out.println(FAIL + " readOnlyResultSetRejectsUpdates: " + e.getMessage());
        }
    }
    
    private static void assertTrue(boolean condition) {
        if (!condition) {
            throw new AssertionError("Expected true, got false");
        }
    }
    
    private static void assertFalse(boolean condition) {
        if (condition) {
            throw new AssertionError("Expected false, got true");
        }
    }
    
    private static void assertEquals(Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError("Expected " + expected + ", got " + actual);
        }
    }

    public void scrollInsensitiveNavigationWorks() throws SQLException {
        VeridicalResultSet rs = new VeridicalResultSet(
                null,
                rowDescription(),
                sampleRows(),
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        assertTrue(rs.first());
        assertEquals(1, rs.getInt("id"));
        assertEquals("alpha", rs.getString("name"));

        assertTrue(rs.absolute(3));
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.isLast());

        assertTrue(rs.relative(-1));
        assertEquals(2, rs.getInt(1));

        assertTrue(rs.previous());
        assertEquals(1, rs.getInt(1));

        rs.beforeFirst();
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.previous());
    }

    public void forwardOnlyRejectsScrollableOperations() throws SQLException {
        VeridicalResultSet rs = new VeridicalResultSet(
                null,
                rowDescription(),
                sampleRows(),
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);

        rs.next();
        
        // Test that previous() throws
        try {
            rs.previous();
            throw new AssertionError("Expected SQLFeatureNotSupportedException from previous()");
        } catch (SQLFeatureNotSupportedException e) {
            // Expected
        }
        
        // Test that absolute() throws
        try {
            rs.absolute(1);
            throw new AssertionError("Expected SQLFeatureNotSupportedException from absolute()");
        } catch (SQLFeatureNotSupportedException e) {
            // Expected
        }
    }

    public void updatableResultSetStagesAndAppliesChanges() throws SQLException {
        VeridicalResultSet rs = new VeridicalResultSet(
                null,
                rowDescription(),
                sampleRows(),
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        assertTrue(rs.next());
        rs.updateString("name", "updated-alpha");
        assertFalse(rs.rowUpdated());

        rs.updateRow();
        assertTrue(rs.rowUpdated());
        assertEquals("updated-alpha", rs.getString("name"));

        rs.cancelRowUpdates();
        assertFalse(rs.rowUpdated());
    }

    public void readOnlyResultSetRejectsUpdates() throws SQLException {
        VeridicalResultSet rs = new VeridicalResultSet(
                null,
                rowDescription(),
                sampleRows(),
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        rs.next();
        try {
            rs.updateString(2, "x");
            throw new AssertionError("Expected SQLFeatureNotSupportedException from updateString()");
        } catch (SQLFeatureNotSupportedException e) {
            // Expected
        }
    }

    private static WireProtocol.RowDescription rowDescription() {
        WireProtocol.RowDescription rowDesc = new WireProtocol.RowDescription();
        rowDesc.addColumn("id", 0, (short) 1, TypeMapper.PG_INT4, (short) 4, -1, (short) 0);
        rowDesc.addColumn("name", 0, (short) 2, TypeMapper.PG_VARCHAR, (short) -1, -1, (short) 0);
        return rowDesc;
    }

    private static List<Object[]> sampleRows() {
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] {1, "alpha"});
        rows.add(new Object[] {2, "beta"});
        rows.add(new Object[] {3, "gamma"});
        return rows;
    }
}
