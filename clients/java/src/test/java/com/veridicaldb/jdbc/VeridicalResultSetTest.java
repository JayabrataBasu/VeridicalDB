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

    @FunctionalInterface
    private interface ThrowingTest {
        void run() throws SQLException;
    }
    
    public static void main(String[] args) {
        VeridicalResultSetTest test = new VeridicalResultSetTest();

        runTest("scrollInsensitiveNavigationWorks", test::scrollInsensitiveNavigationWorks);
        runTest("forwardOnlyRejectsScrollableOperations", test::forwardOnlyRejectsScrollableOperations);
        runTest("updatableResultSetStagesAndAppliesChanges", test::updatableResultSetStagesAndAppliesChanges);
        runTest("readOnlyResultSetRejectsUpdates", test::readOnlyResultSetRejectsUpdates);
        runTest("absoluteAndRelativeOutOfRangeBehaviors", test::absoluteAndRelativeOutOfRangeBehaviors);
        runTest("findColumnCaseInsensitiveAndMissingColumn", test::findColumnCaseInsensitiveAndMissingColumn);
        runTest("closedResultSetRejectsOperations", test::closedResultSetRejectsOperations);
        runTest("wasNullTracksLastReadColumn", test::wasNullTracksLastReadColumn);
    }

    private static void runTest(String name, ThrowingTest test) {
        try {
            test.run();
            System.out.println(PASS + " " + name);
        } catch (SQLException | AssertionError e) {
            System.out.println(FAIL + " " + name + ": " + e.getMessage());
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
        if (expected == null ? actual != null : !expected.equals(actual)) {
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

    public void absoluteAndRelativeOutOfRangeBehaviors() throws SQLException {
        VeridicalResultSet rs = new VeridicalResultSet(
                null,
                rowDescription(),
                sampleRows(),
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        assertFalse(rs.absolute(0));
        assertTrue(rs.isBeforeFirst());

        assertFalse(rs.absolute(99));
        assertTrue(rs.isAfterLast());

        assertFalse(rs.relative(-99));
        assertTrue(rs.isBeforeFirst());
    }

    public void findColumnCaseInsensitiveAndMissingColumn() throws SQLException {
        VeridicalResultSet rs = new VeridicalResultSet(
                null,
                rowDescription(),
                sampleRows(),
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        assertEquals(1, rs.findColumn("ID"));
        assertEquals(2, rs.findColumn("nAmE"));

        try {
            rs.findColumn("missing");
            throw new AssertionError("Expected SQLException for missing column");
        } catch (SQLException e) {
            assertEquals("42S22", e.getSQLState());
        }
    }

    public void closedResultSetRejectsOperations() throws SQLException {
        VeridicalResultSet rs = new VeridicalResultSet(
                null,
                rowDescription(),
                sampleRows(),
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        rs.close();

        try {
            rs.next();
            throw new AssertionError("Expected SQLException from next() on closed ResultSet");
        } catch (SQLException e) {
            assertEquals("08003", e.getSQLState());
        }
    }

    public void wasNullTracksLastReadColumn() throws SQLException {
        List<Object[]> rowsWithNull = new ArrayList<>();
        rowsWithNull.add(new Object[] {1, null});

        VeridicalResultSet rs = new VeridicalResultSet(
                null,
                rowDescription(),
                rowsWithNull,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        assertTrue(rs.next());

        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.wasNull());

        String name = rs.getString("name");
        assertEquals(null, name);
        assertTrue(rs.wasNull());
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
