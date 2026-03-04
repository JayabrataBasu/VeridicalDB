package com.veridicaldb.jdbc;

import java.io.DataInputStream;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC Statement implementation for VeridicalDB.
 * Executes SQL statements and returns result sets.
 */
public class VeridicalStatement implements Statement {
    
    protected final VeridicalConnection connection;
    protected final WireProtocol protocol;
    protected final int resultSetType;
    protected final int resultSetConcurrency;
    protected boolean closed = false;
    protected int fetchSize = 0;
    protected int maxRows = 0;
    protected int maxFieldSize = 0;
    protected int queryTimeout = 0;
    protected ResultSet currentResultSet = null;
    protected int updateCount = -1;
    protected SQLWarning warnings = null;
    protected List<String> batchCommands = new ArrayList<>();
    
    public VeridicalStatement(VeridicalConnection connection, WireProtocol protocol) {
        this(connection, protocol, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public VeridicalStatement(VeridicalConnection connection, WireProtocol protocol, int resultSetType, int resultSetConcurrency) {
        this.connection = connection;
        this.protocol = protocol;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
    }
    
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        
        try {
            // Send query
            protocol.sendSimpleQuery(sql);
            
            // Parse response
            WireProtocol.RowDescription rowDesc = null;
            List<Object[]> rows = new ArrayList<>();
            
            while (true) {
                WireProtocol.Message msg = protocol.receiveMessage();
                
                switch (msg.type) {
                    case WireProtocol.MessageType.ROW_DESCRIPTION:
                        rowDesc = parseRowDescription(msg);
                        break;
                        
                    case WireProtocol.MessageType.DATA_ROW:
                        Object[] row = parseDataRow(msg, rowDesc);
                        rows.add(row);
                        if (maxRows > 0 && rows.size() >= maxRows) {
                            // Skip remaining rows
                            while (protocol.receiveMessage().type != WireProtocol.MessageType.COMMAND_COMPLETE) {
                                // Consume
                            }
                        }
                        break;
                        
                    case WireProtocol.MessageType.COMMAND_COMPLETE:
                        // Query complete
                        break;
                        
                    case WireProtocol.MessageType.READY_FOR_QUERY:
                        // Transaction state - done
                        currentResultSet = new VeridicalResultSet(this, rowDesc, rows, resultSetType, resultSetConcurrency);
                        updateCount = -1;
                        return currentResultSet;
                        
                    case WireProtocol.MessageType.EMPTY_QUERY_RESPONSE:
                        throw new SQLException("Empty query", "42601");
                        
                    default:
                        // Ignore other messages
                        break;
                }
            }
            
        } catch (IOException e) {
            throw new SQLException("Failed to execute query", "08006", e);
        }
    }
    
    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        
        try {
            protocol.sendSimpleQuery(sql);
            
            int count = 0;
            while (true) {
                WireProtocol.Message msg = protocol.receiveMessage();
                
                switch (msg.type) {
                    case WireProtocol.MessageType.COMMAND_COMPLETE:
                        String cmdStatus = msg.getString();
                        count = parseUpdateCount(cmdStatus);
                        break;
                        
                    case WireProtocol.MessageType.READY_FOR_QUERY:
                        updateCount = count;
                        currentResultSet = null;
                        return count;
                        
                    case WireProtocol.MessageType.ROW_DESCRIPTION:
                    case WireProtocol.MessageType.DATA_ROW:
                        throw new SQLException("Query returned a result set, use executeQuery() instead", "0A000");
                        
                    default:
                        // Ignore other messages
                        break;
                }
            }
            
        } catch (IOException e) {
            throw new SQLException("Failed to execute update", "08006", e);
        }
    }
    
    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        
        closed = true;
    }
    
    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return maxFieldSize;
    }
    
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        if (max < 0) {
            throw new SQLException("Max field size must be >= 0", "22003");
        }
        this.maxFieldSize = max;
    }
    
    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }
    
    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        if (max < 0) {
            throw new SQLException("Max rows must be >= 0", "22003");
        }
        this.maxRows = max;
    }
    
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        // No-op - escape processing not needed
    }
    
    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }
    
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        if (seconds < 0) {
            throw new SQLException("Query timeout must be >= 0", "22003");
        }
        this.queryTimeout = seconds;
    }
    
    @Override
    public void cancel() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Statement cancellation not supported");
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return warnings;
    }
    
    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        warnings = null;
    }
    
    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Named cursors not supported");
    }
    
    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        
        try {
            protocol.sendSimpleQuery(sql);
            
            boolean hasResultSet = false;
            WireProtocol.RowDescription rowDesc = null;
            List<Object[]> rows = new ArrayList<>();
            int count = 0;
            
            while (true) {
                WireProtocol.Message msg = protocol.receiveMessage();
                
                switch (msg.type) {
                    case WireProtocol.MessageType.ROW_DESCRIPTION:
                        rowDesc = parseRowDescription(msg);
                        hasResultSet = true;
                        break;
                        
                    case WireProtocol.MessageType.DATA_ROW:
                        Object[] row = parseDataRow(msg, rowDesc);
                        rows.add(row);
                        break;
                        
                    case WireProtocol.MessageType.COMMAND_COMPLETE:
                        String cmdStatus = msg.getString();
                        if (!hasResultSet) {
                            count = parseUpdateCount(cmdStatus);
                        }
                        break;
                        
                    case WireProtocol.MessageType.READY_FOR_QUERY:
                        if (hasResultSet) {
                            currentResultSet = new VeridicalResultSet(this, rowDesc, rows, resultSetType, resultSetConcurrency);
                            updateCount = -1;
                        } else {
                            currentResultSet = null;
                            updateCount = count;
                        }
                        return hasResultSet;
                        
                    default:
                        // Ignore other messages
                        break;
                }
            }
            
        } catch (IOException e) {
            throw new SQLException("Failed to execute statement", "08006", e);
        }
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currentResultSet;
    }
    
    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }
    
    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        
        currentResultSet = null;
        updateCount = -1;
        return false; // Only one result set per statement
    }
    
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_REVERSE && direction != ResultSet.FETCH_UNKNOWN) {
            throw new SQLFeatureNotSupportedException("Unsupported fetch direction");
        }
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        if (rows < 0) {
            throw new SQLException("Fetch size must be >= 0", "22003");
        }
        this.fetchSize = rows;
    }
    
    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }
    
    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return resultSetConcurrency;
    }
    
    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return resultSetType;
    }
    
    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batchCommands.add(sql);
    }
    
    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batchCommands.clear();
    }
    
    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        
        int[] results = new int[batchCommands.size()];
        List<SQLException> exceptions = new ArrayList<>();
        
        for (int i = 0; i < batchCommands.size(); i++) {
            try {
                results[i] = executeUpdate(batchCommands.get(i));
            } catch (SQLException e) {
                results[i] = EXECUTE_FAILED;
                exceptions.add(e);
            }
        }
        
        batchCommands.clear();
        
        if (!exceptions.isEmpty()) {
            throw new BatchUpdateException("Batch execution failed", results);
        }
        
        return results;
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }
    
    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return getMoreResults();
    }
    
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys not supported");
    }
    
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }
    
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }
    
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }
    
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }
    
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }
    
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }
    
    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        // Ignored
    }
    
    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }
    
    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException("Close on completion not supported");
    }
    
    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
    
    /**
     * Parses row description message.
     */
    protected WireProtocol.RowDescription parseRowDescription(WireProtocol.Message msg) throws IOException {
        DataInputStream in = msg.getDataInputStream();
        short columnCount = in.readShort();
        
        WireProtocol.RowDescription rowDesc = new WireProtocol.RowDescription();
        
        for (int i = 0; i < columnCount; i++) {
            String name = readCString(in);
            int tableOid = in.readInt();
            short columnNumber = in.readShort();
            int typeOid = in.readInt();
            short typeSize = in.readShort();
            int typeMod = in.readInt();
            short formatCode = in.readShort();
            
            rowDesc.addColumn(name, tableOid, columnNumber, typeOid, typeSize, typeMod, formatCode);
        }
        
        return rowDesc;
    }
    
    /**
     * Parses data row message.
     */
    protected Object[] parseDataRow(WireProtocol.Message msg, WireProtocol.RowDescription rowDesc) throws IOException, SQLException {
        DataInputStream in = msg.getDataInputStream();
        short columnCount = in.readShort();
        
        Object[] row = new Object[columnCount];
        
        for (int i = 0; i < columnCount; i++) {
            int length = in.readInt();
            
            if (length == -1) {
                row[i] = null;
            } else {
                byte[] data = new byte[length];
                in.readFully(data);

                if (rowDesc != null && i < rowDesc.getColumnCount()) {
                    // Convert to typed Java value when metadata is available.
                    WireProtocol.ColumnInfo col = rowDesc.getColumn(i);
                    row[i] = TypeMapper.decode(col.typeOid, data);
                } else {
                    // Metadata fallback: treat column data as text.
                    row[i] = new String(data);
                }
            }
        }
        
        return row;
    }
    
    /**
     * Parses update count from command status.
     */
    protected int parseUpdateCount(String status) {
        // Status format: "INSERT oid rows", "UPDATE rows", "DELETE rows", "SELECT rows"
        String[] parts = status.split(" ");
        
        if (parts.length >= 2) {
            String lastPart = parts[parts.length - 1];
            try {
                return Integer.parseInt(lastPart);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        
        return 0;
    }
    
    /**
     * Reads null-terminated string from input stream.
     */
    private String readCString(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.readByte()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }
    
    /**
     * Checks if statement is closed and throws SQLException if it is.
     */
    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed", "08003");
        }
    }
}
