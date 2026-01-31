package com.veridicaldb.jdbc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * JDBC Connection implementation for VeridicalDB.
 * Manages a single connection to the database server using PostgreSQL wire protocol.
 */
public class VeridicalConnection implements Connection {
    
    private final ConnectionProperties props;
    private final Socket socket;
    private final WireProtocol protocol;
    private boolean closed = false;
    private boolean autoCommit = true;
    private boolean readOnly = false;
    private int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
    private String catalog = null;
    private int networkTimeout = 0;
    
    /**
     * Creates a new connection to VeridicalDB server.
     *
     * @param props Connection properties
     * @throws SQLException if connection fails
     */
    public VeridicalConnection(ConnectionProperties props) throws SQLException {
        this.props = props;
        
        try {
            // Create TCP socket
            this.socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            
            if (props.getSocketTimeout() > 0) {
                socket.setSoTimeout(props.getSocketTimeout() * 1000);
            }
            
            // Connect to server
            InetSocketAddress address = new InetSocketAddress(props.getHost(), props.getPort());
            socket.connect(address, props.getConnectTimeout() * 1000);
            
            // Initialize wire protocol
            this.protocol = new WireProtocol(socket.getInputStream(), socket.getOutputStream());
            
            // Authenticate
            authenticate();
            
            // Set initial auto-commit mode
            setAutoCommit(props.isAutoCommit());
            
        } catch (IOException e) {
            throw new SQLException("Failed to connect to " + props.getHost() + ":" + props.getPort(), "08001", e);
        }
    }
    
    /**
     * Performs authentication handshake with server.
     */
    private void authenticate() throws SQLException, IOException {
        // Send startup message
        protocol.sendStartup(props.getDatabase(), props.getUser());
        
        // Handle authentication exchange
        WireProtocol.Message authMsg = protocol.receiveMessage();
        
        if (authMsg.type == 'R') { // Authentication request
            int authType = authMsg.getInt();
            
            if (authType == 0) {
                // Auth OK
                return;
            } else if (authType == 3) {
                // Clear text password required
                protocol.sendPassword(props.getPassword());
                
                // Wait for auth response
                WireProtocol.Message authResponse = protocol.receiveMessage();
                if (authResponse.type != 'R' || authResponse.getInt() != 0) {
                    throw new SQLException("Authentication failed", "28P01");
                }
            } else {
                throw new SQLException("Unsupported authentication type: " + authType, "28000");
            }
        }
        
        // Read ready for query message
        protocol.waitForReady();
    }
    
    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new VeridicalStatement(this, protocol);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new VeridicalPreparedStatement(this, protocol, sql);
    }
    
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements not supported");
    }
    
    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return sql; // No translation needed
    }
    
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.autoCommit != autoCommit) {
            this.autoCommit = autoCommit;
            // Server tracks auto-commit state; no explicit message needed initially
        }
    }
    
    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }
    
    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot commit when auto-commit is enabled", "25P01");
        }
        
        try {
            protocol.sendSimpleQuery("COMMIT");
            protocol.waitForReady();
        } catch (IOException e) {
            throw new SQLException("Failed to commit transaction", "08006", e);
        }
    }
    
    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot rollback when auto-commit is enabled", "25P01");
        }
        
        try {
            protocol.sendSimpleQuery("ROLLBACK");
            protocol.waitForReady();
        } catch (IOException e) {
            throw new SQLException("Failed to rollback transaction", "08006", e);
        }
    }
    
    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        
        try {
            // Send terminate message
            protocol.sendTerminate();
            socket.close();
        } catch (IOException e) {
            throw new SQLException("Error closing connection", "08006", e);
        } finally {
            closed = true;
        }
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new VeridicalDatabaseMetaData(this);
    }
    
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        this.readOnly = readOnly;
    }
    
    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return readOnly;
    }
    
    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        this.catalog = catalog;
    }
    
    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog;
    }
    
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        if (level != Connection.TRANSACTION_READ_UNCOMMITTED &&
            level != Connection.TRANSACTION_READ_COMMITTED &&
            level != Connection.TRANSACTION_REPEATABLE_READ &&
            level != Connection.TRANSACTION_SERIALIZABLE) {
            throw new SQLException("Invalid transaction isolation level: " + level, "22023");
        }
        this.transactionIsolation = level;
    }
    
    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null; // No warnings tracked yet
    }
    
    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        // No warnings to clear
    }
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY supported");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Only CONCUR_READ_ONLY supported");
        }
        return createStatement();
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY supported");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Only CONCUR_READ_ONLY supported");
        }
        return prepareStatement(sql);
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements not supported");
    }
    
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type maps not supported");
    }
    
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type maps not supported");
    }
    
    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        // Ignored - holdability not implemented
    }
    
    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }
    
    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements not supported");
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout must be >= 0", "22003");
        }
        
        if (closed || socket.isClosed()) {
            return false;
        }
        
        try {
            // Simple query to test connection
            protocol.sendSimpleQuery("SELECT 1");
            protocol.waitForReady();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // Ignored
    }
    
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // Ignored
    }
    
    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }
    
    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }
    
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Arrays not supported");
    }
    
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Structs not supported");
    }
    
    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        // Schema not implemented
    }
    
    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return null;
    }
    
    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }
    
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkClosed();
        if (milliseconds < 0) {
            throw new SQLException("Network timeout must be >= 0", "22003");
        }
        
        try {
            socket.setSoTimeout(milliseconds);
            this.networkTimeout = milliseconds;
        } catch (IOException e) {
            throw new SQLException("Failed to set network timeout", "08006", e);
        }
    }
    
    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        return networkTimeout;
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
     * Checks if connection is closed and throws SQLException if it is.
     */
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed", "08003");
        }
    }
    
    /**
     * Returns the connection properties.
     */
    public ConnectionProperties getProperties() {
        return props;
    }
    
    /**
     * Returns the wire protocol instance (package-private for internal use).
     */
    @SuppressWarnings("unused")
    WireProtocol getProtocol() {
        return protocol;
    }
}
