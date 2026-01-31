package com.veridicaldb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC PreparedStatement implementation for VeridicalDB.
 * Supports parameterized queries with type-safe parameter binding.
 */
public class VeridicalPreparedStatement extends VeridicalStatement implements PreparedStatement {
    
    private final String sql;
    private final Map<Integer, Parameter> parameters = new HashMap<>();
    
    public VeridicalPreparedStatement(VeridicalConnection connection, WireProtocol protocol, String sql) {
        super(connection, protocol);
        this.sql = sql;
    }
    
    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        validateParameters();
        
        try {
            // Use extended query protocol
            String stmtName = "";
            String portalName = "";
            
            // Parse
            protocol.sendParse(stmtName, sql, new int[0]);
            
            // Bind
            List<byte[]> paramValues = new ArrayList<>();
            for (int i = 1; i <= parameters.size(); i++) {
                Parameter param = parameters.get(i);
                paramValues.add(param != null ? param.value : null);
            }
            protocol.sendBind(portalName, stmtName, paramValues);
            
            // Describe
            protocol.sendDescribe('P', portalName);
            
            // Execute
            protocol.sendExecute(portalName, maxRows);
            
            // Sync
            protocol.sendSync();
            
            // Parse responses
            WireProtocol.RowDescription rowDesc = null;
            List<Object[]> rows = new ArrayList<>();
            
            while (true) {
                WireProtocol.Message msg = protocol.receiveMessage();
                
                switch (msg.type) {
                    case '1': // ParseComplete
                    case '2': // BindComplete
                        break;
                        
                    case WireProtocol.MessageType.ROW_DESCRIPTION:
                        rowDesc = parseRowDescription(msg);
                        break;
                        
                    case WireProtocol.MessageType.DATA_ROW:
                        if (rowDesc != null) {
                            Object[] row = parseDataRow(msg, rowDesc);
                            rows.add(row);
                        }
                        break;
                        
                    case WireProtocol.MessageType.COMMAND_COMPLETE:
                        break;
                        
                    case WireProtocol.MessageType.READY_FOR_QUERY:
                        currentResultSet = new VeridicalResultSet(this, rowDesc, rows);
                        updateCount = -1;
                        return currentResultSet;
                        
                    default:
                        // Ignore
                        break;
                }
            }
            
        } catch (IOException e) {
            throw new SQLException("Failed to execute prepared query", "08006", e);
        }
    }
    
    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        validateParameters();
        
        try {
            String stmtName = "";
            String portalName = "";
            
            protocol.sendParse(stmtName, sql, new int[0]);
            
            List<byte[]> paramValues = new ArrayList<>();
            for (int i = 1; i <= parameters.size(); i++) {
                Parameter param = parameters.get(i);
                paramValues.add(param != null ? param.value : null);
            }
            protocol.sendBind(portalName, stmtName, paramValues);
            protocol.sendExecute(portalName, 0);
            protocol.sendSync();
            
            int count = 0;
            while (true) {
                WireProtocol.Message msg = protocol.receiveMessage();
                
                switch (msg.type) {
                    case '1': // ParseComplete
                    case '2': // BindComplete
                        break;
                        
                    case WireProtocol.MessageType.COMMAND_COMPLETE:
                        String cmdStatus = msg.getString();
                        count = parseUpdateCount(cmdStatus);
                        break;
                        
                    case WireProtocol.MessageType.READY_FOR_QUERY:
                        updateCount = count;
                        currentResultSet = null;
                        return count;
                        
                    default:
                        break;
                }
            }
            
        } catch (IOException e) {
            throw new SQLException("Failed to execute prepared update", "08006", e);
        }
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(null, sqlType));
    }
    
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.BOOLEAN));
    }
    
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode((int) x), Types.TINYINT));
    }
    
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode((int) x), Types.SMALLINT));
    }
    
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.INTEGER));
    }
    
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.BIGINT));
    }
    
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.REAL));
    }
    
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.DOUBLE));
    }
    
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.DECIMAL));
    }
    
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.VARCHAR));
    }
    
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(x, Types.BINARY));
    }
    
    @Override
    public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.DATE));
    }
    
    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.TIME));
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, new Parameter(TypeMapper.encode(x), Types.TIMESTAMP));
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII stream not supported");
    }
    
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode stream not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary stream not supported");
    }
    
    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        parameters.clear();
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        checkClosed();
        
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
            return;
        }
        
        switch (targetSqlType) {
            case Types.BOOLEAN:
                setBoolean(parameterIndex, (Boolean) x);
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                setInt(parameterIndex, ((Number) x).intValue());
                break;
            case Types.BIGINT:
                setLong(parameterIndex, ((Number) x).longValue());
                break;
            case Types.REAL:
                setFloat(parameterIndex, ((Number) x).floatValue());
                break;
            case Types.DOUBLE:
                setDouble(parameterIndex, ((Number) x).doubleValue());
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                setBigDecimal(parameterIndex, (BigDecimal) x);
                break;
            case Types.VARCHAR:
            case Types.CHAR:
                setString(parameterIndex, x.toString());
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                setBytes(parameterIndex, (byte[]) x);
                break;
            case Types.DATE:
                setDate(parameterIndex, (java.sql.Date) x);
                break;
            case Types.TIME:
                setTime(parameterIndex, (Time) x);
                break;
            case Types.TIMESTAMP:
                setTimestamp(parameterIndex, (Timestamp) x);
                break;
            default:
                throw new SQLException("Unsupported SQL type: " + targetSqlType, "22003");
        }
    }
    
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkClosed();
        
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Byte || x instanceof Short || x instanceof Integer) {
            setInt(parameterIndex, ((Number) x).intValue());
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof java.sql.Date) {
            setDate(parameterIndex, (java.sql.Date) x);
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof Date) {
            setTimestamp(parameterIndex, new Timestamp(((Date) x).getTime()));
        } else {
            throw new SQLException("Unsupported object type: " + x.getClass().getName(), "22003");
        }
    }
    
    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        validateParameters();
        
        try {
            String stmtName = "";
            String portalName = "";
            
            protocol.sendParse(stmtName, sql, new int[0]);
            
            List<byte[]> paramValues = new ArrayList<>();
            for (int i = 1; i <= parameters.size(); i++) {
                Parameter param = parameters.get(i);
                paramValues.add(param != null ? param.value : null);
            }
            protocol.sendBind(portalName, stmtName, paramValues);
            protocol.sendDescribe('P', portalName);
            protocol.sendExecute(portalName, maxRows);
            protocol.sendSync();
            
            boolean hasResultSet = false;
            WireProtocol.RowDescription rowDesc = null;
            List<Object[]> rows = new ArrayList<>();
            int count = 0;
            
            while (true) {
                WireProtocol.Message msg = protocol.receiveMessage();
                
                switch (msg.type) {
                    case '1': // ParseComplete
                    case '2': // BindComplete
                        break;
                        
                    case WireProtocol.MessageType.ROW_DESCRIPTION:
                        rowDesc = parseRowDescription(msg);
                        hasResultSet = true;
                        break;
                        
                    case WireProtocol.MessageType.DATA_ROW:
                        if (rowDesc != null) {
                            Object[] row = parseDataRow(msg, rowDesc);
                            rows.add(row);
                        }
                        break;
                        
                    case WireProtocol.MessageType.COMMAND_COMPLETE:
                        String cmdStatus = msg.getString();
                        if (!hasResultSet) {
                            count = parseUpdateCount(cmdStatus);
                        }
                        break;
                        
                    case WireProtocol.MessageType.READY_FOR_QUERY:
                        if (hasResultSet) {
                            currentResultSet = new VeridicalResultSet(this, rowDesc, rows);
                            updateCount = -1;
                        } else {
                            currentResultSet = null;
                            updateCount = count;
                        }
                        return hasResultSet;
                        
                    default:
                        break;
                }
            }
            
        } catch (IOException e) {
            throw new SQLException("Failed to execute prepared statement", "08006", e);
        }
    }
    
    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        validateParameters();
        
        // Store parameters for batch execution
        // For simplicity, fall back to parent addBatch with parameterized SQL
        StringBuilder sb = new StringBuilder(sql);
        for (int i = 1; i <= parameters.size(); i++) {
            Parameter param = parameters.get(i);
            if (param != null && param.value != null) {
                String placeholder = "?";
                int idx = sb.indexOf(placeholder);
                if (idx >= 0) {
                    String valueStr = new String(param.value);
                    sb.replace(idx, idx + 1, "'" + valueStr.replace("'", "''") + "'");
                }
            }
        }
        super.addBatch(sb.toString());
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character stream not supported");
    }
    
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Prepared statement metadata not supported");
    }
    
    @Override
    public void setDate(int parameterIndex, java.sql.Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }
    
    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }
    
    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setString(parameterIndex, x.toString());
    }
    
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameter metadata not supported");
    }
    
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter stream not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII stream not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary stream not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character stream not supported");
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII stream not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary stream not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character stream not supported");
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter stream not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    /**
     * Validates that all required parameters are set.
     */
    private void validateParameters() throws SQLException {
        // Count number of '?' in SQL
        int expectedParams = 0;
        for (char c : sql.toCharArray()) {
            if (c == '?') expectedParams++;
        }
        
        if (parameters.size() < expectedParams) {
            throw new SQLException("Not all parameters are set. Expected: " + expectedParams + ", Got: " + parameters.size(), "07001");
        }
    }
    
    /**
     * Represents a parameter value and its SQL type.
     */
    private static class Parameter {
        final byte[] value;
        @SuppressWarnings("unused")
        final int sqlType;
        
        Parameter(byte[] value, int sqlType) {
            this.value = value;
            this.sqlType = sqlType;
        }
    }
}
