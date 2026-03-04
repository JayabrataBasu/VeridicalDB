package com.veridicaldb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC ResultSet implementation for VeridicalDB.
 * Provides forward-only/scroll-insensitive and read-only/updatable result set functionality.
 */
public class VeridicalResultSet implements ResultSet {
    
    private final Statement statement;
    private final WireProtocol.RowDescription rowDesc;
    private final List<Object[]> rows;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private int currentRow = -1; // Before first row
    private boolean closed = false;
    private boolean wasNull = false;
    private final Map<Integer, Object> pendingUpdates = new HashMap<>();
    private boolean updatedCurrentRow = false;
    
    public VeridicalResultSet(Statement statement, WireProtocol.RowDescription rowDesc, List<Object[]> rows) {
        this(statement, rowDesc, rows, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public VeridicalResultSet(
            Statement statement,
            WireProtocol.RowDescription rowDesc,
            List<Object[]> rows,
            int resultSetType,
            int resultSetConcurrency) {
        this.statement = statement;
        this.rowDesc = rowDesc;
        this.rows = rows;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
    }
    
    @Override
    public boolean next() throws SQLException {
        checkClosed();
        pendingUpdates.clear();
        updatedCurrentRow = false;
        currentRow++;
        return currentRow < rows.size();
    }
    
    @Override
    public void close() throws SQLException {
        closed = true;
    }
    
    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }
    
    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return null;
        return value.toString();
    }
    
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        return Boolean.parseBoolean(value.toString());
    }
    
    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).byteValue();
        return Byte.parseByte(value.toString());
    }
    
    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).shortValue();
        return Short.parseShort(value.toString());
    }
    
    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).intValue();
        return Integer.parseInt(value.toString());
    }
    
    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return 0L;
        if (value instanceof Number) return ((Number) value).longValue();
        return Long.parseLong(value.toString());
    }
    
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return 0.0f;
        if (value instanceof Number) return ((Number) value).floatValue();
        return Float.parseFloat(value.toString());
    }
    
    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return 0.0;
        if (value instanceof Number) return ((Number) value).doubleValue();
        return Double.parseDouble(value.toString());
    }
    
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex);
    }
    
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return null;
        if (value instanceof byte[]) return (byte[]) value;
        return value.toString().getBytes();
    }
    
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return null;
        if (value instanceof Date) return (Date) value;
        if (value instanceof Timestamp) return new Date(((Timestamp) value).getTime());
        return Date.valueOf(value.toString());
    }
    
    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return null;
        if (value instanceof Time) return (Time) value;
        if (value instanceof Timestamp) return new Time(((Timestamp) value).getTime());
        return Time.valueOf(value.toString());
    }
    
    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return null;
        if (value instanceof Timestamp) return (Timestamp) value;
        if (value instanceof Date) return new Timestamp(((Date) value).getTime());
        return Timestamp.valueOf(value.toString());
    }
    
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII stream not supported");
    }
    
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode stream not supported");
    }
    
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary stream not supported");
    }
    
    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }
    
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }
    
    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }
    
    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }
    
    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }
    
    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }
    
    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }
    
    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }
    
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }
    
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }
    
    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }
    
    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }
    
    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }
    
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII stream not supported");
    }
    
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode stream not supported");
    }
    
    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary stream not supported");
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }
    
    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }
    
    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursors not supported");
    }
    
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new VeridicalResultSetMetaData(rowDesc);
    }
    
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValue(columnIndex);
    }
    
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }
    
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        
        for (int i = 0; i < rowDesc.getColumnCount(); i++) {
            if (rowDesc.getColumn(i).name.equalsIgnoreCase(columnLabel)) {
                return i + 1; // JDBC is 1-indexed
            }
        }
        
        throw new SQLException("Column not found: " + columnLabel, "42S22");
    }
    
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character stream not supported");
    }
    
    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character stream not supported");
    }
    
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return null;
        if (value instanceof BigDecimal) return (BigDecimal) value;
        if (value instanceof Number) return BigDecimal.valueOf(((Number) value).doubleValue());
        return new BigDecimal(value.toString());
    }
    
    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }
    
    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return currentRow == -1 && !rows.isEmpty();
    }
    
    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return currentRow >= rows.size() && !rows.isEmpty();
    }
    
    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return currentRow == 0;
    }
    
    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return currentRow == rows.size() - 1 && !rows.isEmpty();
    }
    
    @Override
    public void beforeFirst() throws SQLException {
        checkScrollable();
        pendingUpdates.clear();
        updatedCurrentRow = false;
        currentRow = -1;
    }
    
    @Override
    public void afterLast() throws SQLException {
        checkScrollable();
        pendingUpdates.clear();
        updatedCurrentRow = false;
        currentRow = rows.size();
    }
    
    @Override
    public boolean first() throws SQLException {
        checkScrollable();
        pendingUpdates.clear();
        updatedCurrentRow = false;
        if (rows.isEmpty()) {
            currentRow = rows.size();
            return false;
        }
        currentRow = 0;
        return true;
    }
    
    @Override
    public boolean last() throws SQLException {
        checkScrollable();
        pendingUpdates.clear();
        updatedCurrentRow = false;
        if (rows.isEmpty()) {
            currentRow = rows.size();
            return false;
        }
        currentRow = rows.size() - 1;
        return true;
    }
    
    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return currentRow + 1; // JDBC is 1-indexed
    }
    
    @Override
    public boolean absolute(int row) throws SQLException {
        checkScrollable();
        pendingUpdates.clear();
        updatedCurrentRow = false;
        if (row == 0) {
            currentRow = -1;
            return false;
        }

        int target = row > 0 ? row - 1 : rows.size() + row;
        if (target < 0) {
            currentRow = -1;
            return false;
        }
        if (target >= rows.size()) {
            currentRow = rows.size();
            return false;
        }

        currentRow = target;
        return true;
    }
    
    @Override
    public boolean relative(int offset) throws SQLException {
        checkScrollable();
        pendingUpdates.clear();
        updatedCurrentRow = false;
        int target = currentRow + offset;
        if (target < 0) {
            currentRow = -1;
            return false;
        }
        if (target >= this.rows.size()) {
            currentRow = this.rows.size();
            return false;
        }

        currentRow = target;
        return true;
    }
    
    @Override
    public boolean previous() throws SQLException {
        checkScrollable();
        pendingUpdates.clear();
        updatedCurrentRow = false;
        if (currentRow <= 0) {
            currentRow = -1;
            return false;
        }
        currentRow--;
        return true;
    }
    
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD supported");
        }
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return FETCH_FORWARD;
    }
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        // Ignored - all rows are pre-fetched
    }
    
    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }
    
    @Override
    public int getType() throws SQLException {
        checkClosed();
        return resultSetType;
    }
    
    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return resultSetConcurrency;
    }
    
    @Override
    public boolean rowUpdated() throws SQLException {
        checkClosed();
        return updatedCurrentRow;
    }
    
    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }
    
    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }
    
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        stageUpdate(columnIndex, null);
    }
    
    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        stageUpdate(columnIndex, x);
    }
    
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }
    
    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x, scaleOrLength);
    }
    
    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }
    
    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateRow() throws SQLException {
        checkUpdatable();
        ensureValidCurrentRow();
        if (pendingUpdates.isEmpty()) {
            return;
        }

        Object[] row = rows.get(currentRow);
        for (Map.Entry<Integer, Object> entry : pendingUpdates.entrySet()) {
            row[entry.getKey() - 1] = entry.getValue();
        }
        pendingUpdates.clear();
        updatedCurrentRow = true;
    }
    
    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void refreshRow() throws SQLException {
        checkUpdatable();
        pendingUpdates.clear();
        updatedCurrentRow = false;
    }
    
    @Override
    public void cancelRowUpdates() throws SQLException {
        checkUpdatable();
        pendingUpdates.clear();
        updatedCurrentRow = false;
    }
    
    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }
    
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }
    
    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }
    
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }
    
    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }
    
    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }
    
    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }
    
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }
    
    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }
    
    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }
    
    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }
    
    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }
    
    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public URL getURL(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (value == null) return null;
        try {
            return new URL(value);
        } catch (Exception e) {
            throw new SQLException("Invalid URL: " + value, "22000", e);
        }
    }
    
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }
    
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return HOLD_CURSORS_OVER_COMMIT;
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }
    
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }
    
    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }
    
    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter stream not supported");
    }
    
    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter stream not supported");
    }
    
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("ResultSet is not updateable");
    }
    
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) return null;
        
        if (type.isAssignableFrom(value.getClass())) {
            return type.cast(value);
        }
        
        throw new SQLException("Cannot convert " + value.getClass() + " to " + type, "22000");
    }
    
    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
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

    private void checkScrollable() throws SQLException {
        checkClosed();
        if (resultSetType == TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("ResultSet is TYPE_FORWARD_ONLY");
        }
    }

    private void checkUpdatable() throws SQLException {
        checkClosed();
        if (resultSetConcurrency != CONCUR_UPDATABLE) {
            throw new SQLFeatureNotSupportedException("ResultSet is CONCUR_READ_ONLY");
        }
    }

    private void ensureValidCurrentRow() throws SQLException {
        if (currentRow < 0 || currentRow >= rows.size()) {
            throw new SQLException("No current row", "24000");
        }
    }

    private void validateColumnIndex(int columnIndex) throws SQLException {
        int columnCount = rowDesc != null
                ? rowDesc.getColumnCount()
                : (rows.isEmpty() ? 0 : rows.get(0).length);
        if (columnIndex < 1 || columnIndex > columnCount) {
            throw new SQLException("Invalid column index: " + columnIndex, "42S22");
        }
    }

    private void stageUpdate(int columnIndex, Object value) throws SQLException {
        checkUpdatable();
        ensureValidCurrentRow();
        validateColumnIndex(columnIndex);
        pendingUpdates.put(columnIndex, value);
    }
    
    /**
     * Gets value from current row at given column index (1-based).
     */
    private Object getValue(int columnIndex) throws SQLException {
        checkClosed();

        ensureValidCurrentRow();
        validateColumnIndex(columnIndex);
        
        Object value = rows.get(currentRow)[columnIndex - 1];
        wasNull = (value == null);
        return value;
    }
    
    /**
     * Checks if result set is closed and throws SQLException if it is.
     */
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed", "08003");
        }
    }
}
