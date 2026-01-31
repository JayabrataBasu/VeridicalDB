package com.veridicaldb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * JDBC ResultSetMetaData implementation for VeridicalDB.
 * Provides metadata about columns in a result set.
 * and if it goes wrong it will yeet the mother of all exceptions
 * isAutoIncrement is not supported yet, so it always returns false. Fair few things are not supported yet.
 * please find a comment attached next to them
 */
public class VeridicalResultSetMetaData implements ResultSetMetaData {
    
    private final WireProtocol.RowDescription rowDesc;
    
    public VeridicalResultSetMetaData(WireProtocol.RowDescription rowDesc) {
        this.rowDesc = rowDesc;
    }
    
    @Override
    public int getColumnCount() throws SQLException {
        return rowDesc.getColumnCount();
    }
    
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false; // Not supported yet
    }
    
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.isCaseSensitive(col.typeOid);
    }
    
    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }
    
    @Override
    public boolean isCurrency(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.isCurrency(col.typeOid);
    }
    
    @Override
    public int isNullable(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.getNullable(col.typeOid);
    }
    
    @Override
    public boolean isSigned(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.isSigned(col.typeOid);
    }
    
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.getDisplaySize(col.typeOid);
    }
    
    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumn(column).name;
    }
    
    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).name;
    }
    
    @Override
    public String getSchemaName(int column) throws SQLException {
        return ""; // Schema not implemented
    }
    
    @Override
    public int getPrecision(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.getPrecision(col.typeOid);
    }
    
    @Override
    public int getScale(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.getScale(col.typeOid);
    }
    
    @Override
    public String getTableName(int column) throws SQLException {
        return ""; // Table name not available in protocol
    }
    
    @Override
    public String getCatalogName(int column) throws SQLException {
        return ""; // Catalog not implemented
    }
    
    @Override
    public int getColumnType(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.oidToSqlType(col.typeOid);
    }
    
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.oidToTypeName(col.typeOid);
    }
    
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true; // All columns are read-only (CONCUR_READ_ONLY)
    }
    
    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }
    
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }
    
    @Override
    public String getColumnClassName(int column) throws SQLException {
        WireProtocol.ColumnInfo col = getColumn(column);
        return TypeMapper.oidToClassName(col.typeOid);
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
     * Gets column info for given column index (1-based).
     */
    private WireProtocol.ColumnInfo getColumn(int column) throws SQLException {
        if (column < 1 || column > rowDesc.getColumnCount()) {
            throw new SQLException("Invalid column index: " + column, "42S22");
        }
        return rowDesc.getColumn(column - 1);
    }
}
