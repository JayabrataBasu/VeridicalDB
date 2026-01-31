package com.veridicaldb.jdbc;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Maps between JDBC types and VeridicalDB wire protocol types.
 * Handles encoding Java objects to wire format and decoding wire format to Java objects.
 */
public class TypeMapper {
    
    // PostgreSQL OIDs (Object Identifiers) for common types, i copied this from PG docs    
    public static final int PG_BOOL = 16;
    public static final int PG_BYTEA = 17;
    public static final int PG_INT8 = 20;
    public static final int PG_INT2 = 21;
    public static final int PG_INT4 = 23;
    public static final int PG_TEXT = 25;
    public static final int PG_FLOAT4 = 700;
    public static final int PG_FLOAT8 = 701;
    public static final int PG_VARCHAR = 1043;
    public static final int PG_DATE = 1082;
    public static final int PG_TIME = 1083;
    public static final int PG_TIMESTAMP = 1114;
    public static final int PG_NUMERIC = 1700;
    
    /**
     * Encodes a boolean value to wire format.
     */
    public static byte[] encode(boolean value) {
        return (value ? "t" : "f").getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes an integer value to wire format.
     */
    public static byte[] encode(int value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes a long value to wire format.
     */
    public static byte[] encode(long value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes a float value to wire format.
     */
    public static byte[] encode(float value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes a double value to wire format.
     */
    public static byte[] encode(double value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes a BigDecimal value to wire format.
     */
    public static byte[] encode(BigDecimal value) {
        if (value == null) return null;
        return value.toPlainString().getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes a String value to wire format.
     */
    public static byte[] encode(String value) {
        if (value == null) return null;
        return value.getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes a Date value to wire format.
     */
    public static byte[] encode(Date value) {
        if (value == null) return null;
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
        return fmt.format(value).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes a Time value to wire format.
     */
    public static byte[] encode(Time value) {
        if (value == null) return null;
        SimpleDateFormat fmt = new SimpleDateFormat("HH:mm:ss");
        return fmt.format(value).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes a Timestamp value to wire format.
     */
    public static byte[] encode(Timestamp value) {
        if (value == null) return null;
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return fmt.format(value).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Decodes wire format bytes to appropriate Java object based on PostgreSQL OID.
     */
    public static Object decode(int oid, byte[] data) throws SQLException {
        if (data == null) {
            return null;
        }
        
        String text = new String(data, StandardCharsets.UTF_8);
        
        //i will change these innecessary temps, most likely by removing them
        try {
            switch (oid) {
                case PG_BOOL:
                    return "t".equals(text) || "true".equalsIgnoreCase(text);
                    
                case PG_INT2:
                    return Short.parseShort(text);
                    
                case PG_INT4:
                    return Integer.parseInt(text);
                    
                case PG_INT8:
                    return Long.parseLong(text);
                    
                case PG_FLOAT4:
                    return Float.parseFloat(text);
                    
                case PG_FLOAT8:
                    return Double.parseDouble(text);
                    
                case PG_NUMERIC:
                    return new BigDecimal(text);
                    
                case PG_TEXT:
                case PG_VARCHAR:
                    return text;
                    
                case PG_BYTEA:
                    return data; // Binary data, this is rather strange to me, but learn not too question much
                    
                case PG_DATE:
                    try {
                        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
                        return new Date(fmt.parse(text).getTime());
                    } catch (ParseException e) {
                        throw new SQLException("Invalid date format: " + text, "22007", e);
                    }
                    
                case PG_TIME:
                    try {
                        SimpleDateFormat fmt = new SimpleDateFormat("HH:mm:ss");
                        return new Time(fmt.parse(text).getTime());
                    } catch (ParseException e) {
                        throw new SQLException("Invalid time format: " + text, "22007", e);
                    }
                    
                case PG_TIMESTAMP:
                    try {
                        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        return new Timestamp(fmt.parse(text).getTime());
                    } catch (ParseException e) {
                        throw new SQLException("Invalid timestamp format: " + text, "22007", e);
                    }
                    
                default:
                    // Unknown type - return as string
                    return text;
            }
        } catch (NumberFormatException e) {
            throw new SQLException("Invalid number format: " + text, "22003", e);
        }
    }
    
    /**
     * Maps PostgreSQL OID to JDBC SQL type.
     */
    public static int oidToSqlType(int oid) {
        switch (oid) {
            case PG_BOOL:
                return Types.BOOLEAN;
            case PG_INT2:
                return Types.SMALLINT;
            case PG_INT4:
                return Types.INTEGER;
            case PG_INT8:
                return Types.BIGINT;
            case PG_FLOAT4:
                return Types.REAL;
            case PG_FLOAT8:
                return Types.DOUBLE;
            case PG_NUMERIC:
                return Types.DECIMAL;
            case PG_TEXT:
            case PG_VARCHAR:
                return Types.VARCHAR;
            case PG_BYTEA:
                return Types.BINARY;
            case PG_DATE:
                return Types.DATE;
            case PG_TIME:
                return Types.TIME;
            case PG_TIMESTAMP:
                return Types.TIMESTAMP;
            default:
                return Types.OTHER;
        }
    }
    
    /**
     * Maps PostgreSQL OID to Java class name.
     */
    public static String oidToClassName(int oid) {
        switch (oid) {
            case PG_BOOL:
                return Boolean.class.getName();
            case PG_INT2:
                return Short.class.getName();
            case PG_INT4:
                return Integer.class.getName();
            case PG_INT8:
                return Long.class.getName();
            case PG_FLOAT4:
                return Float.class.getName();
            case PG_FLOAT8:
                return Double.class.getName();
            case PG_NUMERIC:
                return BigDecimal.class.getName();
            case PG_TEXT:
            case PG_VARCHAR:
                return String.class.getName();
            case PG_BYTEA:
                return byte[].class.getName();
            case PG_DATE:
                return Date.class.getName();
            case PG_TIME:
                return Time.class.getName();
            case PG_TIMESTAMP:
                return Timestamp.class.getName();
            default:
                return Object.class.getName();
        }
    }
    
    /**
     * Maps PostgreSQL OID to type name.
     */
    public static String oidToTypeName(int oid) {
        switch (oid) {
            case PG_BOOL:
                return "BOOLEAN";
            case PG_INT2:
                return "SMALLINT";
            case PG_INT4:
                return "INTEGER";
            case PG_INT8:
                return "BIGINT";
            case PG_FLOAT4:
                return "REAL";
            case PG_FLOAT8:
                return "DOUBLE PRECISION";
            case PG_NUMERIC:
                return "NUMERIC";
            case PG_TEXT:
                return "TEXT";
            case PG_VARCHAR:
                return "VARCHAR";
            case PG_BYTEA:
                return "BYTEA";
            case PG_DATE:
                return "DATE";
            case PG_TIME:
                return "TIME";
            case PG_TIMESTAMP:
                return "TIMESTAMP";
            default:
                return "UNKNOWN";
        }
    }
    
    /**
     * Gets precision for a given OID.
     */
    public static int getPrecision(int oid) {
        switch (oid) {
            case PG_BOOL:
                return 1;
            case PG_INT2:
                return 5;
            case PG_INT4:
                return 10;
            case PG_INT8:
                return 19;
            case PG_FLOAT4:
                return 7;
            case PG_FLOAT8:
                return 15;
            case PG_NUMERIC:
                return 131089; // Max precision
            case PG_TEXT:
            case PG_VARCHAR:
                return Integer.MAX_VALUE;
            case PG_DATE:
                return 13;
            case PG_TIME:
                return 15;
            case PG_TIMESTAMP:
                return 29;
            default:
                return 0;
        }
    }
    
    /**
     * Gets scale for a given OID (decimal places).
     */
    public static int getScale(int oid) {
        switch (oid) {
            case PG_NUMERIC:
            case PG_FLOAT4:
            case PG_FLOAT8:
                return 10; // Default scale
            default:
                return 0;
        }
    }
    
    /**
     * Checks if type is nullable.
     */
    public static int getNullable(int oid) {
        return ResultSetMetaData.columnNullableUnknown;
    }
    
    /**
     * Checks if type is signed.
     */
    public static boolean isSigned(int oid) {
        switch (oid) {
            case PG_INT2:
            case PG_INT4:
            case PG_INT8:
            case PG_FLOAT4:
            case PG_FLOAT8:
            case PG_NUMERIC:
                return true;
            default:
                return false;
        }
    }
    
    /**
     * Checks if type is case sensitive.
     */
    public static boolean isCaseSensitive(int oid) {
        switch (oid) {
            case PG_TEXT:
            case PG_VARCHAR:
                return true;
            default:
                return false;
        }
    }
    
    /**
     * Checks if type is searchable.
     */
    public static int isSearchable(int oid) {
        return 3; // ResultSetMetaData.columnSearchable (value is 3)
    }
    
    /**
     * Checks if type is currency.
     */
    public static boolean isCurrency(int oid) {
        return false; // No currency type in VeridicalDB
    }
    
    /**
     * Gets display size for a given OID.
     */
    public static int getDisplaySize(int oid) {
        switch (oid) {
            case PG_BOOL:
                return 5; // "false"
            case PG_INT2:
                return 6; // "-32768"
            case PG_INT4:
                return 11; // "-2147483648"
            case PG_INT8:
                return 20; // "-9223372036854775808"
            case PG_FLOAT4:
                return 14;
            case PG_FLOAT8:
                return 24;
            case PG_NUMERIC:
                return 131089;
            case PG_TEXT:
            case PG_VARCHAR:
                return Integer.MAX_VALUE;
            case PG_DATE:
                return 13; // "2023-12-25"
            case PG_TIME:
                return 15; // "12:34:56"
            case PG_TIMESTAMP:
                return 29; // "2023-12-25 12:34:56"
            default:
                return 0;
        }
    }
}
