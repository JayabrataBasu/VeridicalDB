package com.veridicaldb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements PostgreSQL wire protocol for communication with VeridicalDB.
 * 
 * <p>Message format:
 * <ul>
 *   <li>Type: 1 byte (char)</li>
 *   <li>Length: 4 bytes (int32, includes length itself but not type)</li>
 *   <li>Payload: variable length</li>
 * </ul>
 */
public class WireProtocol {
    
    @SuppressWarnings("unused")
    private final InputStream input;
    @SuppressWarnings("unused")
    private final OutputStream output;
    private final DataInputStream dataInput;
    private final DataOutputStream dataOutput;
    
    /**
     * Protocol message types. According to PostgreSQL documentation. Please refer to offical docs or google.
     */
    public static class MessageType {
        public static final char AUTHENTICATION = 'R';
        public static final char BACKEND_KEY_DATA = 'K';
        public static final char BIND_COMPLETE = '2';
        public static final char CLOSE_COMPLETE = '3';
        public static final char COMMAND_COMPLETE = 'C';
        public static final char DATA_ROW = 'D';
        public static final char EMPTY_QUERY_RESPONSE = 'I';
        public static final char ERROR_RESPONSE = 'E';
        public static final char NO_DATA = 'n';
        public static final char NOTICE_RESPONSE = 'N';
        public static final char PARAMETER_DESCRIPTION = 't';
        public static final char PARAMETER_STATUS = 'S';
        public static final char PARSE_COMPLETE = '1';
        public static final char PORTAL_SUSPENDED = 's';
        public static final char READY_FOR_QUERY = 'Z';
        public static final char ROW_DESCRIPTION = 'T';
    }
    
    public WireProtocol(InputStream input, OutputStream output) {
        this.input = input;
        this.output = output;
        this.dataInput = new DataInputStream(input);
        this.dataOutput = new DataOutputStream(output);
    }
    
    /**
     * Sends startup message to initiate connection.
     *
     * @param database Database name
     * @param user Username
     */
    public void sendStartup(String database, String user) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream buf = new DataOutputStream(buffer);
        
        // Protocol version (3.0)
        buf.writeInt(0x00030000);
        
        // Parameters
        writeString(buf, "user");
        writeString(buf, user);
        writeString(buf, "database");
        writeString(buf, database);
        writeString(buf, "client_encoding");
        writeString(buf, "UTF8");
        
        // Terminator
        buf.writeByte(0);
        
        byte[] payload = buffer.toByteArray();
        
        // Send length (including itself) and payload
        dataOutput.writeInt(payload.length + 4);
        dataOutput.write(payload);
        dataOutput.flush();
    }
    
    /**
     * Sends password message.
     *
     * @param password Clear text password
     */
    public void sendPassword(String password) throws IOException {
        sendMessage('p', password);
    }
    
    /**
     * Sends simple query message.
     *
     * @param sql SQL query
     */
    public void sendSimpleQuery(String sql) throws IOException {
        sendMessage('Q', sql);
    }
    
    /**
     * Sends parse message for prepared statement.
     *
     * @param statementName Statement name (empty for unnamed)
     * @param sql SQL query
     * @param paramTypes Parameter OIDs (empty for auto-detect)
     */
    public void sendParse(String statementName, String sql, int[] paramTypes) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream buf = new DataOutputStream(buffer);
        
        writeString(buf, statementName);
        writeString(buf, sql);
        
        buf.writeShort(paramTypes.length);
        for (int oid : paramTypes) {
            buf.writeInt(oid);
        }
        
        sendMessageBytes('P', buffer.toByteArray());
    }
    
    /**
     * Sends bind message for prepared statement parameters.
     *
     * @param portalName Portal name (empty for unnamed)
     * @param statementName Statement name (empty for unnamed)
     * @param parameters Parameter values
     */
    public void sendBind(String portalName, String statementName, List<byte[]> parameters) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream buf = new DataOutputStream(buffer);
        
        writeString(buf, portalName);
        writeString(buf, statementName);
        
        // Parameter format codes (0 = text, 1 = binary) - use text for all
        buf.writeShort(1);
        buf.writeShort(0); // All text format
        
        // Parameter values
        buf.writeShort(parameters.size());
        for (byte[] param : parameters) {
            if (param == null) {
                buf.writeInt(-1); // NULL
            } else {
                buf.writeInt(param.length);
                buf.write(param);
            }
        }
        
        // Result column format codes - use text for all
        buf.writeShort(1);
        buf.writeShort(0); // All text format
        
        sendMessageBytes('B', buffer.toByteArray());
    }
    
    /**
     * Sends describe message.
     *
     * @param type 'S' for statement, 'P' for portal
     * @param name Name of statement or portal (empty for unnamed)
     */
    public void sendDescribe(char type, String name) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream buf = new DataOutputStream(buffer);
        
        buf.writeByte(type);
        writeString(buf, name);
        
        sendMessageBytes('D', buffer.toByteArray());
    }
    
    /**
     * Sends execute message.
     *
     * @param portalName Portal name (empty for unnamed)
     * @param maxRows Maximum rows to return (0 = unlimited)
     */
    public void sendExecute(String portalName, int maxRows) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream buf = new DataOutputStream(buffer);
        
        writeString(buf, portalName);
        buf.writeInt(maxRows);
        
        sendMessageBytes('E', buffer.toByteArray());
    }
    
    /**
     * Sends sync message to complete extended query protocol.
     */
    public void sendSync() throws IOException {
        sendMessageBytes('S', new byte[0]);
    }
    
    /**
     * Sends terminate message to close connection.
     */
    public void sendTerminate() throws IOException {
        dataOutput.writeByte('X');
        dataOutput.writeInt(4);
        dataOutput.flush();
    }
    
    /**
     * Receives a message from server.
     *
     * @return Message object
     */
    public Message receiveMessage() throws IOException, SQLException {
        char type = (char) dataInput.readByte();
        int length = dataInput.readInt() - 4; // Length includes itself
        
        byte[] payload = new byte[length];
        dataInput.readFully(payload);
        
        Message msg = new Message(type, payload);
        
        // Check for error response
        if (type == MessageType.ERROR_RESPONSE) {
            throw parseError(msg);
        }
        
        return msg;
    }
    
    /**
     * Waits for ReadyForQuery message.
     */
    public void waitForReady() throws IOException, SQLException {
        while (true) {
            Message msg = receiveMessage();
            if (msg.type == MessageType.READY_FOR_QUERY) {
                break;
            }
            // Skip other messages (notice, parameter status, etc.)
        }
    }
    
    /**
     * Parses error message into SQLException.
     */
    private SQLException parseError(Message msg) throws IOException {
        String sqlState = "XX000";
        String message = "Unknown error";
        String detail = null;
        String hint = null;
        
        DataInputStream in = msg.getDataInputStream();
        while (true) {
            char fieldType = (char) in.readByte();
            if (fieldType == 0) break;
            
            String value = readString(in);
            
            switch (fieldType) {
                case 'S': // Severity
                    break;
                case 'C': // SQLSTATE
                    sqlState = value;
                    break;
                case 'M': // Message
                    message = value;
                    break;
                case 'D': // Detail
                    detail = value;
                    break;
                case 'H': // Hint
                    hint = value;
                    break;
            }
        }
        
        String fullMessage = message;
        if (detail != null) {
            fullMessage += "\nDetail: " + detail;
        }
        if (hint != null) {
            fullMessage += "\nHint: " + hint;
        }
        
        return new SQLException(fullMessage, sqlState);
    }
    
    /**
     * Sends a simple text message.
     */
    private void sendMessage(char type, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(bytes);
        buffer.write(0); // Null terminator
        
        sendMessageBytes(type, buffer.toByteArray());
    }
    
    /**
     * Sends a message with raw bytes.
     */
    private void sendMessageBytes(char type, byte[] payload) throws IOException {
        dataOutput.writeByte(type);
        dataOutput.writeInt(payload.length + 4); // +4 for length field itself
        dataOutput.write(payload);
        dataOutput.flush();
    }
    
    /**
     * Writes null-terminated string to output stream.
     */
    private void writeString(DataOutputStream out, String str) throws IOException {
        out.write(str.getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
    }
    
    /**
     * Reads null-terminated string from input stream.
     */
    private String readString(DataInputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.readByte()) != 0) {
            buffer.write(b);
        }
        return buffer.toString(StandardCharsets.UTF_8.name());
    }
    
    /**
     * Represents a protocol message.
     */
    public static class Message {
        public final char type;
        private final byte[] payload;
        private int position = 0;
        
        public Message(char type, byte[] payload) {
            this.type = type;
            this.payload = payload;
        }
        
        public DataInputStream getDataInputStream() {
            return new DataInputStream(new ByteArrayInputStream(payload));
        }
        
        public int getInt() throws IOException {
            DataInputStream in = getDataInputStream();
            in.skip(position);
            int value = in.readInt();
            position += 4;
            return value;
        }
        
        public String getString() throws IOException {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            for (int i = position; i < payload.length; i++) {
                byte b = payload[i];
                if (b == 0) {
                    position = i + 1;
                    break;
                }
                buffer.write(b);
            }
            return buffer.toString(StandardCharsets.UTF_8.name());
        }
        
        public byte[] getPayload() {
            return payload;
        }
        
        public int getLength() {
            return payload.length;
        }
    }
    
    /**
     * Represents row description (column metadata).
     */
    public static class RowDescription {
        public final List<ColumnInfo> columns;
        
        public RowDescription() {
            this.columns = new ArrayList<>();
        }
        
        public void addColumn(String name, int tableOid, short columnNumber, 
                            int typeOid, short typeSize, int typeMod, short formatCode) {
            columns.add(new ColumnInfo(name, tableOid, columnNumber, typeOid, 
                                      typeSize, typeMod, formatCode));
        }
        
        public int getColumnCount() {
            return columns.size();
        }
        
        public ColumnInfo getColumn(int index) {
            return columns.get(index);
        }
    }
    
    /**
     * Represents column metadata.
     */
    public static class ColumnInfo {
        public final String name;
        public final int tableOid;
        public final short columnNumber;
        public final int typeOid;
        public final short typeSize;
        public final int typeMod;
        public final short formatCode;
        
        public ColumnInfo(String name, int tableOid, short columnNumber, 
                         int typeOid, short typeSize, int typeMod, short formatCode) {
            this.name = name;
            this.tableOid = tableOid;
            this.columnNumber = columnNumber;
            this.typeOid = typeOid;
            this.typeSize = typeSize;
            this.typeMod = typeMod;
            this.formatCode = formatCode;
        }
    }
}
