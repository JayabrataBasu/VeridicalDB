package com.veridicaldb.jdbc;

/**
 * Connection properties for VeridicalDB JDBC driver.
 * 
 * <p>Holds configuration parameters for database connections including
 * host, port, database name, credentials, and timeout settings.
 * 
 * @since 0.1.0
 */
public class ConnectionProperties {
    
    private String host = "localhost";
    private int port = 5432;
    private String database = "default";
    private String user = "admin";
    private String password = "";
    private int connectTimeout = 10; // seconds
    private int socketTimeout = 0; // 0 = no timeout
    private boolean autoCommit = true;
    private int fetchSize = 0; // 0 = use default
    
    public ConnectionProperties() {
    }
    
    public String getHost() {
        return host;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public String getDatabase() {
        return database;
    }
    
    public void setDatabase(String database) {
        this.database = database;
    }
    
    public String getUser() {
        return user;
    }
    
    public void setUser(String user) {
        this.user = user;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public int getConnectTimeout() {
        return connectTimeout;
    }
    
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }
    
    public int getSocketTimeout() {
        return socketTimeout;
    }
    
    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }
    
    public boolean isAutoCommit() {
        return autoCommit;
    }
    
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }
    
    public int getFetchSize() {
        return fetchSize;
    }
    
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }
    
    //this is so peak, my dude
    @Override
    public String toString() {
        return "ConnectionProperties{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", database='" + database + '\'' +
                ", user='" + user + '\'' +
                ", connectTimeout=" + connectTimeout +
                ", socketTimeout=" + socketTimeout +
                ", autoCommit=" + autoCommit +
                ", fetchSize=" + fetchSize +
                '}';
    }
}
