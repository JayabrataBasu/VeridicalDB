package com.veridicaldb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * VeridicalDB JDBC Driver implementation.
 * 
 * <p>This driver implements the JDBC 4.3 specification for connecting to VeridicalDB
 * databases using the PostgreSQL wire protocol.
 * 
 * <p>Connection URL format:
 * <pre>jdbc:veridicaldb://host:port/database?user=username&password=password</pre>
 * 
 * <p>Example usage:
 * <pre>{@code
 * String url = "jdbc:veridicaldb://localhost:5432/mydb";
 * Properties props = new Properties();
 * props.setProperty("user", "admin");
 * props.setProperty("password", "");
 * 
 * Connection conn = DriverManager.getConnection(url, props);
 * }</pre>
 * 
 * @since 0.1.0
 */
public class VeridicalDriver implements Driver {
    
    private static final Logger logger = Logger.getLogger(VeridicalDriver.class.getName());
    
    private static final String URL_PREFIX = "jdbc:veridicaldb:";
    private static final int MAJOR_VERSION = 0;
    private static final int MINOR_VERSION = 1;
    
    private static VeridicalDriver registeredDriver;
    
    static {
        try {
            register();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    
    /**
     * Register the driver with the DriverManager.
     * 
     * @throws SQLException if registration fails
     */
    public static void register() throws SQLException {
        if (registeredDriver == null) {
            registeredDriver = new VeridicalDriver();
            DriverManager.registerDriver(registeredDriver);
        }
    }
    
    /**
     * Deregister the driver from the DriverManager.
     * 
     * @throws SQLException if deregistration fails
     */
    public static void deregister() throws SQLException {
        if (registeredDriver != null) {
            DriverManager.deregisterDriver(registeredDriver);
            registeredDriver = null;
        }
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        try {
            ConnectionProperties props = parseURL(url, info);
            return new VeridicalConnection(props);
        } catch (Exception e) {
            throw new SQLException("Failed to establish connection: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }
    

    //this is like an ID card for the driver, verifies stuff
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        ConnectionProperties props = parseURL(url, info);
        
        return new DriverPropertyInfo[] {
            createPropertyInfo("host", props.getHost(), "Database host", true),
            createPropertyInfo("port", String.valueOf(props.getPort()), "Database port", false),
            createPropertyInfo("database", props.getDatabase(), "Database name", true),
            createPropertyInfo("user", props.getUser(), "Username", true),
            createPropertyInfo("password", props.getPassword(), "Password", false),
            createPropertyInfo("connectTimeout", String.valueOf(props.getConnectTimeout()), 
                             "Connection timeout (seconds)", false),
            createPropertyInfo("socketTimeout", String.valueOf(props.getSocketTimeout()), 
                             "Socket timeout (seconds)", false)
        };
    }
    
    private DriverPropertyInfo createPropertyInfo(String name, String value, 
                                                   String description, boolean required) {
        DriverPropertyInfo info = new DriverPropertyInfo(name, value);
        info.description = description;
        info.required = required;
        return info;
    }
    
    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }
    
    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }
    
    @Override
    public boolean jdbcCompliant() {
        // We aim for JDBC compliance but currently have some limitations
        return false;
    }
    
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return logger;
    }
    
    /**
     * Parse the JDBC URL and extract connection properties.
     * 
     * @param url the JDBC URL
     * @param info additional properties
     * @return parsed connection properties
     * @throws SQLException if URL is invalid
     */
    private ConnectionProperties parseURL(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("Invalid URL: " + url);
        }
        
        // Remove prefix: jdbc:veridicaldb://
        String remaining = url.substring(URL_PREFIX.length());
        if (remaining.startsWith("//")) {
            remaining = remaining.substring(2);
        }
        
        ConnectionProperties props = new ConnectionProperties();
        
        // Extract host:port/database?params
        String hostPort;
        String database = "default";
        String params = "";
        
        int slashIndex = remaining.indexOf('/');
        if (slashIndex >= 0) {
            hostPort = remaining.substring(0, slashIndex);
            String rest = remaining.substring(slashIndex + 1);
            
            int questionIndex = rest.indexOf('?');
            if (questionIndex >= 0) {
                database = rest.substring(0, questionIndex);
                params = rest.substring(questionIndex + 1);
            } else {
                database = rest;
            }
        } else {
            int questionIndex = remaining.indexOf('?');
            if (questionIndex >= 0) {
                hostPort = remaining.substring(0, questionIndex);
                params = remaining.substring(questionIndex + 1);
            } else {
                hostPort = remaining;
            }
        }
        
        // Parse host:port
        int colonIndex = hostPort.indexOf(':');
        if (colonIndex >= 0) {
            props.setHost(hostPort.substring(0, colonIndex));
            props.setPort(Integer.parseInt(hostPort.substring(colonIndex + 1)));
        } else {
            props.setHost(hostPort);
            props.setPort(5432); // Default port
        }
        
        props.setDatabase(database);
        
        // Parse URL parameters
        if (!params.isEmpty()) {
            for (String param : params.split("&")) {
                int equalIndex = param.indexOf('=');
                if (equalIndex >= 0) {
                    String key = param.substring(0, equalIndex);
                    String value = param.substring(equalIndex + 1);
                    info.setProperty(key, value);
                }
            }
        }
        
        // Apply properties from Properties object
        if (info != null) {
            if (info.containsKey("user")) {
                props.setUser(info.getProperty("user"));
            }
            if (info.containsKey("password")) {
                props.setPassword(info.getProperty("password"));
            }
            if (info.containsKey("connectTimeout")) {
                props.setConnectTimeout(Integer.parseInt(info.getProperty("connectTimeout")));
            }
            if (info.containsKey("socketTimeout")) {
                props.setSocketTimeout(Integer.parseInt(info.getProperty("socketTimeout")));
            }
        }
        
        return props;
    }
}
