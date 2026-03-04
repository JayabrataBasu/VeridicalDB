/**
 * Module declaration for VeridicalDB JDBC driver.
 * This prevents module-path conflicts with the standard java.sql module.
 */
module com.veridicaldb.jdbc {
    requires java.sql;
    requires java.base;
    
    exports com.veridicaldb.jdbc;
    exports com.veridicaldb.examples;
}
