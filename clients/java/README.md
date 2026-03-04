# VeridicalDB JDBC Driver

A JDBC 4.3 compliant driver for VeridicalDB database, implementing the PostgreSQL wire protocol.

## Features

- ✅ **JDBC 4.3 Compliance** - Standard JDBC API support
- ✅ **Connection Pooling** - Compatible with HikariCP, Apache DBCP2, c3p0
- ✅ **Prepared Statements** - Parameterized query support
- ✅ **Transaction Management** - Full ACID transaction support
- ✅ **Type Mapping** - Complete Java ↔ VeridicalDB type conversion
- ✅ **ResultSet Support** - Forward-scrolling and updateable result sets
- ✅ **Batch Operations** - Efficient batch inserts and updates
- ✅ **Auto-discovery** - Automatic driver registration via SPI

## Requirements

- Java 11 or higher
- Maven 3.6+ or Gradle 7.0+
- VeridicalDB server 0.1.0+

## Installation

### Maven

```xml
<dependency>
    <groupId>com.veridicaldb</groupId>
    <artifactId>veridicaldb-jdbc</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

```gradle
implementation 'com.veridicaldb:veridicaldb-jdbc:0.1.0'
```

### Manual Installation

```bash
cd clients/java
mvn clean install
```

## Quick Start

### Basic Connection

```java
import java.sql.*;

public class Example {
    public static void main(String[] args) {
        String url = "jdbc:veridicaldb://localhost:5432/mydb";
        String user = "admin";
        String password = "";
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to VeridicalDB!");
            
            // Execute query
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM users")) {
                
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    System.out.printf("ID: %d, Name: %s%n", id, name);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```

### Using Prepared Statements

```java
String sql = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";

try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
    pstmt.setInt(1, 1);
    pstmt.setString(2, "Alice");
    pstmt.setString(3, "alice@example.com");
    
    int rowsAffected = pstmt.executeUpdate();
    System.out.println("Inserted " + rowsAffected + " rows");
}
```

### Transaction Management

```java
try {
    conn.setAutoCommit(false);
    
    // Execute multiple statements
    stmt.executeUpdate("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
    stmt.executeUpdate("UPDATE accounts SET balance = balance + 100 WHERE id = 2");
    
    conn.commit();
    System.out.println("Transaction committed successfully");
    
} catch (SQLException e) {
    conn.rollback();
    System.err.println("Transaction rolled back: " + e.getMessage());
}
```

### Batch Operations

```java
String sql = "INSERT INTO users (id, name) VALUES (?, ?)";

try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
    for (int i = 1; i <= 1000; i++) {
        pstmt.setInt(1, i);
        pstmt.setString(2, "User" + i);
        pstmt.addBatch();
        
        if (i % 100 == 0) {
            pstmt.executeBatch(); // Execute every 100 rows
        }
    }
    pstmt.executeBatch(); // Execute remaining
}
```

## Connection URL Format

```java
jdbc:veridicaldb://host:port/database?param1=value1&param2=value2
```

### URL Components

| Component | Description | Default |
| ----------- | ------------- | --------- |
| `host` | Server hostname or IP address | localhost |
| `port` | Server port number | 5432 |
| `database` | Database name | default |

### Connection Parameters

| Parameter | Type | Description | Default |
| ----------- | ------ | ------------- | --------- |
| `user` | String | Username | admin |
| `password` | String | Password | (empty) |
| `connectTimeout` | int | Connection timeout (seconds) | 10 |
| `socketTimeout` | int | Socket timeout (seconds) | 0 (no timeout) |
| `sslMode` | String | TLS mode: `disable`, `prefer`, `require` | disable |

### Example URLs

```java
// Basic connection
"jdbc:veridicaldb://localhost:5432/mydb"

// With inline parameters
"jdbc:veridicaldb://localhost:5432/mydb?user=admin&password=secret"

// Custom timeouts
"jdbc:veridicaldb://localhost:5432/mydb?connectTimeout=5&socketTimeout=30"

// Opportunistic TLS (fallback to plaintext if TLS unavailable)
"jdbc:veridicaldb://localhost:5432/mydb?sslMode=prefer"

// Strict TLS (fail if TLS unavailable)
"jdbc:veridicaldb://localhost:5432/mydb?sslMode=require"

// Remote server
"jdbc:veridicaldb://db.example.com:5432/production"
```

## Connection Pooling

### HikariCP (Recommended)

```java
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:veridicaldb://localhost:5432/mydb");
config.setUsername("admin");
config.setPassword("");
config.setMaximumPoolSize(10);
config.setMinimumIdle(2);
config.setConnectionTimeout(5000);

HikariDataSource ds = new HikariDataSource(config);

// Use the datasource
try (Connection conn = ds.getConnection()) {
    // Execute queries
}

// Shutdown
ds.close();
```

### Apache DBCP2

```java
import org.apache.commons.dbcp2.BasicDataSource;

BasicDataSource ds = new BasicDataSource();
ds.setUrl("jdbc:veridicaldb://localhost:5432/mydb");
ds.setUsername("admin");
ds.setPassword("");
ds.setInitialSize(2);
ds.setMaxTotal(10);
ds.setMaxIdle(5);
ds.setMinIdle(2);

try (Connection conn = ds.getConnection()) {
    // Execute queries, hahaha
}

ds.close();
```

## Type Mapping

| Java Type | VeridicalDB Type | JDBC Type |
| ----------- | ------------------ | ----------- |
| `int`, `Integer` | INT32 | INTEGER |
| `long`, `Long` | INT64 | BIGINT |
| `float`, `Float` | FLOAT32 | REAL |
| `double`, `Double` | FLOAT64 | DOUBLE |
| `String` | TEXT | VARCHAR |
| `boolean`, `Boolean` | BOOL | BOOLEAN |
| `byte[]` | BYTEA | BINARY |
| `java.sql.Date` | DATE | DATE |
| `java.sql.Time` | TIME | TIME |
| `java.sql.Timestamp` | TIMESTAMP | TIMESTAMP |
| `java.math.BigDecimal` | NUMERIC | DECIMAL |

## Error Handling

```java
try (Connection conn = DriverManager.getConnection(url, user, password)) {
    // Database operations
    
} catch (SQLTimeoutException e) {
    System.err.println("Connection timeout: " + e.getMessage());
} catch (SQLSyntaxErrorException e) {
    System.err.println("SQL syntax error: " + e.getMessage());
} catch (SQLException e) {
    System.err.println("Database error: " + e.getMessage());
    System.err.println("SQL State: " + e.getSQLState());
    System.err.println("Error Code: " + e.getErrorCode());
}
```

## Advanced Features

### ResultSet Metadata

```java
ResultSet rs = stmt.executeQuery("SELECT * FROM users");
ResultSetMetaData meta = rs.getMetaData();

int columnCount = meta.getColumnCount();
for (int i = 1; i <= columnCount; i++) {
    System.out.printf("Column %d: %s (%s)%n",
        i,
        meta.getColumnName(i),
        meta.getColumnTypeName(i)
    );
}
```

### Database Metadata

```java
DatabaseMetaData dbMeta = conn.getMetaData();
System.out.println("Database: " + dbMeta.getDatabaseProductName());
System.out.println("Version: " + dbMeta.getDatabaseProductVersion());
System.out.println("Driver: " + dbMeta.getDriverName());
System.out.println("Driver Version: " + dbMeta.getDriverVersion());

// List tables
ResultSet tables = dbMeta.getTables(null, null, "%", new String[]{"TABLE"});
while (tables.next()) {
    System.out.println("Table: " + tables.getString("TABLE_NAME"));
}
```

## Building from Source

```bash
# Clone repository
git clone https://github.com/JayabrataBasu/VeridicalDB.git
cd VeridicalDB/clients/java

# Build with Maven
mvn clean package

# Build with Gradle
gradle build

# Run tests
mvn test  # or: gradle test

# Install locally
mvn install
```

## Examples

See the `examples/` directory for complete working examples:

- `BasicConnection.java` - Simple connection and queries
- `PreparedStatementExample.java` - Parameterized queries
- `TransactionExample.java` - Transaction management
- `BatchOperationExample.java` - Batch inserts
- `ConnectionPoolExample.java` - HikariCP integration
- `ResultSetExample.java` - Working with result sets

## Testing

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=VeridicalDriverTest

# Run with coverage
mvn test jacoco:report
```

## Performance Tips

1. **Use Prepared Statements** - Reuse for multiple executions
2. **Enable Connection Pooling** - Use HikariCP for production
3. **Batch Operations** - Combine multiple inserts/updates
4. **Set Fetch Size** - Optimize large result sets
5. **Close Resources** - Use try-with-resources
6. **Disable Auto-commit** - For bulk operations

## Limitations

Current version (0.1.0) has the following limitations:

- ❌ Stored procedures not yet supported
- ❌ Scrollable result sets (TYPE_SCROLL_*) not supported
- ❌ Updatable result sets (CONCUR_UPDATABLE) not supported
- ❌ Array and custom types limited support
- ❌ SSL/TLS connections not yet implemented

These features are planned for future releases.

## Troubleshooting

### Driver not found

**Issue:** `java.sql.SQLException: No suitable driver found`

**Solution:** Ensure the driver JAR is in your classpath, or add Maven/Gradle dependency.

### Connection timeout

**Issue:** Connection hangs or times out

**Solution:**

- Check server is running: `telnet localhost 5432`
- Increase `connectTimeout` parameter
- Verify firewall settings

### SQL syntax errors

**Issue:** `SQLSyntaxErrorException` on valid SQL

**Solution:**

- Check VeridicalDB SQL dialect compatibility
- Use prepared statements for complex queries
- Escape special characters properly

## License

MIT License - See main repository for details

## Contributing

Contributions welcome! See the main repository's CONTRIBUTING.md for guidelines.

## Support

- **Issues:** <https://github.com/JayabrataBasu/VeridicalDB/issues>
- **Discussions:** <https://github.com/JayabrataBasu/VeridicalDB/discussions>
- **Documentation:** <https://github.com/JayabrataBasu/VeridicalDB/tree/master/docs>

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.
