package com.evotechsoftwaresolutions.evemp.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MySQL {
    // Static variable to hold the single connection (singleton pattern)
    private static Connection connection;
    
    // Database connection details - consider moving these to a config file
    private static final String USER = "root";
    private static final String PASSWORD = "Slk2005RC@";
    private static final String DB_NAME = "evotech_db";
    
    // Logger for recording errors and events
    private static final Logger logger = Logger.getLogger(MySQL.class.getName());
    
    /**
     * Gets a database connection (creates one if it doesn't exist or is closed)
     * Synchronized to prevent multiple threads from creating connections at the same time
     * @return Active database connection
     */
    public static synchronized Connection getConnection() {
        try {
            // Check if we need a new connection
            if (connection == null || connection.isClosed()) {
                // Load the MySQL driver
                Class.forName("com.mysql.cj.jdbc.Driver");
                
                // Create new connection to database
                connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/" + DB_NAME, USER, PASSWORD);
                
                logger.info("New database connection established");
            }
            return connection;
        } catch (ClassNotFoundException | SQLException ex) {
            // Log the error and throw a runtime exception
            logger.log(Level.SEVERE, "Connection error", ex);
            throw new ExceptionInInitializerError(ex);
        }
    }
    
    /**
     * Closes the main database connection
     * Call this when your application shuts down
     */
    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
                logger.info("Database connection closed");
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, "Error closing connection", ex);
                throw new RuntimeException(ex);
            }
        }
    }
    
    /**
     * Test if database connection is working
     * @return true if connection is successful, false otherwise
     */
    public static boolean testConnection() {
        try {
            // Try to get a connection and test it with a simple query
            Connection conn = getConnection();
            if (conn != null && !conn.isClosed()) {
                // Test with a simple query that should always work
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 1");
                     ResultSet rs = stmt.executeQuery()) {
                    
                    boolean hasResult = rs.next();
                    logger.info("Database connection test: " + (hasResult ? "SUCCESS" : "FAILED"));
                    return hasResult;
                }
            }
            return false;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Database connection test failed", e);
            return false;
        }
    }
    
    /**
     * Execute INSERT, UPDATE, DELETE queries (IUD = Insert, Update, Delete)
     * Automatically closes all resources when done
     * @param query SQL query with ? placeholders
     * @param parameters Values to replace the ? placeholders
     */
    public static void iud(String query, Object... parameters) {
        // try-with-resources automatically closes PreparedStatement and Connection
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            // Set each parameter in the query (? placeholders)
            for (int i = 0; i < parameters.length; i++) {
                stmt.setObject(i + 1, parameters[i]); // SQL parameters start at 1, not 0
            }
            
            // Execute the query and get number of affected rows
            int affectedRows = stmt.executeUpdate();
            logger.info("Query executed successfully. Affected rows: " + affectedRows);
            
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "IUD error for query: " + query, ex);
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Execute SELECT queries and return data as a List of Maps
     * Each Map represents one row, with column names as keys
     * Automatically closes all resources after copying data
     * @param query SQL SELECT query with ? placeholders
     * @param parameters Values to replace the ? placeholders
     * @return List of rows, each row is a Map of column names to values
     */
    public static List<Map<String, Object>> search(String query, Object... parameters) {
        List<Map<String, Object>> results = new ArrayList<>();
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            for (int i = 0; i < parameters.length; i++) {
                stmt.setObject(i + 1, parameters[i]);
            }
            
            // Execute query and process results
            try (ResultSet rs = stmt.executeQuery()) {
                // Get information about the columns
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                // Process each row
                while (rs.next()) {
                    // Create a map for this row
                    Map<String, Object> row = new HashMap<>();
                    
                    // Copy each column value into the map
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object value = rs.getObject(i);
                        row.put(columnName, value);
                    }
                    
                    // Add this row to results
                    results.add(row);
                }
            }
            
            logger.info("Search query executed successfully. Rows returned: " + results.size());
            
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Search error for query: " + query, ex);
            throw new RuntimeException(ex);
        }
        
        return results;
    }
    
    /**
     * Check if a record exists
     * @param query SQL query (usually SELECT COUNT(*) or SELECT 1)
     * @param parameters Values to replace the ? placeholders
     * @return true if at least one record exists
     */
    public static boolean exists(String query, Object... parameters) {
        List<Map<String, Object>> results = search(query, parameters);
        return !results.isEmpty();
    }
}