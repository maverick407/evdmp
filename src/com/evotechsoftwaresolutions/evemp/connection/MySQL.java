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
    private static Connection connection;
    private static final String USER = "root";
    private static final String PASSWORD = "Slk2005RC@";
    private static final String DB_NAME = "evotech_db";
    private static final Logger logger = Logger.getLogger(MySQL.class.getName());

    public static synchronized Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/" + DB_NAME, USER, PASSWORD);
                logger.info("New database connection established");
            }
            return connection;
        } catch (ClassNotFoundException | SQLException ex) {
            logger.log(Level.SEVERE, "Connection error", ex);
            throw new ExceptionInInitializerError(ex);
        }
    }

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

    public static boolean testConnection() {
        try {
            Connection conn = getConnection();
            if (conn != null && !conn.isClosed()) {
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

    public static void iud(String query, Object... parameters) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < parameters.length; i++) {
                stmt.setObject(i + 1, parameters[i]);
            }
            int affectedRows = stmt.executeUpdate();
            logger.info("Query executed successfully. Affected rows: " + affectedRows);
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "IUD error for query: " + query, ex);
            throw new RuntimeException(ex);
        }
    }

    public static List<Map<String, Object>> search(String query, Object... parameters) {
        List<Map<String, Object>> results = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < parameters.length; i++) {
                stmt.setObject(i + 1, parameters[i]);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object value = rs.getObject(i);
                        row.put(columnName, value);
                    }
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

    public static boolean exists(String query, Object... parameters) {
        List<Map<String, Object>> results = search(query, parameters);
        return !results.isEmpty();
    }
}
