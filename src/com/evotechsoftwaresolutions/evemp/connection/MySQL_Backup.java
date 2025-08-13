package com.evotechsoftwaresolutions.evemp.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MySQL_Backup {

    private static Connection connection;
    private static final String USER = "root";
    private static final String PASSWORD = "Slk2005RC";
    private static final String DB_NAME = "textile_db";

    public static Connection getConnection() {
        try {
            if (connection == null) {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + DB_NAME, USER, PASSWORD);
            }
            return connection;
        } catch (ClassNotFoundException ex) {
            throw new ExceptionInInitializerError("Driver not found");
        } catch (SQLException ex) {
            throw new ExceptionInInitializerError("SQL Error");
        }
    }

    public static void closeConnection() {
        try {
            connection.close();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void iud(String query) {
        try {
            MySQL_Backup.getConnection().createStatement().executeUpdate(query);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static ResultSet search(String query) throws SQLException {
        return getConnection().createStatement().executeQuery(query);
    }
}
