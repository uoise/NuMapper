package com.ll.simpleDb;

import java.sql.*;

public class SimpleDb {
    private Connection connection;
    private Statement statement;
    private static final String hostFormat;

    static {
        hostFormat = "jdbc:mysql://%s:3306/%s";
    }

    public SimpleDb(String host, String id, String password, String database) {
        try {
            connection = DriverManager.getConnection(hostFormat.formatted(host, database), id, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setDevMode(boolean b) {
    }

    public void run(String queryString) {
        try {
            statement = connection.createStatement();
            boolean result = statement.execute(queryString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void run(String queryString, Object... args) {
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(queryString);
            int idx = 0;
            for (Object o : args) pstmt.setObject(++idx, o);
            int rowCount = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Sql genSql() {
        return Sql.of(connection);
    }
}
