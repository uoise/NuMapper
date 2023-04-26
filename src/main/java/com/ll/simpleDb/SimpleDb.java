package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleDb {
    private final DBConnectionPool dbConnectionPool;
    private Connection connection;
    private static final String HOST_FORMAT;
    private static final int PORT;

    static {
        PORT = 3306;
        HOST_FORMAT = "jdbc:mysql://%s:%s/%s";
    }

    public SimpleDb(String host, String id, String password, String database) {
        dbConnectionPool = new DBConnectionPool(HOST_FORMAT.formatted(host, PORT, database), id, password);
    }

    public void setDevMode(boolean b) {
    }

    public void run(String queryString) {
        Statement statement;
        try {
            connection = dbConnectionPool.get();
            statement = connection.createStatement();
            boolean result = statement.execute(queryString);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnectionPool.release(connection);
        }
    }


    public void run(String queryString, Object... args) {
        PreparedStatement pstmt;
        try {
            connection = dbConnectionPool.get();
            pstmt = connection.prepareStatement(queryString);
            int idx = 0;
            for (Object o : args) pstmt.setObject(++idx, o);
            int rowCount = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public Sql genSql() {
        return Sql.of(dbConnectionPool);
    }
}
