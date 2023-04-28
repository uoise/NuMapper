package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleDb {
    private final DBConnectionPool dbConnectionPool;
    private Connection connection;
    private int queryTimeout;
    private boolean devMode;
    private static final String HOST_FORMAT;
    private static final int PORT;
    private static final int DEFAULT_QUERY_TIMEOUT;

    static {
        DEFAULT_QUERY_TIMEOUT = 1;
        PORT = 3306;
        HOST_FORMAT = "jdbc:mysql://%s:%s/%s";
    }

    public SimpleDb(String host, String id, String password, String database) {
        dbConnectionPool = new DBConnectionPool(HOST_FORMAT.formatted(host, PORT, database), id, password);
        queryTimeout = DEFAULT_QUERY_TIMEOUT;
        devMode = false;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public void setDevMode(boolean devMode) {
        this.devMode = devMode;
    }

    public void run(String queryString) {
        Statement statement;
        try {
            connection = dbConnectionPool.get();
            statement = connection.createStatement();
            boolean result = statement.execute(queryString);
        } catch (SQLException | InterruptedException e) {
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
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public Sql genSql() {
        return Sql.of(dbConnectionPool, queryTimeout, devMode);
    }

    public void close() {
        dbConnectionPool.closeAll();
    }
}
