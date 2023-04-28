package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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

    public void run(String queryString, Object... args) {
        try {
            connection = dbConnectionPool.getConnection();
            try (PreparedStatement pStmt = connection.prepareStatement(queryString)) {
                pStmt.setQueryTimeout(queryTimeout);
                int idx = 0;
                for (Object o : args) pStmt.setObject(++idx, o);
                pStmt.executeUpdate();
            } catch (SQLException e) {
                System.out.println("SQL Exception: Failed to get Statement");
            }
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            dbConnectionPool.releaseConnection(connection);
        }
    }

    public Sql genSql() {
        return Sql.of(dbConnectionPool, queryTimeout, devMode);
    }

    public void close() {
        dbConnectionPool.closeAllConnections();
    }
}
