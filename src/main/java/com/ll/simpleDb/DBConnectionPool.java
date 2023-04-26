package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DBConnectionPool {
    private static final int DEFAULT_INITIAL_POOL_SIZE = 5;
    private final String url;
    private final String username;
    private final String password;
    private final int initialPoolSize;
    private final long connectionTimeout;
    private final List<Connection> availableConnections;
    private final List<Connection> usedConnections;

    public DBConnectionPool(String url, String username, String password) {
        this(url, username, password, DEFAULT_INITIAL_POOL_SIZE, TimeUnit.SECONDS.toMillis(1));
    }

    public DBConnectionPool(String url, String username, String password, int initialPoolSize, long connectionTimeout) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.initialPoolSize = initialPoolSize;
        this.connectionTimeout = connectionTimeout;
        this.availableConnections = new ArrayList<>(initialPoolSize);
        this.usedConnections = new ArrayList<>();
        initializeConnections();
    }

    private void initializeConnections() {
        for (int i = 0; i < initialPoolSize; i++) {
            availableConnections.add(createConnection());
        }
    }

    private Connection createConnection() {
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new SQLRuntimeException("Error creating connection", e);
        }
    }

    public synchronized Connection get() {
        Connection connection;
        if (availableConnections.isEmpty()) {
            connection = createConnection();
        } else {
            connection = availableConnections.remove(availableConnections.size() - 1);
            if (!isValid(connection)) {
                closeConnection(connection);
                connection = createConnection();
            }
        }
        usedConnections.add(connection);
        return connection;
    }

    private boolean isValid(Connection connection) {
        try {
            return !connection.isClosed() && connection.isValid((int) connectionTimeout);
        } catch (SQLException e) {
            return false;
        }
    }

    private void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            // ignore
        }
    }

    public synchronized void release(Connection connection) {
        if (usedConnections.remove(connection)) {
            if (isValid(connection)) {
                availableConnections.add(connection);
            } else {
                closeConnection(connection);
            }
        }
    }

    public synchronized void closeAll() {
        for (Connection connection : availableConnections) {
            closeConnection(connection);
        }
        availableConnections.clear();
        for (Connection connection : usedConnections) {
            closeConnection(connection);
        }
        usedConnections.clear();
    }
}
