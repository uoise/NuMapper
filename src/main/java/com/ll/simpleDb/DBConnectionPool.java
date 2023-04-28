package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DBConnectionPool {
    private record ConnectionInfo(Connection connection, long timestamp) {
    }

    private static final int DEFAULT_MIN_POOL_SIZE = 1;
    private static final int DEFAULT_MAX_POOL_SIZE = 2;
    private static final int DEFAULT_WAIT_TIMEOUT = 5;
    private static final int DEFAULT_MAX_IDLE_TIME = 30;
    private final String url;
    private final String username;
    private final String password;
    private final int minPoolSize;
    private final int maxPoolSize;
    private final long waitTimeout;
    private final long maxIdleTime;
    private final Queue<ConnectionInfo> availableConnections;
    private final Set<Connection> usedConnections;
    private int activeConnectionCount;

    public DBConnectionPool(String url, String username, String password) {
        this(url, username, password, DEFAULT_MIN_POOL_SIZE, DEFAULT_MAX_POOL_SIZE, DEFAULT_WAIT_TIMEOUT, DEFAULT_MAX_IDLE_TIME);
    }

    public DBConnectionPool(String url, String username, String password, int minPoolSize, int maxPoolSize, int waitTimeout, int maxIdleTime) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.minPoolSize = minPoolSize;
        this.maxPoolSize = maxPoolSize;
        this.waitTimeout = TimeUnit.SECONDS.toSeconds(waitTimeout);
        this.maxIdleTime = TimeUnit.SECONDS.toSeconds(maxIdleTime);
        activeConnectionCount = 0;
        availableConnections = new LinkedList<>();
        usedConnections = new HashSet<>();
    }

    private Connection createConnection() {
        try {
            Connection connection = DriverManager.getConnection(url, username, password);
            usedConnections.add(connection);
            return connection;
        } catch (SQLException e) {
            throw new SQLRuntimeException("Create connection failed", e);
        }
    }

    public synchronized Connection get() throws SQLException, InterruptedException {
        if (availableConnections.isEmpty() && activeConnectionCount < maxPoolSize) {
            availableConnections.add(new ConnectionInfo(createConnection(), System.currentTimeMillis()));
            activeConnectionCount++;
        }

        long start = System.currentTimeMillis();
        long remaining = waitTimeout;

        while (availableConnections.isEmpty()) {
            wait(remaining);
            remaining = waitTimeout - (System.currentTimeMillis() - start);
            if (remaining <= 0) {
                throw new SQLTimeoutException("Timeout while waiting for a connection from the pool");
            }
        }

        ConnectionInfo connectionInfo = availableConnections.poll();
        long idleTime = System.currentTimeMillis() - connectionInfo.timestamp;
        if (idleTime > maxIdleTime) {
            closeConnection(connectionInfo.connection);
            connectionInfo = null;
        }

        if (connectionInfo == null) {
            return get();
        }

        Connection connection = connectionInfo.connection;
        usedConnections.add(connection);
        return connection;
    }

    public synchronized void release(Connection connection) {
        usedConnections.remove(connection);
        availableConnections.offer(new ConnectionInfo(connection, System.currentTimeMillis()));
        notifyAll();
    }

    public void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new SQLRuntimeException("Fail to close connection", e);
        }
    }

    public synchronized void closeAll() {
        for (ConnectionInfo connectionInfo : availableConnections) {
            closeConnection(connectionInfo.connection);
        }
        availableConnections.clear();
        for (Connection connection : usedConnections) {
            closeConnection(connection);
        }
        usedConnections.clear();
    }
}