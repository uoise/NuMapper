package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DBConnectionPool {
    private static class ConnectionInfo {
        public final Connection connection;
        public final long timestamp;

        ConnectionInfo(Connection connection) {
            this.connection = connection;
            timestamp = System.currentTimeMillis();
        }
    }

    private static final int DEFAULT_MIN_POOL_SIZE = 5;
    private static final int DEFAULT_MAX_POOL_SIZE = 10;
    private static final long DEFAULT_WAIT_TIMEOUT = 1000;
    private static final long DEFAULT_MAX_IDLE_TIME = 30000;
    private final String url;
    private final String username;
    private final String password;
    private final int minPoolSize;
    private final int maxPoolSize;
    private final long waitTimeout;
    private final long maxIdleTime;
    private final ConcurrentLinkedQueue<ConnectionInfo> availableConnections;
    private final ConcurrentLinkedQueue<Connection> usedConnections;
    private final AtomicInteger activeConnectionCount;

    public DBConnectionPool(String url, String username, String password) {
        this(url, username, password, DEFAULT_MIN_POOL_SIZE, DEFAULT_MAX_POOL_SIZE, DEFAULT_WAIT_TIMEOUT, DEFAULT_MAX_IDLE_TIME);
    }

    public DBConnectionPool(String url, String username, String password, int minPoolSize, int maxPoolSize, long waitTimeoutMillis, long maxIdleTimeMillis) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.minPoolSize = minPoolSize;
        this.maxPoolSize = maxPoolSize;
        this.waitTimeout = waitTimeoutMillis;
        this.maxIdleTime = maxIdleTimeMillis;
        activeConnectionCount = new AtomicInteger(0);
        availableConnections = new ConcurrentLinkedQueue<>();
        usedConnections = new ConcurrentLinkedQueue<>();

        initializeConnections();
    }

    private void initializeConnections() {
        while (activeConnectionCount.get() < minPoolSize) {
            Connection connection = createConnection();
            activeConnectionCount.incrementAndGet();
            availableConnections.offer(new ConnectionInfo(connection));
        }
    }

    private synchronized ConnectionInfo getConnectionFromPool() {
        if (!availableConnections.isEmpty()) {
            return availableConnections.poll();
        }
        if (activeConnectionCount.get() < maxPoolSize) {
            Connection connection = createConnection();
            activeConnectionCount.incrementAndGet();
            return new ConnectionInfo(connection);
        }
        notifyAll();

        return null;
    }

    private ConnectionInfo waitForConnection(long remaining) throws InterruptedException, SQLTimeoutException {
        long start = System.currentTimeMillis();
        ConnectionInfo connectionInfo;
        while ((connectionInfo = getConnectionFromPool()) == null) {
            synchronized (this) {
                wait(remaining);
            }
            remaining = waitTimeout - (System.currentTimeMillis() - start);
            if (remaining <= 0) {
                throw new SQLTimeoutException("Timeout while waiting for a connection from the pool");
            }
        }
        return connectionInfo;
    }

    private Connection createConnection() {
        try {
            Connection connection = DriverManager.getConnection(url, username, password);
            usedConnections.offer(connection);
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException("Create connection failed", e);
        }
    }

    public Connection getConnection() throws SQLException, InterruptedException {
        ConnectionInfo connectionInfo;
        synchronized (this) {
            connectionInfo = getConnectionFromPool();
            if (connectionInfo == null) {
                connectionInfo = waitForConnection(waitTimeout);
            }
        }

        long idleTime = System.currentTimeMillis() - connectionInfo.timestamp;
        if (idleTime > maxIdleTime) {
            closeConnection(connectionInfo.connection);
            return getConnection();
        }

        Connection connection = connectionInfo.connection;
        synchronized (this) {
            usedConnections.offer(connection);
        }
        return connection;
    }

    public synchronized void releaseConnection(Connection connection) {
        usedConnections.remove(connection);
        availableConnections.offer(new ConnectionInfo(connection));
    }

    private void closeConnection(Connection connection) {
        try {
            usedConnections.remove(connection);
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("Fail to close connection", e);
        }
    }

    public synchronized void closeAllConnections() {
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