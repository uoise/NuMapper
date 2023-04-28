package com.ll.simpleDb;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class Sql {
    private final DBConnectionPool dbConnectionPool;
    private final int queryTimeout;
    private final boolean devMode;
    private Connection connection;
    private final StringBuilder queryString;

    private Sql(DBConnectionPool dbConnectionPool, int queryTimeout, boolean devMode) {
        this.dbConnectionPool = dbConnectionPool;
        this.queryTimeout = queryTimeout;
        this.queryString = new StringBuilder();
        this.devMode = devMode;
    }

    private void setConnection() {
        try {
            connection = dbConnectionPool.get();
        } catch (SQLException | InterruptedException e) {
            throw new SQLRuntimeException("setConnection fail", e);
        }
    }

    private PreparedStatement getStmt() {
        if (devMode) System.out.println(queryString.toString().trim());
        try {
            setConnection();
            PreparedStatement pStmt = connection.prepareStatement(queryString.toString().trim());
            pStmt.setQueryTimeout(queryTimeout);
            return pStmt;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private PreparedStatement getStmt(final int statementConstant) {
        if (devMode) System.out.println(queryString.toString().trim());
        try {
            setConnection();
            PreparedStatement pStmt = connection.prepareStatement(queryString.toString().trim(), statementConstant);
            pStmt.setQueryTimeout(queryTimeout);
            return pStmt;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Sql of(DBConnectionPool dbConnectionPool, int queryTimeout, boolean devMode) {
        return new Sql(dbConnectionPool, queryTimeout, devMode);
    }

    public Sql append(String rawSql, Object... args) {
        for (Object o : args) rawSql = rawSql.replaceFirst("[?]", "'" + o + "'");
        this.queryString.append(' ').append(rawSql);
        return this;
    }

    public Sql appendIn(String rawSql, Collection<?> args) {
        String mappedSql = rawSql.trim().replace("?", args
                .stream()
                .map(o -> String.format("'%s'", o))
                .collect(Collectors.joining(", ")));
        this.queryString.append(' ').append(mappedSql);
        return this;
    }

    public long insert() {
        try (PreparedStatement stmt = getStmt(Statement.RETURN_GENERATED_KEYS)) {
            stmt.executeUpdate();
            ResultSet rs = stmt.getGeneratedKeys();
            return rs.next() ? rs.getInt(1) : -1;
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public long update() {
        try (PreparedStatement stmt = getStmt()) {
            return stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public long delete() {
        try (PreparedStatement stmt = getStmt()) {
            return stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public LocalDateTime selectDatetime() {
        try (PreparedStatement stmt = getStmt()) {
            ResultSet rs = stmt.executeQuery();
            return rs.next() ? rs.getTimestamp(1).toLocalDateTime() : null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public long selectLong() {
        try (PreparedStatement stmt = getStmt()) {
            ResultSet rs = stmt.executeQuery();
            return rs.next() ? rs.getLong(1) : -1;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public String selectString() {
        try (PreparedStatement stmt = getStmt()) {
            ResultSet rs = stmt.executeQuery();
            return rs.next() ? rs.getString(1) : null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public Map<String, Object> selectRow() {
        Map<String, Object> ret = new HashMap<>();
        try (PreparedStatement stmt = getStmt()) {
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String column = metaData.getColumnName(i + 1);
                    ret.put(column, rs.getObject(column));
                }
            }
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyMap();
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public <R> R selectRow(Class<R> clazz) {
        try (PreparedStatement stmt = getStmt()) {
            Constructor<?> constructor = clazz.getConstructor();
            R ret = (R) constructor.newInstance();
            ResultSet rs = stmt.executeQuery();
            Field[] fields = clazz.getDeclaredFields();
            while (rs.next()) {
                for (Field f : fields) {
                    f.setAccessible(true);
                    f.set(ret, rs.getObject(f.getName()));
                }
            }
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public <R> List<R> selectRows(Class<R> clazz) {
        List<R> ret = new ArrayList<>();
        try (PreparedStatement stmt = getStmt()) {
            Constructor<?> constructor = clazz.getConstructor();
            ResultSet rs = stmt.executeQuery();
            Field[] fields = clazz.getDeclaredFields();
            while (rs.next()) {
                R obj = (R) constructor.newInstance();
                for (Field f : fields) {
                    f.setAccessible(true);
                    f.set(obj, rs.getObject(f.getName()));
                }
                ret.add(obj);
            }
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        } finally {
            dbConnectionPool.release(connection);
        }
    }

    public List<Long> selectLongs() {
        List<Long> ret = new ArrayList<>();
        try (PreparedStatement stmt = getStmt()) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) ret.add(rs.getLong(1));
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        } finally {
            dbConnectionPool.release(connection);
        }
    }
}
