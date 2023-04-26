package com.ll.simpleDb;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class Sql implements AutoCloseable {
    private final DBConnectionPool dbConnectionPool;
    private Connection connection;
    private final StringBuilder queryString;
    private PreparedStatement stmt;
    private ResultSet rs;

    private Sql(DBConnectionPool dbConnectionPool) {
        this.dbConnectionPool = dbConnectionPool;
        this.queryString = new StringBuilder();
    }

    private void setConnection() {
        connection = dbConnectionPool.get();
    }

    private PreparedStatement getStmt() throws SQLException {
        setConnection();
        return connection.prepareStatement(queryString.toString().trim());
    }

    private PreparedStatement getStmt(final int statementConstant) throws SQLException {
        setConnection();
        return connection.prepareStatement(queryString.toString().trim(), statementConstant);
    }

    @Override
    public void close() {
        try {
            if (stmt != null) {
                stmt.close();
            }
            if (rs != null) {
                rs.close();
            }
            if (connection != null) {
                dbConnectionPool.release(connection);
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException("Error closing Sql object", e);
        }
    }

    public static Sql of(DBConnectionPool dbConnectionPool) {
        return new Sql(dbConnectionPool);
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
        try {
            stmt = getStmt(Statement.RETURN_GENERATED_KEYS);
            stmt.executeUpdate();
            rs = stmt.getGeneratedKeys();
            if (rs.next()) return rs.getInt(1);
            return -1;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    public long update() {
        try {
            stmt = getStmt();
            return stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    public long delete() {
        try {
            stmt = getStmt();
            return stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    public LocalDateTime selectDatetime() {
        try {
            stmt = getStmt();
            rs = stmt.executeQuery();
            return rs.next() ? rs.getTimestamp(1).toLocalDateTime() : null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public long selectLong() {
        try {
            stmt = getStmt();
            rs = stmt.executeQuery();
            return rs.next() ? rs.getLong(1) : -1;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    public String selectString() {
        try {
            stmt = getStmt();
            rs = stmt.executeQuery();
            return rs.next() ? rs.getString(1) : null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Map<String, Object> selectRow() {
        Map<String, Object> ret = new HashMap<>();
        try {
            stmt = getStmt();
            rs = stmt.executeQuery();
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
        }
    }

    public <R> R selectRow(Class<R> clazz) {
        try {
            Constructor<?> constructor = clazz.getConstructor();
            R ret = (R) constructor.newInstance();
            stmt = getStmt();
            rs = stmt.executeQuery();
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
        }
    }

    public <R> List<R> selectRows(Class<R> clazz) {
        List<R> ret = new ArrayList<>();
        try {
            Constructor<?> constructor = clazz.getConstructor();
            stmt = getStmt();
            rs = stmt.executeQuery();
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
        }
    }

    public List<Long> selectLongs() {
        List<Long> ret = new ArrayList<>();
        try {
            stmt = getStmt();
            rs = stmt.executeQuery();
            while (rs.next()) ret.add(rs.getLong(1));
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }
}
