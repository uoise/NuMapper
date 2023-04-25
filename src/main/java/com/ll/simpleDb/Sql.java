package com.ll.simpleDb;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class Sql {
    private final Connection connection;
    private final StringBuilder queryString;
    private PreparedStatement pstmt;

    private Sql(Connection connection) {
        this.connection = connection;
        this.queryString = new StringBuilder();
    }

    private Sql(Sql sql) {
        this.connection = sql.connection;
        this.queryString = sql.queryString;
    }

    static Sql of(Connection connection) {
        return new Sql(connection);
    }

    static Sql of(Sql sql) {
        return new Sql(sql);
    }

    public Sql append(String rawSql) {
        this.queryString.append(' ').append(rawSql.trim());
        return this;
    }

    public Sql append(String rawSql, Object arg) {
        String mappedSql = rawSql.trim().replace("?", "'" + arg + "'");
        this.queryString.append(' ').append(mappedSql);
        return this;
    }

    public Sql append(String rawSql, Object... args) {
        String mappedSql = String.format(rawSql.trim().replace("?", "'%s'"), args);
        this.queryString.append(' ').append(mappedSql);
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
        System.out.println(queryString);
        try {
            pstmt = connection.prepareStatement(queryString.toString().trim(), Statement.RETURN_GENERATED_KEYS);
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) return rs.getInt(1);
            return -1;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public long update() {
        try {
            pstmt = connection.prepareStatement(queryString.toString().trim());
            pstmt.executeUpdate();
            return pstmt.getUpdateCount();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public long delete() {
        try {
            pstmt = connection.prepareStatement(queryString.toString().trim());
            pstmt.executeUpdate();
            return pstmt.getUpdateCount();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public LocalDateTime selectDatetime() {
        try {
            pstmt = connection.prepareStatement(queryString.toString().trim());
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) return rs.getTimestamp(1).toLocalDateTime();
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Long selectLong() {
        try {
            pstmt = connection.prepareStatement(queryString.toString().trim());
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) return rs.getLong(1);
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String selectString() {
        try {
            pstmt = connection.prepareStatement(queryString.toString().trim());
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) return rs.getString(1);
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Map<String, Object> selectRow() {
        Map<String, Object> ret = new HashMap<>();
        try {
            pstmt = connection.prepareStatement(queryString.toString().trim());
            ResultSet rs = pstmt.executeQuery();
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
            return ret;
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public <R> R selectRow(Class<R> clazz) {
        try {
            Constructor<?> constructor = Arrays.stream(clazz.getConstructors()).findFirst().orElse(null);
            R tmp = (R) constructor.newInstance();
            pstmt = connection.prepareStatement(queryString.toString().trim());
            ResultSet rs = pstmt.executeQuery();
            Field[] fields = clazz.getDeclaredFields();
            while (rs.next()) {
                for (Field f : fields) {
                    f.setAccessible(true);
                    System.out.println(rs.getObject(f.getName()));
                    f.set(tmp, rs.getObject(f.getName()));
                }
            }
            return tmp;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public <R> List<R> selectRows(Class<R> clazz) {
        List<R> ret = new ArrayList<>();
        try {
            Constructor<?> constructor = Arrays.stream(clazz.getConstructors()).findFirst().orElse(null);
            pstmt = connection.prepareStatement(queryString.toString().trim());
            ResultSet rs = pstmt.executeQuery();
            Field[] fields = clazz.getDeclaredFields();
            while (rs.next()) {
                R tmp = (R) constructor.newInstance();
                for (Field f : fields) {
                    f.setAccessible(true);
                    System.out.println(rs.getObject(f.getName()));
                    f.set(tmp, rs.getObject(f.getName()));
                }
                ret.add(tmp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ret;
    }

    public List<Long> selectLongs() {
        List<Long> ret = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(queryString.toString().trim());
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) ret.add(rs.getLong(1));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ret;
    }
}
