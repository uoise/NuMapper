package com.ll.simpleDb;

import java.sql.SQLException;

public class SQLRuntimeException extends RuntimeException {
    public SQLRuntimeException(String s, SQLException e) {
    }
}
