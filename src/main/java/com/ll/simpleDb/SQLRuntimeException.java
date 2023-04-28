package com.ll.simpleDb;

import java.sql.SQLException;

public class SQLRuntimeException extends RuntimeException {
    public SQLRuntimeException(String s) {
        System.out.println(s);
    }

    public SQLRuntimeException(String s, SQLException e) {
        System.out.println(s);
        e.printStackTrace();
    }

    public SQLRuntimeException(String s, InterruptedException e) {
        System.out.println(s);
        e.printStackTrace();
    }
}
