package com.ll.simpleDb;

public class SQLRuntimeException extends RuntimeException {
    public SQLRuntimeException(String s) {
        System.out.println(s);
    }

    public SQLRuntimeException(String s, Exception e) {
        System.out.println(s);
        e.printStackTrace();
    }
}
