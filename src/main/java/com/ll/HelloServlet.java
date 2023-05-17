package com.ll;

import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;

@WebServlet("/usr/hello")
public class HelloServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
        try {
            Person p = new Person(1, "ex");
            resp.getWriter()
                    .append("Hello, Servlet!!!!!!!")
                    .append("\n")
                    .append(p.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
        try {
            resp.getWriter().append("Hello, Servlet!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}


@AllArgsConstructor
@ToString
@Getter
class Person {
    private int id;
    private String name;
}