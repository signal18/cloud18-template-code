// Cloud18 sample Java application: says hello and proves the database path end to end.
// The template passes the connection through the environment (DB_HOST is the cluster's
// proxy, so the answer comes from the current primary and follows a switchover).
package io.signal18.hello;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class App {
    static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }

    static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&#39;");
    }

    public static void main(String[] args) throws Exception {
        String host = env("DB_HOST", "127.0.0.1"), port = env("DB_PORT", "3306"), name = env("DB_NAME", "");
        String url = "jdbc:mariadb://" + host + ":" + port + "/" + name + "?connectTimeout=3000";

        HttpServer server = HttpServer.create(new InetSocketAddress(Integer.parseInt(env("PORT", "8080"))), 0);
        server.createContext("/", exchange -> {
            int status = 200;
            String title, detail;
            try (Connection db = DriverManager.getConnection(url, env("DB_USER", ""), env("DB_PASSWORD", ""));
                 Statement st = db.createStatement();
                 ResultSet rs = st.executeQuery("SELECT @@server_id, @@hostname, VERSION()")) {
                rs.next();
                title = "Hello world, I'm connected to database server id " + rs.getLong(1);
                detail = "Java, " + rs.getString(2) + ", " + rs.getString(3) + ", schema " + name + ", through " + host + ":" + port;
            } catch (Exception err) {
                status = 503;
                title = "Hello world, I'm not connected to the database";
                detail = String.valueOf(err.getMessage());
            }
            byte[] body = ("<!DOCTYPE html>\n<html>\n  <head>\n    <meta charset=\"utf-8\">\n    <title>Cloud18 sample Java application</title>\n  </head>\n  <body>\n    <h1>"
                    + escape(title) + "</h1>\n    <p>" + escape(detail) + "</p>\n  </body>\n</html>\n").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
    }
}
