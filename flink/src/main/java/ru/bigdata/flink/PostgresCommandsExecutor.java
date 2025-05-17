package ru.bigdata.flink;

import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@AllArgsConstructor
public class PostgresCommandsExecutor {

    private final String url;
    private final String user;
    private final String password;

    public void executeSqlQuery(String sql) {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Успешное выполнение sql команды.");
        } catch (Exception e) {
            System.err.println("Ошибка при создании таблицы: " + e.getMessage());
        }
    }

    public static Connection getConnection() throws SQLException {
        final String jdbcUrl = "jdbc:postgresql://localhost:5438/db";
        final String jdbcUser = "user";
        final String jdbcPassword = "password";
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
    }
}
