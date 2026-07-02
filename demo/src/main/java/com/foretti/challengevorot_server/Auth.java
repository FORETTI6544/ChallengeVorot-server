package com.foretti.challengevorot_server;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.Row;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

public class Auth {
    private final Pool pool;

    public Auth(Pool pool) {
        this.pool = pool;
    }

    // 📝 POST /posts/register
    public void register(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();

        // Проверяем поля
        if (body == null || !body.containsKey("username") || !body.containsKey("email") || !body.containsKey("password")) {
            sendError(ctx, 400, "Missing fields: username, email, password");
            return;
        }

        String username = body.getString("username").trim();
        String email = body.getString("email").trim();
        String password = body.getString("password");

        if (password.length() < 6) {
            sendError(ctx, 400, "Password must be at least 6 characters");
            return;
        }

        String passwordHash = hashPassword(password);

        pool.preparedQuery(
            "INSERT INTO users (username, email, password_hash) VALUES ($1, $2, $3) RETURNING user_id"
        ).execute(Tuple.of(username, email, passwordHash))
        .onSuccess(rows -> {
            Row row = rows.iterator().next();
            JsonObject response = new JsonObject()
                .put("success", true)
                .put("message", "User registered")
                .put("user", new JsonObject()
                    .put("user_id", row.getString("user_id"))
                    .put("username", username)
                    .put("email", email)
                );
            ctx.response()
                .setStatusCode(201)
                .putHeader("Content-Type", "application/json")
                .end(response.encode());
        })
        .onFailure(err -> {
            String msg = err.getMessage();
            if (msg.contains("duplicate key") && msg.contains("username")) {
                sendError(ctx, 400, "Username already exists");
            } else if (msg.contains("duplicate key") && msg.contains("email")) {
                sendError(ctx, 400, "Email already exists");
            } else {
                sendError(ctx, 500, "Database error");
            }
        });
    }

    // 🔑 POST /posts/login
    public void login(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();

        if (body == null || !body.containsKey("user") || !body.containsKey("password")) {
            sendError(ctx, 400, "Missing fields: user, password");
            return;
        }

        String username = body.getString("user").trim();
        String password = body.getString("password");

        pool.preparedQuery(
            "SELECT user_id, room IS NOT NULL AS in_room FROM users " +
               "WHERE (username = $1 OR email = $1) AND password_hash = $2"
        ).execute(Tuple.of(username, hashPassword(password)))
        .onSuccess(rows -> {
            if (rows.size() == 0) {
                sendError(ctx, 401, "Invalid username or password");
                return;
            }

            Row row = rows.iterator().next();

            // ✅ Успешный вход
            JsonObject response = new JsonObject()
                .put("user_id", row.getString("user_id"))
                .put("in_room", row.getBoolean("in_room"));
            ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(response.encode());
        })
        .onFailure(err -> {
            sendError(ctx, 500, "Database error");
        });
    }

    // 🛠 Вспомогательные методы
    private void sendError(RoutingContext ctx, int code, String message) {
        ctx.response()
            .setStatusCode(code)
            .putHeader("Content-Type", "application/json")
            .end(new JsonObject().put("error", message).encode());
    }

    private String hashPassword(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (Exception e) {
            throw new RuntimeException("Hash error", e);
        }
    }
}