package com.foretti.challengevorot_server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgNotification;
import io.vertx.sqlclient.Tuple;

public class WebSocketManager {
    private final Pool pool;
    public final SteamGameSearcher gameSearcher;

    private final Map<String, ServerWebSocket> sessions = new ConcurrentHashMap<>();
    private final Map<ServerWebSocket, String> wsToUserId = new ConcurrentHashMap<>();
    private final Map<ServerWebSocket, Boolean> pendingAuth = new ConcurrentHashMap<>();

    public WebSocketManager(Pool pool, SteamGameSearcher gameSearcher) {
        this.pool = pool;
        this.gameSearcher = gameSearcher;
    }

    public void handle(ServerWebSocket ws) {
        System.out.println("🔌 Новый WebSocket клиент подключился");
        pendingAuth.put(ws, true);

        ws.textMessageHandler(msg -> {
            System.out.println("📩 Получено сообщение: " + msg);
            try {
                JsonObject json = new JsonObject(msg);
                typeHandler(ws, json);
            } catch (Exception e) {
                ws.writeTextMessage(new JsonObject()
                        .put("error", "Invalid JSON: " + e.getMessage())
                        .encode());
            }
        });

        ws.closeHandler(v -> {
            System.out.println("🔌 Клиент отключился");
            sessions.remove(wsToUserId.get(ws));
        });
    }

    public void startListening() {
        pool.getConnection().onSuccess(conn -> {
            System.out.println("✅ Подключение к БД для LISTEN установлено");
            PgConnection pgConn = PgConnection.cast(conn);
            pgConn.notificationHandler(notification -> {
                handleNotification(notification);
            });
            pgConn.query("LISTEN notifyer").execute()
                    .onSuccess(result -> {
                        System.out.println("👂 Подписка на канал 'notifyer' успешна");
                    })
                    .onFailure(err -> {
                        System.err.println("❌ Ошибка LISTEN: " + err.getMessage());
                        pgConn.close();
                    });

        }).onFailure(err -> {
            System.err.println("❌ Не удалось получить соединение: " + err.getMessage());
        });

    }

    private void handleNotification(PgNotification notification) {
        try {
            String payload = notification.getPayload();
            String channel = notification.getChannel();
            int processId = notification.getProcessId();

            System.out.println("📨 Получено уведомление от БД:");
            System.out.println("  Канал: " + channel);
            System.out.println("  Process ID: " + processId);
            System.out.println("  Payload: " + payload);

            if (payload == null || payload.isEmpty()) {
                System.out.println("⚠️ Пустой payload в уведомлении");
                return;
            }

            JsonObject notificationJson = new JsonObject(payload);
            notificationTypeHandler(notificationJson);

        } catch (Exception e) {
            System.err.println("❌ Ошибка обработки уведомления: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void notificationTypeHandler(JsonObject notificationJson) {
        String type = notificationJson.getString("type");
        switch (type) {
            case "user_update":
                handleGetUser(sessions.get(notificationJson.getString("user_id")));
                break;

        }
    }

    private void typeHandler(ServerWebSocket ws, JsonObject json) {
        String type = json.getString("type");
        switch (type) {
            case "auth":
                handleAuth(ws, json);
                break;
            case "get_user":
                handleGetUser(ws);
                break;
            case "get_rooms":
                handleGetRooms(ws);
                break;
            case "set_name":
                String new_name = json.getString("name");
                handleSetName(ws, new_name);
                break;
            case "set_avatar":
                String new_avatar = json.getString("base64");
                handleSetAvatar(ws, new_avatar);
                break;
            case "get_users":
                handleGetUsers(ws);
                break;
            case "get_rotation_status":
                handleGetRotationStatus(ws);
                break;
            case "search_games":
                handleSearchGames(ws, json);
                break;
            default:
                ws.writeTextMessage(new JsonObject()
                        .put("error", "Unknown message type: " + type)
                        .encode());
        }
    }

    private void handleAuth(ServerWebSocket ws, JsonObject json) {
        String user_id = json.getString("user_id");

        if (user_id == null || user_id.isEmpty()) {
            ws.writeTextMessage("{\"error\":\"Missing user id\"}");
            ws.close();
            return;
        }
        if (sessions.containsKey(user_id)) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User already connected")
                    .encode());
            ws.close();
            return;
        }

        pool.preparedQuery(
                "SELECT username FROM users WHERE user_id = $1").execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    if (rows.size() == 0) {
                        ws.writeTextMessage(new JsonObject()
                                .put("error", "Missing user_id")
                                .encode());
                        ws.close();
                        return;
                    }

                    pendingAuth.remove(ws);
                    sessions.put(user_id, ws);
                    wsToUserId.put(ws, user_id);

                    JsonObject response = new JsonObject()
                            .put("type", "auth")
                            .put("status", "auth_success")
                            .put("user_id", user_id)
                            .put("message", "Authentication successful");
                    ws.writeTextMessage(response.encode());

                    System.out.println("✅ Пользователь " + user_id + " аутентифицирован");

                }).onFailure(err -> {
                    System.err.println("❌ Ошибка БД: " + err.getMessage());
                    ws.writeTextMessage(new JsonObject()
                            .put("error", "Database error")
                            .encode());
                    ws.close();
                });
    }

    private void handleGetUser(ServerWebSocket ws) {
        String user_id = wsToUserId.get(ws);

        if (user_id == null) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User id is null")
                    .encode());
            return;
        }

        System.out.println("📥 Запрос данных пользователя: " + user_id);

        pool.preparedQuery(
                "SELECT username, avatar, balance, ask_to, readiness, genre," +
                        " game, game_cover, game_status, game_started_date, allow_wheel_spinning," +
                        " rerolls_count FROM users WHERE user_id = $1")
                .execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    Row row = rows.iterator().next();

                    JsonObject user = new JsonObject()
                            .put("type", "user_update")
                            .put("username", row.getString("username"))
                            .put("avatar", row.getString("avatar"))
                            .put("balance", row.getInteger("balance"))
                            .put("ask_to", row.getString("ask_to"))
                            .put("readiness", row.getBoolean("readiness"))
                            .put("genre", row.getString("genre"))
                            .put("game", row.getString("game"))
                            .put("game_cover", row.getString("game_cover"))
                            .put("game_status", row.getString("game_status"))
                            .put("game_started_date", row.getInteger("game_started_date"))
                            .put("allow_wheel_spinning", row.getBoolean("allow_wheel_spinning"))
                            .put("rerolls_count", row.getInteger("rerolls_count"));

                    sendToUser(user_id, user);
                })
                .onFailure(err -> {

                });

    }

    private void handleGetRooms(ServerWebSocket ws) {
        String user_id = wsToUserId.get(ws);

        if (user_id == null) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User id is null")
                    .encode());
            return;
        }

        System.out.println("📥 Запрос списка комнат от: " + user_id);

        pool.preparedQuery(
                "SELECT name, cardinality(users) as users_count, rotation_status FROM rooms").execute()
                .onSuccess(rows -> {
                    JsonObject rooms = new JsonObject();
                    rooms.put("type", "rooms_list");
                    JsonArray rooms_list = new JsonArray();

                    for (Row row : rows) {
                        JsonObject room = new JsonObject()
                                .put("name", row.getString("name"))
                                .put("users_count", row.getInteger("users_count"))
                                .put("rotation_status", row.getBoolean("rotation_status"));
                        rooms_list.add(room);
                    }
                    rooms.put("rooms", rooms_list);
                    sendToUser(user_id, rooms);
                })
                .onFailure(err -> {
                    err.printStackTrace();
                });

    }

    private void handleSetName(ServerWebSocket ws, String new_name) {
        String user_id = wsToUserId.get(ws);

        if (user_id == null) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User id is null")
                    .encode());
            return;
        }

        System.out.println("📥 Запрос на смену ника от: " + user_id);

        pool.preparedQuery(
                "UPDATE users SET username = $2 WHERE user_id = $1")
                .execute(Tuple.of(user_id, new_name))
                .onSuccess(rows -> {

                })
                .onFailure(err -> {
                    err.printStackTrace();
                });
    }

    private void handleSetAvatar(ServerWebSocket ws, String new_avatar) {
        String user_id = wsToUserId.get(ws);

        if (user_id == null) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User id is null")
                    .encode());
            return;
        }

        System.out.println("📥 Запрос на смену аватара от: " + user_id);

        pool.preparedQuery(
                "UPDATE users SET avatar = $2 WHERE user_id = $1")
                .execute(Tuple.of(user_id, new_avatar))
                .onSuccess(rows -> {

                })
                .onFailure(err -> {
                    err.printStackTrace();
                });
    }

    private void handleGetUsers(ServerWebSocket ws) {
        String user_id = wsToUserId.get(ws);

        if (user_id == null) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User id is null")
                    .encode());
            return;
        }

        System.out.println("📥 Запрос данных пользователей от: " + user_id);

        pool.preparedQuery(
                "SELECT username, avatar, genre," +
                        " game, game_status, user_id," +
                        " ask_to, readiness FROM users WHERE room IN" +
                        " (SELECT room FROM users WHERE user_id = $1);")
                .execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    JsonArray users = new JsonArray();
                    for (Row row : rows) {
                        JsonObject user = new JsonObject()
                                .put("username", row.getString("username"))
                                .put("avatar", row.getString("avatar"))
                                .put("genre", row.getString("genre"))
                                .put("game", row.getString("game"))
                                .put("game_status", row.getString("game_status"))
                                .put("user_id", row.getString("user_id"))
                                .put("ask_to", row.getString("ask_to"))
                                .put("readiness", row.getBoolean("readiness"));
                        users.add(user);
                    }
                    sendToUser(user_id, new JsonObject()
                            .put("type", "users_list")
                            .put("users", users));
                })
                .onFailure(err -> {

                });
    }

    private void handleGetRotationStatus(ServerWebSocket ws) {
        String user_id = wsToUserId.get(ws);

        if (user_id == null) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User id is null")
                    .encode());
            return;
        }

        System.out.println("📥 Запрос данных статуса ротации: " + user_id);

        pool.preparedQuery(
                "SELECT rotation_status FROM rooms WHERE name IN (SELECT room FROM users WHERE user_id = $1);")
                .execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    Row row = rows.iterator().next();

                    JsonObject roomData = new JsonObject()
                            .put("type", "room_update")
                            .put("rotation_status", row.getBoolean("rotation_status"));

                    sendToUser(user_id, roomData);
                })
                .onFailure(err -> {
                    System.err.println("❌ Ошибка БД: " + err.getMessage());
                    ws.writeTextMessage(new JsonObject()
                            .put("error", "Database error")
                            .encode());
                });
    }

    private void handleSearchGames(ServerWebSocket ws, JsonObject json) {
        String user_id = wsToUserId.get(ws);
        String query = json.getString("query");

        if (user_id == null) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User id is null")
                    .encode());
            return;
        }

        System.out.println("📥 Запрос на поиск игры от пользователя: " + user_id);

        pool.preparedQuery(
                "SELECT app_id, game_name FROM steam_games WHERE LOWER(game_name) LIKE LOWER('%' || $1 || '%') LIMIT 50")
                .execute(Tuple.of(query))
                .onSuccess(rows -> {
                    JsonArray games = new JsonArray();
                    for (Row row : rows) {
                        JsonObject game = new JsonObject()
                                .put("preview_image", "https://shared.cloudflare.steamstatic.com/store_item_assets/steam/apps/" + row.getInteger("app_id") + "/header.jpg")
                                .put("app_id", row.getInteger("app_id"))
                                .put("name", row.getString("game_name"));
                        games.add(game);
                    }
                    sendToUser(user_id, new JsonObject()
                            .put("type", "game_search_result")
                            .put("games", games));
                })
                .onFailure(err -> {

                });

    }

    public void sendToUser(String userId, JsonObject message) {
        ServerWebSocket ws = sessions.get(userId);
        if (ws != null) {
            ws.writeTextMessage(message.encode());
            // System.out.println("✅ Отправка: " + message + " пользователю " + userId);

        } else {
            System.out.println("⚠️ Пользователь " + userId + " не в сети");
        }
    }

    public void broadcast(JsonObject message) {
        String encoded = message.encode();
        sessions.values().forEach(ws -> ws.writeTextMessage(encoded));
    }
}