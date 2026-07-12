package com.foretti.challengevorot_server;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.core.Future;
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
        }
    }

    private void notificationTypeHandler(JsonObject notificationJson) {
        String type = notificationJson.getString("type");
        switch (type) {
            case "user_update":
                if (sessions.get(notificationJson.getString("user_id")) != null) {
                    handleGetUser(sessions.get(notificationJson.getString("user_id")));
                }
                handleRoomUserUpdate(notificationJson.getString("user_id"));
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
            case "get_genres":
                handleGetGenres(ws);
                break;
            case "spin_wheel":
                handleSpinWheel(ws);
                break;
            case "get_chat":
                handleGetChat(ws);
                break;
            case "send_message":
                handleSendMessage(ws, json);
                break;
            case "set_readiness":
                handleSetReadiness(ws, json);
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
                            .put("user_id", user_id)
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

    private void handleRoomUserUpdate(String user_id) {
        pool.preparedQuery(
                "SELECT username, avatar, genre, game, game_status, ask_to, readiness" +
                        " FROM users WHERE user_id = $1")
                .execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    Row row = rows.iterator().next();
                    System.out.println(row);

                    JsonObject user = new JsonObject();
                    user.put("user_id", user_id)
                            .put("username", row.getString("username"))
                            .put("avatar", row.getString("avatar"))
                            .put("genre", row.getString("genre"))
                            .put("game", row.getString("game"))
                            .put("game_status", row.getString("game_status"))
                            .put("ask_to", row.getString("ask_to"))
                            .put("readiness", row.getBoolean("readiness"));
                    JsonObject message = new JsonObject();

                    message.put("type", "room_user_update")
                            .put("user", user);
                    pool.preparedQuery(
                            "SELECT users FROM rooms WHERE name IN (SELECT room FROM users" +
                                    " WHERE user_id = $1)")
                            .execute(Tuple.of(user_id))
                            .onSuccess(rows_2 -> {
                                Row row_2 = rows_2.iterator().next();
                                List<String> usersList = Arrays.asList(row_2.getArrayOfStrings("users"));
                                for (String user_id_for_message : usersList) {
                                    sendToUser(user_id_for_message, message);
                                }
                            });
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
                                .put("preview_image",
                                        "https://shared.cloudflare.steamstatic.com/store_item_assets/steam/apps/"
                                                + row.getInteger("app_id") + "/header.jpg")
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

    private void handleGetGenres(ServerWebSocket ws) {
        String user_id = wsToUserId.get(ws);

        if (user_id == null) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User id is null")
                    .encode());
            return;
        }

        System.out.println("📥 Запрос жанров от: " + user_id);

        pool.preparedQuery(
                "SELECT genre FROM genres WHERE type IN (SELECT current_rotation_type FROM rooms WHERE name IN (SELECT room FROM users WHERE user_id = $1));")
                .execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    JsonArray genres = new JsonArray();
                    for (Row row : rows) {
                        JsonObject genre = new JsonObject()
                                .put("genre", row.getValue("genre"));

                        genres.add(genre);
                    }
                    sendToUser(user_id, new JsonObject()
                            .put("type", "genres_list")
                            .put("genres", genres));
                })
                .onFailure(err -> {
                    System.err.println("❌ Ошибка БД: " + err.getMessage());
                    ws.writeTextMessage(new JsonObject()
                            .put("error", "Database error")
                            .encode());
                    ws.close();
                });
    }

    private void handleSpinWheel(ServerWebSocket ws) {
        String user_id = wsToUserId.get(ws);

        if (user_id == null) {
            ws.writeTextMessage(new JsonObject()
                    .put("error", "User id is null")
                    .encode());
            return;
        }

        System.out.println("📥 Запрос на прокрутку колеса: " + user_id);

        pool.preparedQuery(
                "SELECT genre FROM genres WHERE type IN (SELECT current_rotation_type FROM rooms WHERE name IN (SELECT room FROM users WHERE user_id = $1)) ORDER BY RANDOM() LIMIT 1;")
                .execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    Row row = rows.iterator().next();

                    JsonObject genre = new JsonObject()
                            .put("type", "spinning_result")
                            .put("genre", row.getString("genre"));

                    pool.preparedQuery(
                            "CALL public.set_genre($1, $2);")
                            .execute(Tuple.of(user_id, row.getString("genre"))).onFailure(err -> {
                                System.err.println("❌ Ошибка БД: " + err.getMessage());
                                ws.writeTextMessage(new JsonObject()
                                        .put("error", "Database error")
                                        .encode());
                                ws.close();
                            });

                    sendToUser(user_id, genre);
                })
                .onFailure(err -> {

                });

    }

    private void handleGetChat(ServerWebSocket ws) {
        String user_id = wsToUserId.get(ws);
        System.out.println("📥 Запрос чата от: " + user_id);

        JsonArray chat_users = new JsonArray();
        JsonArray messages = new JsonArray();
        JsonObject lastReadMessageObj = new JsonObject();

        // 1. Создаем три асинхронных запроса (Future)
        Future<RowSet<Row>> queryUsers = pool.preparedQuery(
                "SELECT user_id, username, avatar" +
                        " FROM users WHERE user_id IN (SELECT user_id" +
                        " FROM messages WHERE room_name IN (SELECT room FROM users WHERE user_id = $1)" +
                        " ORDER BY created_at ASC LIMIT 50);")
                .execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    for (Row row : rows) {
                        JsonObject user = new JsonObject()
                                .put("user_id", row.getString("user_id"))
                                .put("username", row.getString("username"))
                                .put("avatar", row.getString("avatar"));
                        chat_users.add(user);
                    }
                });

        Future<RowSet<Row>> queryMessages = pool.preparedQuery(
                "SELECT * FROM (SELECT id, user_id, room_name, type, content, attachment_base64, created_at" +
                        " FROM messages WHERE room_name IN (SELECT room FROM users WHERE user_id = $1)" +
                        " ORDER BY created_at DESC LIMIT 50) ORDER BY created_at ASC;")
                .execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    for (Row row : rows) {
                        JsonObject message = new JsonObject()
                                .put("id", row.getInteger("id"))
                                .put("room_name", row.getString("room_name"))
                                .put("user_id", row.getString("user_id"))
                                .put("type", row.getString("type"))
                                .put("content", row.getString("content"))
                                .put("attachment_base64", row.getString("attachment_base64"))
                                .put("created_at", row.getOffsetDateTime("created_at").toString());
                        messages.add(message);
                    }
                });

        Future<RowSet<Row>> queryStatus = pool.preparedQuery(
                "SELECT last_read_message_id FROM chat_read_status" +
                        " WHERE room_name IN (SELECT room FROM users WHERE user_id = $1) AND user_id = $1")
                .execute(Tuple.of(user_id))
                .onSuccess(rows -> {
                    if (rows.iterator().hasNext()) { // Проверка на случай, если записи нет
                        Row row = rows.iterator().next();
                        lastReadMessageObj.put("last_read_message_id", row.getInteger("last_read_message_id"));
                    }
                });

        // 2. Ждем выполнения ВСЕХ трех запросов
        Future.all(queryUsers, queryMessages, queryStatus)
                .onSuccess(compositeResult -> {
                    // 3. Формируем единый финальный JSON-ответ
                    JsonObject response = new JsonObject()
                            .put("type", "chat_history")
                            .put("users", chat_users)
                            .put("messages", messages)
                            .put("last_read_message_id", lastReadMessageObj.getInteger("last_read_message_id", 0));

                    // 4. ОТПРАВЛЯЕМ СООБЩЕНИЕ ПОЛЬЗОВАТЕЛЮ ЗДЕСЬ
                    sendToUser(user_id, response);
                })
                .onFailure(err -> {
                    System.err.println("❌ Ошибка при получении данных чата: " + err.getMessage());
                    // Можно отправить пользователю сообщение об ошибке
                    ws.writeTextMessage(new JsonObject().put("error", "Failed to fetch chat data").encode());
                });
    }

    private void handleSendMessage(ServerWebSocket ws, JsonObject json) {
        String user_id = wsToUserId.get(ws);

        JsonObject message = json.getJsonObject("message");
        String type = message.getString("type");
        String content = message.getString("content");
        String attachment_base64 = message.getString("attachment_base64");

        pool.preparedQuery(
                "INSERT INTO messages (room_name, user_id, type, content, attachment_base64)" +
                        " VALUES ((SELECT room FROM users WHERE user_id = $1), $1, $2, $3, $4)" +
                        " RETURNING id, room_name, created_at")
                .execute(Tuple.of(user_id, type, content, attachment_base64))
                .onSuccess(rows -> {
                    Row row = rows.iterator().next();
                    Long id = row.getLong("id");
                    String room_name = row.getString("room_name");
                    String created_at = row.getOffsetDateTime("created_at").toString();

                    message.put("id", id)
                            .put("room_name", room_name)
                            .put("user_id", user_id)
                            .put("created_at", created_at);

                    JsonObject notify = new JsonObject()
                            .put("type", "new_message")
                            .put("message", message);

                    pool.preparedQuery(
                            "SELECT unnest(users) AS user_id FROM rooms WHERE name = $1")
                            .execute(Tuple.of(room_name))
                            .onSuccess(rows_2 -> {
                                for (Row row_2 : rows_2) {
                                    sendToUser(row_2.getString("user_id"), notify);
                                }
                            }).onFailure(err -> {
                                System.err.println("❌ Ошибка бд: " + err.getMessage());
                                ws.writeTextMessage(
                                        new JsonObject().put("error", "Failed to fetch chat data").encode());
                            });
                })
                .onFailure(err -> {
                    System.err.println("❌ Ошибка при отправки сообщения: " + err.getMessage());
                    // Можно отправить пользователю сообщение об ошибке
                    ws.writeTextMessage(new JsonObject().put("error", "Failed to fetch chat data").encode());
                });
    }

    private void handleSetReadiness(ServerWebSocket ws, JsonObject json) {
        String user_id = wsToUserId.get(ws);
        System.out.println("📥 Изменение готовности у: " + user_id);

        pool.preparedQuery(
                "UPDATE users SET readiness = $2 WHERE user_id = $1")
                .execute(Tuple.of(user_id, json.getBoolean("readiness")))
                .onFailure(err -> {
                    System.err.println("❌ Ошибка бд: " + err.getMessage());
                    ws.writeTextMessage(
                            new JsonObject().put("error", "Failed to fetch chat data").encode());
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