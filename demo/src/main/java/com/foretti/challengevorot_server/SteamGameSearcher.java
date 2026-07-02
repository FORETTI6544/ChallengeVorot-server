package com.foretti.challengevorot_server;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

public class SteamGameSearcher {
    private static final String STEAM_HOST = "api.steampowered.com";
    private static final int STEAM_PORT = 443;
    private static final String STEAM_PATH = "/IStoreService/GetAppList/v1/";

    private final String apiKey = "F0B7337A62C3383B2AAB6F34682BB30A";
    private final Pool pool;
    private final HttpClient httpClient;

    private int lastAppId = 0;
    private final int maxResults = 50000;
    public boolean isLoading = false;

    public SteamGameSearcher(Vertx vertx, Pool pool) {
        this.pool = pool;
        this.httpClient = vertx.createHttpClient(new HttpClientOptions()
                .setConnectTimeout(10000)
                .setIdleTimeout(30)
                .setSsl(true));
        loadLastAppId();
    }

    private void loadLastAppId() {
        pool.preparedQuery(
                "SELECT COALESCE(MAX(app_id), 0) as last_app_id FROM steam_games").execute()
                .onSuccess(rows -> {
                    Row row = rows.iterator().next();
                    this.lastAppId = row.getInteger("last_app_id");
                    System.out.println("Локальный счетчик lastAppId успешно инициализирован: " + this.lastAppId);
                    fetchAndSaveNextPage();
                }).onFailure(err -> {
                    System.err.println("Ошибка при загрузке lastAppId: " + err.getMessage());
                });
    }

    public void fetchAndSaveNextPage() {
        isLoading = true;

        // Название параметра исправлено на last_appid для корректной пагинации Steam API
        String requestURI = STEAM_PATH + "?key=" + apiKey + 
                            "&last_appid=" + lastAppId + 
                            "&max_results=" + maxResults +
                            "&include_games=true";
        
        System.out.println("Запрос к Steam для last_appid: " + lastAppId);

        httpClient.request(HttpMethod.GET, STEAM_PORT, STEAM_HOST, requestURI)
            .compose(req -> req.send())
            .onSuccess(response -> {
                if (response.statusCode() != 200) {
                    System.err.println("❌ [SteamGameSearcher] Ошибка HTTP: " + response.statusCode());
                    isLoading = false;
                    return;
                }

                // Достаем буфер ответа через встроенный механизм Vert.x HttpClientResponse
                response.body().onSuccess(buffer -> {
                    JsonObject json = buffer.toJsonObject();
                    
                    if (json == null || !json.containsKey("response")) {
                        System.out.println("Неверный формат ответа от Steam.");
                        isLoading = false;
                        return;
                    }

                    JsonArray apps = json.getJsonObject("response").getJsonArray("apps");

                    if (apps == null || apps.isEmpty()) {
                        System.out.println("Список пуст или все игры уже скачаны.");
                        isLoading = false;
                        return;
                    }

                    // Наполняем батч для базы данных
                    List<Tuple> batch = new ArrayList<>();
                    for (int i = 0; i < apps.size(); i++) {
                        JsonObject app = apps.getJsonObject(i);
                        int appId = app.getInteger("appid");
                        String name = app.getString("name");

                        batch.add(Tuple.of(appId, name));

                        // Запоминаем последний ID для следующего шага
                        if (appId > lastAppId) {
                            lastAppId = appId;
                        }
                    }

                    // Сохраняем всю пачку в Postgres одной командой
                    pool.preparedQuery("INSERT INTO steam_games (app_id, game_name) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                            .executeBatch(batch)
                            .onSuccess(rows -> {
                                System.out.println("Успешно сохранено игр: " + apps.size());
                                isLoading = false;
                            })
                            .onFailure(err -> {
                                System.err.println("Ошибка записи в БД: " + err.getMessage());
                                isLoading = false;
                            });
                }).onFailure(err -> {
                    System.err.println("Ошибка чтения тела ответа: " + err.getMessage());
                    isLoading = false;
                });
            })
            .onFailure(err -> {
                System.err.println("Ошибка сети/Steam: " + err.getMessage());
                isLoading = false;
            });
    }
}
