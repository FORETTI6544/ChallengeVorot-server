package com.foretti.challengevorot_server;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;

public class Main {

    private static Pool pool;
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        // 1. Подключаемся к БД
        connectToDatabase(vertx);

        // 3. Настраиваем REST API
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        Auth auth = new Auth(pool);
        router.post("/posts/register").handler(auth::register);
        router.post("/posts/login").handler(auth::login);

        // 4. Настраиваем WebSocket

        // Создаем SteamGameSearcher
        SteamGameSearcher searcher = new SteamGameSearcher(vertx, pool);

        WebSocketManager wsManager = new WebSocketManager(pool, searcher);
        wsManager.startListening();

        // 5. Запускаем сервер
        HttpServerOptions options = new HttpServerOptions()
                .setMaxWebSocketFrameSize(10 * 1024 * 1024);

        HttpServer server = vertx.createHttpServer(options);

        server.requestHandler(request -> {
            // Если это WebSocket — пропускаем, обрабатывается отдельно
            String upgrade = request.headers().get("Upgrade");
            if (upgrade != null && upgrade.equalsIgnoreCase("websocket")) {
                return;
            }
            router.handle(request);
        });

        server.webSocketHandler(wsManager::handle);

        server.listen(8888, res -> {
            if (res.succeeded()) {
                System.out.println("🌐 Сервер запущен");
            } else {
                System.err.println("❌ Ошибка: " + res.cause().getMessage());
            }
        });
    }

    private static void connectToDatabase(Vertx vertx) {
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(5432)
                .setHost("localhost")
                .setDatabase("ChallengeVorot")
                .setUser("postgres")
                .setPassword("postgres");

        PoolOptions poolOptions = new PoolOptions().setMaxSize(10);
        pool = Pool.pool(vertx, connectOptions, poolOptions);
        System.out.println("✅ Подключение к БД установлено");
    }

    public static Pool getPool() {
        return pool;
    }
}