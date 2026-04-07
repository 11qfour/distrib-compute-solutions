package company.vk.edu.distrib.compute.v11qfour;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class V11qfourKVServiceFactory implements KVService {
    private static final Logger log = LoggerFactory.getLogger(V11qfourKVServiceFactory.class);
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;
    private HttpServer server;
    private final InetSocketAddress address;
    private final Dao<byte[]> dao;

    public V11qfourKVServiceFactory(int port, Dao<byte[]> dao) {
        this.dao = dao;
        this.address = createInetSocketAddress(port);
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(address, 0);
            server.createContext("/v0/status", exchange -> {
                try (exchange) {
                    exchange.sendResponseHeaders(200, -1);
                } catch (IOException e) {
                    log.error("Status error", e);
                }
            });
            server.createContext("/v0/entity", exchange -> {
                try (exchange) {
                    handleEntityRequest(exchange);
                } catch (IOException e) {
                    log.error("Entity error", e);
                }
            });
            server.start();
        } catch (IOException exception) {
            log.error("Server is failed to start jn {}", address, exception);
            throw new UncheckedIOException("Server is failed to start", exception);
        }
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void handleEntityRequest(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        if (query == null || !query.contains("id=")) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }
        String id = validateId(exchange, query);
        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handlePut(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] value = dao.get(id);
            exchange.sendResponseHeaders(200, value.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value);
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try (var is = exchange.getRequestBody()) {
            dao.upsert(id, is.readAllBytes());
        }
        exchange.sendResponseHeaders(201, -1);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, -1);
    }

    private String validateId(HttpExchange exchange, String query) throws IOException {
        String id = null;
        for (String param : query.split("&")) {
            if (param.startsWith("id=")) {
                id = param.substring(3);
                break;
            }
        }
        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
        }
        return id;
    }

    private InetSocketAddress createInetSocketAddress(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            log.error("Port must be in range 1 to 65535");
            throw new IllegalArgumentException("Port must be in range 1 to 65535");
        }
        return new InetSocketAddress(port);
    }
}
