package company.vk.edu.distrib.compute.v11qfour.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.v11qfour.cluster.V11qfourNode;
import company.vk.edu.distrib.compute.v11qfour.cluster.V11qfourRoutingStrategy;
import company.vk.edu.distrib.compute.v11qfour.proxy.V11qfourProxyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executors;

public class V11qfourKVServiceFactory implements KVService {
    private static final Logger log = LoggerFactory.getLogger(V11qfourKVServiceFactory.class);
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;
    private HttpServer server;
    private final InetSocketAddress address;
    private final Dao<byte[]> dao;
    private final String selfUrl;
    private final V11qfourProxyClient proxyClient;
    private final V11qfourRoutingStrategy routingStrategy;
    private final List<V11qfourNode> clusterNodes;

    public V11qfourKVServiceFactory(int port,
                                    Dao<byte[]> dao,
                                    V11qfourRoutingStrategy routingStrategy,
                                    List<V11qfourNode> clusterNodes,
                                    String selfUrl,
                                    V11qfourProxyClient proxyClient) {
        this.dao = dao;
        this.routingStrategy = routingStrategy;
        this.clusterNodes = clusterNodes;
        this.selfUrl = selfUrl;
        this.proxyClient = proxyClient;
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
        String id = validateId(query);
        if (id == null) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }
        V11qfourNode responsibleNode = routingStrategy.getResponsibleNode(id, clusterNodes);
        if (responsibleNode.url().equals(selfUrl)) {
            switch (exchange.getRequestMethod()) {
                case "GET" -> handleGet(exchange, id);
                case "PUT" -> handlePut(exchange, id);
                case "DELETE" -> handleDelete(exchange, id);
                default -> exchange.sendResponseHeaders(405, -1);
            }
        } else {
            proxyClient.proxy(exchange, responsibleNode);
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

    private String validateId(String query) throws IOException {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("id=")) {
                String val = param.substring(3);
                return val.isEmpty() ? null : val;
            }
        }
        return null;
    }

    private InetSocketAddress createInetSocketAddress(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            log.error("Port must be in range 1 to 65535");
            throw new IllegalArgumentException("Port must be in range 1 to 65535");
        }
        return new InetSocketAddress(port);
    }
}
