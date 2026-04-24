package company.vk.edu.distrib.compute.v11qfour.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.v11qfour.cluster.V11qfourNode;
import company.vk.edu.distrib.compute.v11qfour.cluster.V11qfourRoutingStrategy;
import company.vk.edu.distrib.compute.v11qfour.proxy.V11qfourProxyClient;
import company.vk.edu.distrib.compute.v11qfour.replica.V11qfourReplicator;
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
    private final int amountN;
    private final V11qfourReplicator replicator;

    public V11qfourKVServiceFactory(int port,
                                    Dao<byte[]> dao,
                                    V11qfourRoutingStrategy routingStrategy,
                                    List<V11qfourNode> clusterNodes,
                                    String selfUrl,
                                    V11qfourProxyClient proxyClient,
                                    int amountN,
                                    V11qfourReplicator replicator) {
        this.dao = dao;
        this.routingStrategy = routingStrategy;
        this.clusterNodes = clusterNodes;
        this.selfUrl = selfUrl;
        this.proxyClient = proxyClient;
        this.address = createInetSocketAddress(port);
        this.amountN = amountN;
        this.replicator = replicator;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(address, 0);
            server.setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4));
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
            log.info("Service started on port {} with amountN = {}", address.getPort(), amountN);
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
        int ack = parseAck(query);

        if (id == null) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }
        List<V11qfourNode> targetNodes = routingStrategy.getResponsibleNodes(id, clusterNodes, amountN);

        if (ack > amountN) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        boolean isReplica = targetNodes.stream().anyMatch(node -> node.url().equals(selfUrl));

        if (isReplica) {
            handleWithReplication(exchange, id, targetNodes, ack);
        } else {
            proxyClient.proxy(exchange, targetNodes.get(0));
        }
    }

    private void handleWithReplication(HttpExchange exchange, String id,
                                       List<V11qfourNode> targetNodes, int ack) throws IOException {
        String method = exchange.getRequestMethod();
        byte[] body = null;
        if ("PUT".equals(method)) {
            body = exchange.getRequestBody().readAllBytes();
        }
        LocalResult local = performLocalOperation(id, method, body);

        if ("GET".equals(method) && local.notFound) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        List<V11qfourNode> otherNodes = targetNodes.stream()
                .filter(n -> !n.url().equals(selfUrl))
                .toList();

        boolean remoteSuccess = true;
        if (ack > 1 && !otherNodes.isEmpty()) {
            remoteSuccess = replicator.sendWithAck(id,
                    method, body, otherNodes, ack - 1).join();
        }

        if (local.success && remoteSuccess) {
            int responseCode = switch (method) {
                case "PUT" -> 201;
                case "DELETE" -> 202;
                case "GET" -> 200;
                default -> 200;
            };

            if ("GET".equals(method) && local.data != null) {
                exchange.sendResponseHeaders(responseCode, local.data.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(local.data);
                }
            } else {
                exchange.sendResponseHeaders(responseCode, -1);
            }
        } else {
            exchange.sendResponseHeaders(503, -1);
        }
    }

    private final class LocalResult {
        boolean success;
        boolean notFound;
        byte[] data;
    }

    private LocalResult performLocalOperation(String id, String method, byte[] body) {
        LocalResult result = new LocalResult();
        try {
            switch (method) {
                case "GET" -> {
                    result.data = dao.get(id);
                    result.success = true;
                }
                case "PUT" -> {
                    dao.upsert(id, body);
                    result.success = true;
                }
                case "DELETE" -> {
                    dao.delete(id);
                    result.success = true;
                }
                default -> {
                    log.warn("Unsupported method: {}", method);
                    result.success = false;
                }
            }
        } catch (NoSuchElementException e) {
            result.notFound = true;
            result.success = !"GET".equals(method);
        } catch (Exception e) {
            log.error("Local DAO error", e);
            result.success = false;
        }
        return result;
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

    private int parseAck(String query) {
        if (query == null || !query.contains("ack=")) {
            return 1;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("ack=")) {
                try {
                    return Integer.parseInt(param.substring(4));
                } catch (NumberFormatException e) {
                    return 1;
                }
            }
        }
        return 1;
    }

    private InetSocketAddress createInetSocketAddress(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            log.error("Port must be in range 1 to 65535");
            throw new IllegalArgumentException("Port must be in range 1 to 65535");
        }
        return new InetSocketAddress(port);
    }
}
