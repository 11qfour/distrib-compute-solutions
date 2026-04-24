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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;

public class V11qfourKVServiceFactory implements KVService {
    private static final Logger log = LoggerFactory.getLogger(V11qfourKVServiceFactory.class);
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private HttpServer server;
    private final InetSocketAddress address;
    private final Dao<byte[]> dao;
    private final String selfUrl;
    private final V11qfourProxyClient proxyClient;
    private final V11qfourRoutingStrategy routingStrategy;
    private final List<V11qfourNode> clusterNodes;
    private final int amountN;
    private final V11qfourReplicator replicator;

    public V11qfourKVServiceFactory(int port, Dao<byte[]> dao, V11qfourRoutingStrategy routingStrategy,
                                    List<V11qfourNode> clusterNodes, String selfUrl,
                                    V11qfourProxyClient proxyClient, int amountN,
                                    V11qfourReplicator replicator) {
        this.dao = dao;
        this.routingStrategy = routingStrategy;
        this.clusterNodes = clusterNodes;
        this.selfUrl = selfUrl;
        this.proxyClient = proxyClient;
        this.address = new InetSocketAddress(port);
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
                } catch (IOException ex) {
                    if (log.isErrorEnabled()) {
                        log.error("Status error", ex);
                    }
                }
            });
            server.createContext("/v0/entity", exchange -> {
                try (exchange) {
                    handleEntityRequest(exchange);
                } catch (IOException ex) {
                    if (log.isErrorEnabled()) {
                        log.error("Entity error", ex);
                    }
                }
            });
            server.start();
        } catch (IOException exception) {
            throw new UncheckedIOException("Server failed to start", exception);
        }
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void handleEntityRequest(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String id = getParam(query, "id=");
        String ackStr = getParam(query, "ack=");
        int ack = Integer.parseInt(Optional.ofNullable(ackStr).orElse("1"));

        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        List<V11qfourNode> targets = routingStrategy.getResponsibleNodes(id, clusterNodes, amountN);
        if (ack > amountN || ack <= 0) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        if (targets.stream().anyMatch(n -> n.url().equals(selfUrl))) {
            handleWithReplication(exchange, id, targets, ack);
        } else {
            proxyClient.proxy(exchange, targets.get(0));
        }
    }

    private boolean replicate(String id, String method, byte[] body, List<V11qfourNode> targets, int ack) {
        List<V11qfourNode> others = targets.stream()
                .filter(n -> !n.url().equals(selfUrl))
                .toList();

        if (ack <= 1 || others.isEmpty()) {
            return true;
        }

        return replicator.sendWithAck(id, method, body, others, ack - 1).join();
    }

    private int getSuccessCode(String method) {
        return switch (method) {
            case METHOD_PUT -> 201;
            case METHOD_DELETE -> 202;
            default -> 200;
        };
    }

    private void handleWithReplication(HttpExchange exchange, String id,
                                       List<V11qfourNode> targets, int ack) throws IOException {
        String method = exchange.getRequestMethod();

        byte[] body = null;
        if (METHOD_PUT.equals(exchange.getRequestMethod())) {
            body = exchange.getRequestBody().readAllBytes();
        }

        LocalResult local = performLocalOperation(id, method, body);

        if (METHOD_GET.equals(method) && local.notFound) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        boolean remoteSuccess = replicate(id, method, body, targets, ack);

        if (local.success && remoteSuccess) {
            sendResponse(exchange, getSuccessCode(method), local.data);
        } else {
            exchange.sendResponseHeaders(503, -1);
        }
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] data) throws IOException {
        if (data != null) {
            exchange.sendResponseHeaders(code, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        } else {
            exchange.sendResponseHeaders(code, -1);
        }
    }

    private LocalResult performLocalOperation(String id, String method, byte[] body) {
        LocalResult res = new LocalResult();
        try {
            switch (method) {
                case METHOD_GET -> {
                    res.data = dao.get(id);
                    res.success = true;
                }
                case METHOD_PUT -> {
                    dao.upsert(id, body);
                    res.success = true;
                }
                case METHOD_DELETE -> {
                    dao.delete(id);
                    res.success = true;
                }
                default -> {
                    res.success = false;
                }
            }
        } catch (NoSuchElementException e) {
            res.notFound = true;
            res.success = !METHOD_GET.equals(method);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("DAO error", e);
            }
            res.success = false;
        }
        return res;
    }

    private String getParam(String query, String prefix) {
        if (query == null) {
            return null;
        }
        return Arrays.stream(query.split("&"))
                .filter(s -> s.startsWith(prefix))
                .map(s -> s.substring(prefix.length()))
                .findFirst().orElse(null);
    }

    private static final class LocalResult {
        private boolean success;
        private boolean notFound;
        private byte[] data;
    }
}
