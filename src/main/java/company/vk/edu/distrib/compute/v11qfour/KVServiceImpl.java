package company.vk.edu.distrib.compute.v11qfour;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class KVServiceImpl implements KVService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;
    private HttpServer server;
    private final InetSocketAddress address;
    private final Dao<byte[]> dao;

    public KVServiceImpl(int port, Dao<byte[]> dao) {
        this.dao = dao;
        this.address = createInetSocketAddress(port);
    }

    private InetSocketAddress createInetSocketAddress(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            log.error("Port must be in range 1 to 65535");
            throw new IllegalArgumentException("Port must be in range 1 to 65535");
        }
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(address, 0);
            server.createContext("/v0/status", exchange -> {
                exchange.sendResponseHeaders(200, 0);
                exchange.close();
            });
            server.createContext("/v0/entity", exchange -> {
                String query = exchange.getRequestURI().getQuery();
                String key = query.split("=")[1];
                byte [] answer = dao.get(key);
                exchange.sendResponseHeaders(200, answer.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(answer);
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
}
