package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class NihuawayKVService implements company.vk.edu.distrib.compute.KVService {
    private static final Logger log = LoggerFactory.getLogger(NihuawayKVService.class);

    private HttpServer server;
    int port;

    NihuawayKVService(int port) {
        this.port = port;
    }

    @Override
    public void start() {
        try {
            InetSocketAddress addr = new InetSocketAddress(port);
            server = HttpServer.create(addr, 0);
            registerContexts();
            server.start();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(0);
        } else {
            log.warn("Server is not started");
        }
    }

    private void registerContexts(){
        EntityDao dao = new EntityDao();
        server.createContext("/v0/entity", new EntityHandler(dao));
        server.createContext("/v0/status", new PingHandler());
    }
}
