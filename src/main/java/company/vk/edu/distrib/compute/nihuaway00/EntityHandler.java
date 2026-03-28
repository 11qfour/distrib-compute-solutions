package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class EntityHandler implements HttpHandler {

    private final EntityDao dao;

    EntityHandler(EntityDao dao) {
        this.dao = dao;
    }


    public Map<String, String> queryToMap(String query) throws UnsupportedEncodingException {
        if (query == null) {
            return null;
        }
        Map<String, String> result = new ConcurrentHashMap<>();
        for (String param : query.split("&")) {
            String[] entry = param.split("=");
            if (entry.length > 1) {
                result.put(
                        URLDecoder.decode(entry[0], Charset.defaultCharset()),
                        URLDecoder.decode(entry[1], Charset.defaultCharset())
                );
            } else {
                result.put(
                        URLDecoder.decode(entry[0], Charset.defaultCharset()),
                        ""
                );
            }
        }
        return result;
    }


    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        URI uri = exchange.getRequestURI();
        Map<String, String> params = queryToMap(uri.getQuery());

        switch (method) {
            case "GET" -> {
                handleGetEntity(exchange);
            }
            case "PUT" -> {
                handlePutEntity(exchange);
            }
            case "DELETE" -> {
                handleDeleteEntity(exchange);
            }
            default -> {
                exchange.close();
            }
        }

    }

    public void handleGetEntity(HttpExchange exchange) throws IOException {
        URI uri = exchange.getRequestURI();
        Map<String, String> params = queryToMap(uri.getQuery());
        String id = params.get("id");

        try {
            byte[] data = dao.get(id);
            exchange.sendResponseHeaders(200, data.length);
            OutputStream os = exchange.getResponseBody();
            os.write(data);

        } catch (NoSuchElementException err) {
            exchange.sendResponseHeaders(404, 0);
        } catch (IllegalArgumentException err) {
            exchange.sendResponseHeaders(400, 0);
        }

        exchange.close();
    }

    public void handlePutEntity(HttpExchange exchange) throws IOException {
        URI uri = exchange.getRequestURI();
        Map<String, String> params = queryToMap(uri.getQuery());

        String id = params.get("id");
        InputStream is = exchange.getRequestBody();
        var data = is.readAllBytes();
        is.close();

        try {
            dao.upsert(id, data);
            exchange.sendResponseHeaders(201, 0);
        } catch (IllegalArgumentException err) {
            exchange.sendResponseHeaders(400, 0);
        }

        exchange.close();

    }

    public void handleDeleteEntity(HttpExchange exchange) throws IOException {
        URI uri = exchange.getRequestURI();
        Map<String, String> params = queryToMap(uri.getQuery());

        String id = params.get("id");

        try {
            dao.delete(id);
            exchange.sendResponseHeaders(202, 0);
        } catch (IllegalArgumentException err) {
            exchange.sendResponseHeaders(400, 0);
        }

        exchange.close();
    }
}
