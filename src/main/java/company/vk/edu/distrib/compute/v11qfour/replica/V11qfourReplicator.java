package company.vk.edu.distrib.compute.v11qfour.replica;

import company.vk.edu.distrib.compute.v11qfour.cluster.V11qfourNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class V11qfourReplicator {
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Logger log = LoggerFactory.getLogger(V11qfourReplicator.class);

    public CompletableFuture<Boolean> sendWithAck(String id,
                                                  String method,
                                                  byte[] body,
                                                  List<V11qfourNode> targets,
                                                  int ack) {
        List<CompletableFuture<Integer>> futures = targets.stream()
                .map(node -> {
                    String targetUrl = node.url() + "/v0/entity?id=" + id;
                    HttpRequest.Builder builder = HttpRequest.newBuilder()
                            .uri(URI.create(targetUrl))
                            .method(method, body == null
                                    ? HttpRequest.BodyPublishers.noBody() :
                                    HttpRequest.BodyPublishers.ofByteArray(body));

                    return httpClient.sendAsync(builder.build(), HttpResponse.BodyHandlers.discarding())
                            .thenApply(HttpResponse::statusCode)
                            .exceptionally(ex -> 0);
                })
                .toList();
        return CompletableFuture.supplyAsync(() -> {
            int successCount = 0;
            for (CompletableFuture<Integer> future : futures) {
                try {
                    int statusCode = future.get(2, TimeUnit.SECONDS);
                    if (statusCode >= 200 && statusCode < 300) {
                        successCount++;
                    }
                } catch (Exception e) {
                    log.error("Not answer from replica", e);
                }
            }
            return successCount >= ack;
        });
    }
}
