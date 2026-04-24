package company.vk.edu.distrib.compute.v11qfour.replica;

import company.vk.edu.distrib.compute.v11qfour.cluster.V11qfourNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class V11qfourReplicator {
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public CompletableFuture<Boolean> sendWithAck(String path,
                                                  String method,
                                                  byte[] body,
                                                  List<V11qfourNode> targets,
                                                  int ack) {
        List<CompletableFuture<Integer>> futures = targets.stream()
                .map(node -> {
                    HttpRequest.Builder builder = HttpRequest.newBuilder()
                            .uri(URI.create(node.url() + path))
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
                    throw new IllegalStateException("Replica not answered or failed", e);
                }
            }
            return successCount >= ack;
        });
    }
}
