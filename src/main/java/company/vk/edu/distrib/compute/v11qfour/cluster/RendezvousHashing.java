package company.vk.edu.distrib.compute.v11qfour.cluster;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class RendezvousHashing implements V11qfourRoutingStrategy {
    public static final String HASH_ALGORITHM = "SHA-256";

    @Override
    public V11qfourNode getResponsibleNode(String key, List<V11qfourNode> allNodes) {
        validateKey(key);
        long maxValue = Long.MIN_VALUE;
        V11qfourNode bestNode = null;
        for (V11qfourNode node : allNodes) {
            long tempValue = getCurrentHash(key + '|' + node);
            if (tempValue > maxValue) {
                maxValue = tempValue;
                bestNode = node;
            }
        }
        return bestNode;
    }

    @Override
    public List<V11qfourNode> getResponsibleNodes(String key, List<V11qfourNode> allNodes, int n) {
        validateKey(key);
        return allNodes.stream()
                .sorted((node1, node2) -> {
                    long hash1 = getCurrentHash(key + '|' + node1);
                    long hash2 = getCurrentHash(key + '|' + node2);
                    return Long.compare(hash2, hash1);
                })
                .limit(n)
                .toList();
    }

    private long getCurrentHash(String s) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hash = messageDigest.digest(s.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(hash).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
    }
}
