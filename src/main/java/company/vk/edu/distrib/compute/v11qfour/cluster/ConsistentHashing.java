package company.vk.edu.distrib.compute.v11qfour.cluster;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashing implements V11qfourRoutingStrategy {
    private final NavigableMap<Long, V11qfourNode> circle = new TreeMap<>();
    private static final int VIRTUAL_NODES = 100;
    public static final String HASH_ALGORITHM = "SHA-256";

    public ConsistentHashing(List<V11qfourNode> allNodes) {
        for (V11qfourNode node : allNodes) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                long hash = hash(node.url() + "#" + i);
                circle.put(hash, node);
            }
        }
    }

    private long hash(String s) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hash = messageDigest.digest(s.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(hash).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public V11qfourNode getResponsibleNode(String key, List<V11qfourNode> allNodes) {
        if (circle.isEmpty()) {
            throw new IllegalStateException("Cluster is empty");
        }

        long hash = hash(key);

        Map.Entry<Long, V11qfourNode> entry = circle.ceilingEntry(hash);

        if (entry == null) {
            entry = circle.firstEntry();
        }

        return entry.getValue();
    }

    @Override
    public List<V11qfourNode> getResponsibleNodes(String key, List<V11qfourNode> allNodes, int n) {
        if (circle.isEmpty()) {
            throw new IllegalStateException("Cluster is empty");
        }

        long hash = hash(key);
        List<V11qfourNode> result = new ArrayList<>();

        SortedMap<Long, V11qfourNode> tailMap = circle.tailMap(hash); //all alements from hash to end of ring
        Iterator<V11qfourNode> it = new Iterator<>() {
            private final Iterator<V11qfourNode> tailIt = tailMap.values().iterator();
            private final Iterator<V11qfourNode> headIt = circle.values().iterator();
            private boolean useTail = true;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public V11qfourNode next() {
                if (useTail && tailIt.hasNext()) {
                    return tailIt.next();
                }
                useTail = false;
                return headIt.next();
            }
        };

        while (result.size() < n && result.size() < allNodes.size()) {
            V11qfourNode node = it.next();
            if (!result.contains(node)) {
                result.add(node);
            }
        }
        return result;
    }
}
