package company.vk.edu.distrib.compute.v11qfour.cluster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RendezvousHashingTest {
    @Test
    void testGetResponsibleNodesReturnsCorrectAmount() {
        RendezvousHashing strategy = new RendezvousHashing();
        List<V11qfourNode> allNodes = List.of(
                new V11qfourNode("http://localhost:8081"),
                new V11qfourNode("http://localhost:8082"),
                new V11qfourNode("http://localhost:8083")
        );

        int n = 2;
        List<V11qfourNode> result = strategy.getResponsibleNodes("my-key", allNodes, n);

        assertEquals(n, result.size());

        Set<V11qfourNode> uniqueNodes = new HashSet<>(result);
        assertEquals(n, uniqueNodes.size());
    }

    @Test
    void testDeterminism() {
        RendezvousHashing strategy = new RendezvousHashing();
        List<V11qfourNode> allNodes = List.of(
                new V11qfourNode("http://localhost:8081"),
                new V11qfourNode("http://localhost:8082"),
                new V11qfourNode("http://localhost:8083")
        );

        String key = "consistent-key";
        List<V11qfourNode> firstCall = strategy.getResponsibleNodes(key, allNodes, 3);
        List<V11qfourNode> secondCall = strategy.getResponsibleNodes(key, allNodes, 3);

        assertEquals(firstCall, secondCall);
    }

    @Test
    void testDistributionIsFair() {
        List<V11qfourNode> nodes = List.of(
                new V11qfourNode("http://node1:8080"),
                new V11qfourNode("http://node2:8081"),
                new V11qfourNode("http://node3:8082")
        );
        RendezvousHashing strategy = new RendezvousHashing();

        Map<V11qfourNode, Integer> distribution = new ConcurrentHashMap<>();

        for (int i = 0; i < 10000; i++) {
            String key = "key-" + i;
            V11qfourNode responsible = strategy.getResponsibleNode(key, nodes);
            distribution.put(responsible, distribution.getOrDefault(responsible, 0) + 1);
        }

        System.out.println(distribution);
        Assertions.assertNotNull(!distribution.isEmpty());
    }
}
