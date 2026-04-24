package company.vk.edu.distrib.compute.v11qfour.cluster;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.v11qfour.dao.V11qfourPersistentDao;
import company.vk.edu.distrib.compute.v11qfour.proxy.V11qfourProxyClient;
import company.vk.edu.distrib.compute.v11qfour.replica.V11qfourReplicator;
import company.vk.edu.distrib.compute.v11qfour.service.V11qfourKVServiceFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class V11qfourKVClusterFactory {
    public V11qfourKVCluster create(List<Integer> ports) throws IOException {
        String algo = System.getProperty("algo", "rendezvous");
        List<V11qfourNode> allNodes = ports.stream()
                .map(p -> new V11qfourNode("http://localhost:" + p))
                .toList();
        V11qfourRoutingStrategy strategy;
        if ("consistent".equals(algo)) {
            strategy = new ConsistentHashing(allNodes);
        } else {
            strategy = new RendezvousHashing();
        }
        V11qfourProxyClient proxyClient = new V11qfourProxyClient();
        V11qfourReplicator replicator = new V11qfourReplicator();
        //int n = Integer.parseInt(System.getProperty("replication.factor", "1"));
        int n = 3;
        Map<String, KVService> nodesMap = new ConcurrentHashMap<>();
        for (Integer port : ports) {
            String url = "http://localhost:" + port;
            nodesMap.put(url, nodeService(port, url, strategy, allNodes, proxyClient, n, replicator));
        }
        return new V11qfourKVCluster(nodesMap);
    }

    private static V11qfourKVServiceFactory nodeService(
            int port,
            String url,
            V11qfourRoutingStrategy strategy,
            List<V11qfourNode> allNodes,
            V11qfourProxyClient proxyClient,
            int n,
            V11qfourReplicator replicator) {
        try {
            return new V11qfourKVServiceFactory(
                    port,
                    new V11qfourPersistentDao(),
                    strategy,
                    allNodes,
                    url,
                    proxyClient,
                    n,
                    replicator);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
