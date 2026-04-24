package company.vk.edu.distrib.compute.v11qfour.service;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.v11qfour.cluster.*;
import company.vk.edu.distrib.compute.v11qfour.dao.V11qfourPersistentDao;
import company.vk.edu.distrib.compute.v11qfour.proxy.V11qfourProxyClient;
import company.vk.edu.distrib.compute.v11qfour.replica.V11qfourReplicator;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class V11qfourKVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        //int n = Integer.parseInt(System.getProperty("replication.factor", "1"));
        int n = 3;
        Dao<byte[]> dao = new V11qfourPersistentDao();
        String clusterNodesProp = System.getProperty("cluster.nodes", "http://localhost:" + port);
        List<V11qfourNode> clusterNodes = Arrays.stream(clusterNodesProp.split(","))
                .map(V11qfourNode::new)
                .collect(Collectors.toList());

        String selfUrl = "http://localhost:" + port;
        V11qfourReplicator replicator = new V11qfourReplicator();
        V11qfourRoutingStrategy routingStrategy = new RendezvousHashing();
        V11qfourProxyClient proxyClient = new V11qfourProxyClient();
        return new V11qfourKVServiceFactory(port, dao, routingStrategy, clusterNodes,
                selfUrl, proxyClient, n, replicator);
    }
}
