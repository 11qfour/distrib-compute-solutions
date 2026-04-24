package company.vk.edu.distrib.compute.v11qfour.cluster;

import java.util.List;

@FunctionalInterface
public interface V11qfourRoutingStrategy {
    default V11qfourNode getResponsibleNode(String key, List<V11qfourNode> allNodes) {
        return getResponsibleNodes(key, allNodes, 1).get(0);
    }

    List<V11qfourNode> getResponsibleNodes(String key, List<V11qfourNode> allNodes, int n);
}
