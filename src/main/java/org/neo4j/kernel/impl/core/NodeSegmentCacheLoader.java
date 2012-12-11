package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.concurrent.Callable;

public class NodeSegmentCacheLoader implements Callable<Integer> {
    private final long start;
    private final long segment;
    private final NodeManager nodeManager;

    public NodeSegmentCacheLoader(long start, long segment, NodeManager nodeManager) {
        this.start = start;
        this.segment = segment;
        this.nodeManager = nodeManager;
    }

    @Override
    public Integer call() throws Exception {
        int count=0;
        for(long n = start;n< start + segment;n++) {
            Node node = nodeManager.getNodeByIdOrNull(n);
            if (node==null) continue;
            count++;
            for (Relationship relationship : node.getRelationships()) {
                relationship.getOtherNode(node);
                count++;
            }
        }
        return count;
    }
}
