package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.concurrent.Callable;

public class NodeSegmentCacheLoader implements Callable<Integer> {
    private static final int LOG_PARTS = 4;
    private final long segment;
    private final NodeManager nodeManager;
    private final long minNodeId;
    private final long index;

    public NodeSegmentCacheLoader(long minNodeId,long index, long segment, NodeManager nodeManager) {
        this.minNodeId = minNodeId;
        this.index = index;
        this.segment = segment;
        this.nodeManager = nodeManager;
    }

    @Override
    public Integer call() throws Exception {
        int count=0;
        long fragment = segment / LOG_PARTS;
        long start = minNodeId + index * segment;
        long end = start + segment;
        System.out.printf("%2d. Loading nodes from %10d up to %10d%n",index,start,end);
        long logAt = start + fragment;
        for(long n = start;n < end; n++) {
            Node node = nodeManager.getNodeByIdOrNull(n);
            if (n > logAt) {
                System.out.printf("%2d. %3d%%%n",index,100/LOG_PARTS*(n-start)/fragment);
                System.out.flush();
                logAt+=fragment;
            }
            if (node==null) continue;
            count++;
            for (Relationship relationship : node.getRelationships()) {
                relationship.getOtherNode(node);
                count++;
            }
        }
        System.out.printf("%2d. 100%% Done, loaded %d nodes+relationships %n",index,count);
        return count;
    }
}
