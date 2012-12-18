package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * @author mh
 * @since 18.12.12
 */
public class NodePreloader extends Preloader {

    public NodePreloader(int index, long segment, long minNodeId, long nodeCount, DependencyResolver dependencyResolver) {
        super(segment, minNodeId, nodeCount, index, dependencyResolver);
    }

    @Override
    protected int doLoad(long id) {
        int count = 0;
        Node node = nodeManager.getNodeByIdOrNull(id);
        if (node != null) {
            count++;
            for (Relationship relationship : node.getRelationships()) {
                relationship.getOtherNode(node);
                count++;
            }
        }
        return count;
    }

}
