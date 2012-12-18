package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

/**
 * @author mh
 * @since 18.12.12
 */
public class RelationshipPreloader extends Preloader {

    public RelationshipPreloader(int index, long segment, long minNodeId, long nodeCount, DependencyResolver dependencyResolver) {
        super(segment, minNodeId, nodeCount, index,dependencyResolver);
    }

    @Override
    protected int doLoad(long id) {
        final RelationshipRecord record = persistenceManager.loadLightRelationship(id);
        if (record == null || (!isInNodeRange(record.getFirstNode()) && !isInNodeRange(record.getSecondNode())))
            return 0;
        nodeManager.getRelationshipByIdOrNull(id);
        return 1;
    }

}
