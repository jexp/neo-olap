package org.neo4j.olap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.kernel.GraphDatabaseAPI;

import java.util.Iterator;
import java.util.Random;

/**
 * @author mh
 * @since 12.12.12
 */
public abstract class OlapRunner implements Runnable {
    protected final Random random = new Random();

    protected int id;

    protected final GraphDatabaseAPI db;
    protected final int timeInMillis;

    protected final long maxNodeCount;
    protected int[] nodes;
    protected volatile long nodeCount = 0;
    protected volatile long hitCount = 0;
    private final long minNodeId;

    public OlapRunner(int timeInSeconds, int id, final int[] nodes, GraphDatabaseAPI db, long minNodeId, final long nodeCount) {
        this.minNodeId = minNodeId;
        this.timeInMillis = timeInSeconds * 1000;
        this.id = id;
        this.nodes = nodes;
        this.db = db;
        this.maxNodeCount = nodeCount;
    }

    protected boolean isInNodeRange(long id) {
        return id >= minNodeId && id < minNodeId + maxNodeCount;
    }

    public long getNodeCount() {
        return nodeCount;
    }

    protected void countNodes(Path path) {
        final Iterator<Node> it = path.nodes().iterator();
        it.next(); // ignore first
        while (it.hasNext()) {
            final Node node = it.next();
            if (it.hasNext()) { // ignore last
                nodes[((int) node.getId())]++;
            }
        }
    }

    protected Node randomNode() {
        while (true) {
            try {
                final long id = minNodeId + ( random.nextLong() % maxNodeCount);
                if (isInNodeRange(id)) return db.getNodeById(id);
            } catch (NotFoundException nfe) {

            }
        }
    }

    public long getHitCount() {
        return hitCount;
    }

    class RelationshipFilter extends FilteringIterable<Relationship> {
        RelationshipFilter(final Node node) {
            super(node.getRelationships(), new Predicate<Relationship>() {
                @Override
                public boolean accept(Relationship item) {
                    final Node otherNode = item.getOtherNode(node);
                    return isInNodeRange(otherNode.getId());
                }
            });
        }
    }
}
