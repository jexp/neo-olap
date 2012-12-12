package org.neo4j.olap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.GraphDatabaseAPI;

/**
 * @author mh
 * @since 23.11.12
 */
public class RandomWalkingRunner extends OlapRunner {
    public RandomWalkingRunner(GraphDatabaseAPI db, int id, final long maxNodeId, int timeInSeconds, final int[] nodes) {
        super(timeInSeconds, id, nodes, db, maxNodeId);
    }

    public void run() {
        long time = System.currentTimeMillis();
        Node node=randomNode();
        while (true) {
            for (Relationship relationship : node.getRelationships()) {
                final Node newNode = relationship.getOtherNode(node);
                nodeCount++;
                if (random.nextBoolean() && isInNodeRange(newNode)) {
                    node = newNode;
                    hitCount++;
                    nodes[((int) node.getId())]++;
                    break;
                }
            }
            if (System.currentTimeMillis() - time > timeInMillis) break;
        }
        System.out.printf("Thread %d In %d seconds %d hits %d nodes %n", id, (System.currentTimeMillis() - time)/1000, hitCount, nodeCount);
    }
}
