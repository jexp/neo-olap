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
            Node newNode=null;
            for (Relationship relationship : node.getRelationships()) {
                Node otherNode = relationship.getOtherNode(node);
                nodeCount++;
                if (random.nextBoolean() && isInNodeRange(otherNode)) {
                    newNode = otherNode;
                    hitCount++;
                    nodes[((int) newNode.getId())]++;
                    break;
                }
            }
            if (newNode==null) node=randomNode();
            if (System.currentTimeMillis() - time > timeInMillis) break;
        }
        System.out.printf("Thread %d In %d seconds %d hits %d nodes %n", id, (System.currentTimeMillis() - time)/1000, hitCount, nodeCount);
    }
}
