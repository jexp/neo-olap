package org.neo4j.olap;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.kernel.GraphDatabaseAPI;

/**
 * @author mh
 * @since 23.11.12
 */
public class PathFinderNodeCountingRunner extends OlapRunner {
    private final PathFinder<Path> pathFinder;

    public PathFinderNodeCountingRunner(GraphDatabaseAPI db, int id, long minNodeId, long nodeCount, int timeInSeconds, final int[] nodes, int maxDepth) {
        super(timeInSeconds, id, nodes, db, minNodeId, nodeCount);
        pathFinder = GraphAlgoFactory.shortestPath(new PathExpander() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState state) {
                final Node end = path.endNode();
                if (PathFinderNodeCountingRunner.this.isInNodeRange(end.getId())) {
                    return new RelationshipFilter(end);
                }
                return null;
            }

            @Override
            public PathExpander reverse() {
                return this;
            }
        }, maxDepth);
    }

    public void run() {
        long time = System.currentTimeMillis();
        while (true) {
            final Iterable<Path> paths = pathFinder.findAllPaths(randomNode(), randomNode());
            for (Path path : paths) {
                hitCount++;
                countNodes(path);
                nodeCount += path.length()+1;
            }
            if (System.currentTimeMillis() - time > timeInMillis) break;
        }
        System.out.printf("Thread %d In %d seconds %d paths %d nodes %n", id, (System.currentTimeMillis() - time)/1000, getHitCount(), nodeCount);
    }

}
