package org.neo4j.olap;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeSegmentCacheLoader;

import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author mh
 * @since 23.11.12
 */
public class Runner implements Runnable {
    public static final int MEMORY_PER_NODE = 1024;
    private final GraphDatabaseAPI db;
    private final int timeInMillis;
    private final long maxNodeId;
    private final Random random = new Random();
    private final PathFinder<Path> pathFinder;
    private int[] nodes;
    private volatile int pathCount = 0;
    private volatile int nodeCount = 0;
    private int id;

    class RelationshipFilter extends FilteringIterable<Relationship> {
        RelationshipFilter(final Node node) {
            super(node.getRelationships(), new Predicate<Relationship>() {
                @Override
                public boolean accept(Relationship item) {
                    return isInNodeRange(item.getOtherNode(node));
                }
            });
        }
    }

    public Runner(GraphDatabaseAPI db, int id, final long maxNodeId, int timeInSeconds, final int[] nodes, int maxDepth) {
        this.db = db;
        this.id = id;
        this.timeInMillis = timeInSeconds * 1000;
        this.maxNodeId = maxNodeId;
        this.nodes = nodes;
        pathFinder = GraphAlgoFactory.shortestPath(new PathExpander() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState state) {
                final Node end = path.endNode();
                if (isInNodeRange(end)) {
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

    private boolean isInNodeRange(Node end) {
        return end.getId() < maxNodeId;
    }

    public static void main(String[] args) throws Exception {
        final String path = args[0];
        final GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path).setConfig(config()).newGraphDatabase();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (db != null) db.shutdown();
            }
        });
        final int processors = Runtime.getRuntime().availableProcessors() * 2;
        System.out.println("processors = " + processors);
        final ExecutorService pool = Executors.newFixedThreadPool(processors);

        NodeManager nodeManager = db.getDependencyResolver().resolveDependency(NodeManager.class);
        long maxNodeId = nodeManager.getHighestPossibleIdInUse(Node.class) + 1 ;
        long memory = Runtime.getRuntime().freeMemory();
        long nodesInMemory = memory / 2 / MEMORY_PER_NODE;
        System.out.println("maxNodeId = " + maxNodeId+" memory "+memory+" nodes in memory "+nodesInMemory);
        final long nodeIdLimit = Math.min(nodesInMemory,maxNodeId);
        long time=System.currentTimeMillis();
        long nodeAndRelCount = fillCache(nodeIdLimit, processors, nodeManager);
        System.out.println("filled cache with up to " + nodeIdLimit+" nodes, "+nodeAndRelCount+" nodes+relationships in "+(System.currentTimeMillis()-time)+" ms.");
        final int[] nodes = new int[(int)nodeIdLimit];
        final int timeInSeconds = 100;
        final int maxDepth = 10;
        Collection<Runner> runners=new ArrayList<Runner>();
        for (int i=0;i<processors;i++) {
            final Runner runner = new Runner(db, i,nodeIdLimit, timeInSeconds, nodes, maxDepth);
            runners.add(runner);
            pool.submit(runner);
        }
        pool.shutdown();
        pool.awaitTermination(timeInSeconds*2, TimeUnit.SECONDS);
        printTop(nodes,nodeIdLimit,10);
        printNumbers(runners,timeInSeconds);

    }

    private static long fillCache(long maxNodeId, int processors, final NodeManager nodeManager) throws ExecutionException, InterruptedException {
        final long segment = maxNodeId / processors;
        final ExecutorService pool = Executors.newFixedThreadPool(processors);
        Collection<Future<Integer>> futures=new ArrayList<Future<Integer>>(processors);
        for (int i=0;i<processors;i++) {
            long start = segment * i;
            NodeSegmentCacheLoader loader = new NodeSegmentCacheLoader(i, segment, nodeManager);
            Future<Integer> future = pool.submit(loader);
            futures.add(future);
        }
        long count=0;
        for (Future<Integer> future : futures) {
            count+=future.get();
        }
        pool.shutdown();
        return count;
    }

    private static void printNumbers(Collection<Runner> runners, int timeInSeconds) {
        int pathCount=0,nodeCount=0;
        for (Runner runner : runners) {
            pathCount+=runner.getPathCount();
            nodeCount+=runner.getNodeCount();
        }
        System.out.printf("In %d seconds %d paths %d nodes %n", timeInSeconds, pathCount, nodeCount);
    }

    public void run() {
        long time = System.currentTimeMillis();
        while (true) {
            final Iterable<Path> paths = pathFinder.findAllPaths(randomNode(), randomNode());
            for (Path path : paths) {
                pathCount++;
                countNodes(path);
                nodeCount += path.length()+1;
            }
            if (System.currentTimeMillis() - time > timeInMillis) break;
        }
        System.out.printf("Thread %d In %d seconds %d paths %d nodes %n", id, (System.currentTimeMillis() - time)/1000, pathCount, nodeCount);
    }

    public int getPathCount() {
        return pathCount;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    private void countNodes(Path path) {
        final Iterator<Node> it = path.nodes().iterator();
        it.next(); // ignore first
        while (it.hasNext()) {
            final Node node = it.next();
            if (it.hasNext()) { // ignore last
                nodes[((int) node.getId())]++;
            }
        }
    }

    private static void printTop(int[] nodes, long maxNodeId, int howMany) {
        int[] ids = new int[howMany];
        int[] counts = new int[howMany];
        int minCount = 0;
        int minIdx = 0;
        for (int i = 0; i < maxNodeId; i++) {
            if (nodes[i] > minCount) {
                ids[minIdx] = i;
                counts[minIdx] = nodes[i];
                minCount = nodes[i];
                for (int j = 0; j < howMany; j++) {
                    if (counts[j] < minCount) {
                        minCount = counts[j];
                        minIdx = j;
                    }
                }
            }
        }
        for (int j = 0; j < howMany; j++) {
            System.out.printf("Node %d Count %d%n", ids[j], counts[j]);
        }
    }

    private Node randomNode() {
        while (true) {
            try {
                final long id = ( random.nextLong() % maxNodeId );
                return db.getNodeById(id);
            } catch (NotFoundException nfe) {

            }
        }
    }

    static Map<String, String> config() {
        Map<String, String> config = new HashMap<String, String>();
        try {
            if (new File("batch.properties").exists()) {
                System.out.println("Using Existing Configuration File");
            } else {
                System.out.println("Writing Configuration File to batch.properties");
                FileWriter fw = new FileWriter("batch.properties");
                fw.append("use_memory_mapped_buffers=true\n"
                        + "neostore.nodestore.db.mapped_memory=100M\n"
                        + "neostore.relationshipstore.db.mapped_memory=500M\n"
                        + "neostore.propertystore.db.mapped_memory=1G\n"
                        + "neostore.propertystore.db.strings.mapped_memory=200M\n"
                        + "neostore.propertystore.db.arrays.mapped_memory=0M\n"
                        + "neostore.propertystore.db.index.keys.mapped_memory=15M\n"
                        + "neostore.propertystore.db.index.mapped_memory=15M");
                fw.close();
            }

            config = MapUtil.load(new File("batch.properties"));

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return config;
    }

}
