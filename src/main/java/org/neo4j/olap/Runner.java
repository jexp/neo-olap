package org.neo4j.olap;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.cache.Cache;
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
public class Runner {
    public static final int MEMORY_PER_NODE = 5*1024;
    private static final int MEGABYTE = 1024*1024;
    private static final int timeInSeconds = 100;

    private final GraphDatabaseAPI db;

    public Runner(GraphDatabaseAPI db) {
        this.db = db;
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
        final Runner runner = new Runner(db);
        runner.runAnalysis();
    }

    public void runAnalysis() throws Exception {
        System.out.println("threads = " + getMaxNumberOfThreads());
        NodeManager nodeManager = db.getDependencyResolver().resolveDependency(NodeManager.class);
        long maxNodeId = nodeManager.getHighestPossibleIdInUse(Node.class) + 1 ;
        final int[] nodes = new int[(int)maxNodeId];

        final long nodesPerRound = determineNodesPerRound(maxNodeId);

        for (long minNodeId=0;minNodeId+nodesPerRound < maxNodeId;minNodeId+=nodesPerRound) {
            runRound(nodes, minNodeId, nodesPerRound);
        }

        printTop(nodes, 10);
    }

    private long determineNodesPerRound(long maxNodeId) {
        long memory = Runtime.getRuntime().freeMemory();
        long nodesInMemory = memory / 2 / MEMORY_PER_NODE;
        final long nodesPerRound = Math.min(nodesInMemory,maxNodeId);

        System.out.printf("maxNodeId = %d memory %d MB nodes in memory %d nodesPerRound %d%n", maxNodeId, memory / MEGABYTE, nodesInMemory,nodesPerRound);
        return nodesPerRound;
    }

    private int getMaxNumberOfThreads() {
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    private void runRound(int[] nodes, long minNodeId, long nodeCount) throws ExecutionException, InterruptedException {
        NodeManager nodeManager = db.getDependencyResolver().resolveDependency(NodeManager.class);
        final int processors = getMaxNumberOfThreads();

        long time=System.currentTimeMillis();
        nodeManager.clearCache();
        long nodeAndRelCount = fillCache(minNodeId,nodeCount, processors/4, nodeManager);
        System.out.printf("filled cache with up to %d nodes, %d nodes+relationships in %d ms, memory %d MB%n", nodeCount, nodeAndRelCount,
                            System.currentTimeMillis() - time, Runtime.getRuntime().freeMemory() / MEGABYTE);


        final ExecutorService pool = Executors.newFixedThreadPool(processors);
        Collection<OlapRunner> runners=new ArrayList<OlapRunner>();
        for (int i=0;i<processors;i++) {
            final OlapRunner runner = createRunner(db, nodes, minNodeId, nodeCount, i);
            runners.add(runner);
            pool.submit(runner);
        }
        pool.shutdown();
        pool.awaitTermination(timeInSeconds * 2, TimeUnit.SECONDS);
        pool.shutdownNow();
        printNumbers(runners, timeInSeconds);
    }

    protected RandomWalkingRunner createRunner(GraphDatabaseAPI db, int[] nodes, long minNodeId, long nodeCount, int i) {
        return new RandomWalkingRunner(db, i, minNodeId, nodeCount, timeInSeconds, nodes);
    }

    protected long fillCache(long minNodeId, long nodeCount, int processors, final NodeManager nodeManager) throws ExecutionException, InterruptedException {
        final long segment = nodeCount / processors;
        final ExecutorService pool = Executors.newFixedThreadPool(processors);
        Collection<Future<Integer>> futures=new ArrayList<Future<Integer>>(processors);
        for (int i=0;i<processors;i++) {
            NodeSegmentCacheLoader loader = new NodeSegmentCacheLoader(minNodeId, i, segment, nodeManager);
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

    protected void printNumbers(Collection<OlapRunner> runners, int timeInSeconds) {
        int pathCount=0,nodeCount=0;
        for (OlapRunner runner : runners) {
            pathCount+=runner.getHitCount();
            nodeCount+=runner.getNodeCount();
        }
        System.out.printf("In %d seconds %d paths %d nodes %n", timeInSeconds, pathCount, nodeCount);
    }

    protected void printTop(int[] nodes, int howMany) {
        final TopNSelector selector = new TopNSelector(nodes);
        for (Pair<Integer, Integer> pair : selector.selectTopN(howMany)) {
            System.out.printf("Node %d Count %d%n", pair.first(), pair.other());
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
