package org.neo4j.olap;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
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
public class Runner {
    public static final int MEMORY_PER_NODE = 5*1024;
    private static final int MEGABYTE = 1024*1024;
    private static final int timeInSeconds = 100;

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
        final long nodeIdLimit = Math.min(nodesInMemory,maxNodeId);

        System.out.printf("maxNodeId = %d memory %d MB nodes in memory %d nodeIdLimit %d%n", maxNodeId, memory / MEGABYTE, nodesInMemory,nodeIdLimit);
        long time=System.currentTimeMillis();
        long nodeAndRelCount = fillCache(nodeIdLimit, processors/4, nodeManager);
        System.out.printf("filled cache with up to %d nodes, %d nodes+relationships in %d ms, memory %d MB%n", nodeIdLimit, nodeAndRelCount,
                            System.currentTimeMillis() - time, Runtime.getRuntime().freeMemory() / MEGABYTE);


        final int[] nodes = new int[(int)nodeIdLimit];
        Collection<OlapRunner> runners=new ArrayList<OlapRunner>();
        for (int i=0;i<processors;i++) {
            final OlapRunner runner = new RandomWalkingRunner(db, i,nodeIdLimit, timeInSeconds, nodes);
            runners.add(runner);
            pool.submit(runner);
        }
        pool.shutdown();
        pool.awaitTermination(timeInSeconds *2, TimeUnit.SECONDS);
        printTop(nodes,nodeIdLimit,10);
        printNumbers(runners, timeInSeconds);

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

    private static void printNumbers(Collection<OlapRunner> runners, int timeInSeconds) {
        int pathCount=0,nodeCount=0;
        for (OlapRunner runner : runners) {
            pathCount+=runner.getHitCount();
            nodeCount+=runner.getNodeCount();
        }
        System.out.printf("In %d seconds %d paths %d nodes %n", timeInSeconds, pathCount, nodeCount);
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
