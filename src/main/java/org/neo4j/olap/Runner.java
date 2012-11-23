package org.neo4j.olap;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.Traversal;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author mh
 * @since 23.11.12
 */
public class Runner implements Runnable {
    private final GraphDatabaseAPI db;
    private final int timeInMillis;
    private final long maxNodeId;
    private final Random random = new Random();
    private final PathFinder<Path> pathFinder = GraphAlgoFactory.shortestPath(Traversal.expanderForAllTypes(), 20);
    private int[] nodes;

    public Runner(GraphDatabaseAPI db, final long maxNodeId, int timeInSeconds, final int[] nodes) {
        this.db = db;
        this.timeInMillis = timeInSeconds * 1000;
        this.maxNodeId = maxNodeId;
        this.nodes = nodes;
    }

    public static void main(String[] args) throws InterruptedException {
        final String path = args[0];
        final GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path).setConfig(config()).newGraphDatabase();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (db != null) db.shutdown();
            }
        });
        final long maxNodeId = db.getNodeManager().getHighestPossibleIdInUse(Node.class) + 1;
        System.out.println("maxNodeId = " + maxNodeId);
        final int[] nodes = new int[(int)maxNodeId];
        final int processors = Runtime.getRuntime().availableProcessors();
        System.out.println("processors = " + processors);
        final ExecutorService pool = Executors.newFixedThreadPool(processors);
        final int timeInSeconds = 100;
        for (int i=0;i<processors;i++) {
            final Runner runner = new Runner(db, maxNodeId, timeInSeconds, nodes);
            pool.submit(runner);
        }
        pool.shutdown();
        pool.awaitTermination(timeInSeconds*2, TimeUnit.SECONDS);
        printTop(nodes,maxNodeId,10);
    }

    public void run() {
        long time = System.currentTimeMillis();
        int pathCount = 0, nodeCount = 0;
        while (true) {
            final Iterable<Path> paths = pathFinder.findAllPaths(randomNode(), randomNode());
            for (Path path : paths) {
                pathCount++;
                countNodes(path);
                nodeCount += path.length();
            }
            if (System.currentTimeMillis() - time > timeInMillis) break;
        }
        System.out.printf("In %d seconds %d paths %d nodes %n", timeInMillis / 1000, pathCount, nodeCount);
    }

    private void countNodes(Path path) {
        for (Node node : path.nodes()) {
            nodes[((int) node.getId())]++;
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
                final long id = random.nextLong() % maxNodeId;
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
