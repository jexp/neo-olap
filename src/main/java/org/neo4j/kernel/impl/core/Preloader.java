package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;

/**
 * @author mh
 * @since 18.12.12
 */
public abstract class Preloader implements Callable<Integer> {
    private static final int LOG_PARTS = 4;
    protected final int index;
    protected final long segment;
    protected final long minNodeId;
    protected final NodeManager nodeManager;
    protected final PersistenceManager persistenceManager;
    protected final long maxNodeId;
    private static Field persistenceManagerField = getPersistenceManagerField();

    private static Field getPersistenceManagerField() {
        try {
            final Field field = NodeManager.class.getDeclaredField("persistenceManager");
            field.setAccessible(true);
            return field;
        } catch (Exception e) {
            throw new RuntimeException("Error retrieving NodeManager.persistenceManager", e);
        }
    }

    public Preloader(long segment, long minNodeId,
                     long nodeCount, int index, DependencyResolver dependencyResolver) {
        this.nodeManager = dependencyResolver.resolveDependency(NodeManager.class);
        this.persistenceManager = getPersistenceManager(nodeManager);
        this.segment = segment;
        this.minNodeId = minNodeId;
        this.maxNodeId = minNodeId + nodeCount;
        this.index = index;
    }

    private PersistenceManager getPersistenceManager(NodeManager nodeManager) {
        try {
            return (PersistenceManager) persistenceManagerField.get(nodeManager);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public Integer call() throws Exception {
        final long start = index * segment;
        final long end = (index + 1) * segment;
        long fragment = segment / LOG_PARTS;
        long logAt = start + fragment;
        System.out.printf("%2d. %s Loading from %10d up to %10d%n", index, getClass().getSimpleName(), start, end);
        int count = 0;
        for (long id = start; id < end; id++) {
            count += doLoad(id);
            if (id > logAt) {
                System.out.printf("%2d. %3d%%%n", index, 100 / LOG_PARTS * (id - start) / fragment);
                System.out.flush();
                logAt += fragment;
            }
        }
        System.out.printf("%2d. %s 100%% Done, loaded %d %n", index, getClass().getSimpleName(), count);
        return count;
    }

    protected abstract int doLoad(long id);

    protected boolean isInNodeRange(long id) {
        return id >= minNodeId && id < maxNodeId;
    }
}
