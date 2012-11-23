package org.neo4j.olap;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 23.11.12
 */
public class ArrayStoreTest {
    @Test
    public void testWriteArray() throws Exception {
        final String file = "target/test.ints";
        final ArrayStore store = new ArrayStore(file, 5);
        final int[] data = new int[10];
        Arrays.fill(data,42);
        store.write(data);
        assertEquals((data.length+1)*Integer.SIZE/8,new File(file).length());
        final int[] read = store.read();
        assertArrayEquals(data,read);
    }

    @Test
    public void testWriteLargeArray() throws Exception {
        final String file = "target/test.ints";
        final ArrayStore store = new ArrayStore(file);
        final int[] data = new int[ArrayStore.MEGABYTE*25];
        Arrays.fill(data,42);
        store.write(data);
        assertEquals((data.length+1)*(Integer.SIZE/8),new File(file).length());
        final int[] read = store.read();
        assertArrayEquals(data,read);
    }
}
