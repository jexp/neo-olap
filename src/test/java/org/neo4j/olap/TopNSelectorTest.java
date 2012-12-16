package org.neo4j.olap;

import org.junit.Test;
import org.neo4j.helpers.Pair;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 12.12.12
 */
public class TopNSelectorTest {
    @Test
    public void testSelectTopOne() throws Exception {
        final int[] data = {6, 3, 1, 9};
        final TopNSelector selector = new TopNSelector(data);
        final List<Pair<Integer,Integer>> pairs = selector.selectTopN(1);
        assertEquals(1,pairs.size());
        assertEquals(3, (int)pairs.get(0).first());
        assertEquals(9, (int)pairs.get(0).other());
    }
    @Test
    public void testSelectTopThree() throws Exception {
        final int[] data = {6, 3, 1, 9};
        final TopNSelector selector = new TopNSelector(data);
        final List<Pair<Integer,Integer>> pairs = selector.selectTopN(3);
        assertEquals(3,pairs.size());
        assertEquals(3, (int)pairs.get(0).first());
        assertEquals(9, (int)pairs.get(0).other());
        assertEquals(0, (int)pairs.get(1).first());
        assertEquals(6, (int)pairs.get(1).other());
        assertEquals(1, (int)pairs.get(2).first());
        assertEquals(3, (int)pairs.get(2).other());
    }
   
    @Test
    public void testSelectTop3OfMany() throws Exception {
        final int[] data = new int[10000];
        for (int i = 0; i < data.length; i++) {
            data[i]= (int) (Math.random()*100000);
        }
        data[100]=100001;
        data[1000]=100002;
        data[2000]=100003;

        final TopNSelector selector = new TopNSelector(data);
        final List<Pair<Integer,Integer>> pairs = selector.selectTopN(3);
        assertEquals(3,pairs.size());
        assertEquals(2000, (int)pairs.get(0).first());
        assertEquals(100003, (int)pairs.get(0).other());
        assertEquals(1000, (int)pairs.get(1).first());
        assertEquals(100002, (int)pairs.get(1).other());
        assertEquals(100, (int) pairs.get(2).first());
        assertEquals(100001, (int)pairs.get(2).other());
    }
}
