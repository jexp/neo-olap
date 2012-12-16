package org.neo4j.olap;

import org.neo4j.helpers.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
* @author mh
* @since 12.12.12
*/
public class TopNSelector {
    private final int[] nodes;
    private final long maxNodeId;

    public TopNSelector(int[] nodes) {
        this.nodes = nodes;
        this.maxNodeId = nodes.length;
    }

    List<Pair<Integer,Integer>> selectTopN(int howMany) {
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
        List<Pair<Integer,Integer>> result = new ArrayList<Pair<Integer, Integer>>(howMany);
        for (int i=0;i<howMany;i++) {
            result.add(Pair.of(ids[i],counts[i]));
        }
        Collections.sort(result, new Comparator<Pair<Integer, Integer>>() {
            @Override
            public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                return o2.other().compareTo(o1.other());
            }
        });
        return result;
    }
}
