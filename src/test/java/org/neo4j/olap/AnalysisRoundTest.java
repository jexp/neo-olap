package org.neo4j.olap;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 16.12.12
 */
public class AnalysisRoundTest {

    private static final int MAX_NODE_ID = 200;

    @Test
    public void testParseFileName() throws Exception {
        final Runner runner = new Runner(null) {
            @Override
            public void runAnalysis(String file) throws Exception {
                final AnalysisRound round = new AnalysisRound(file, MAX_NODE_ID).initialize();
                assertEquals(10,round.getMinNodeId());
                assertEquals(100, round.getNodesPerRound());
                assertEquals(MAX_NODE_ID, round.getMaxNodeId());
                assertEquals(42, round.getNodes().length);
            }

            @Override
            protected int[] loadArray(String fileName) throws IOException {
                return new int[42];
            }
        };
        runner.runAnalysis("page_rank_10_100.int");
    }
    @Test
    public void testNoFileUseDefaults() throws Exception {
        final Runner runner = new Runner(null) {
            @Override
            public void runAnalysis(String file) throws Exception {
                final AnalysisRound round = new AnalysisRound(file, MAX_NODE_ID).initialize();
                assertEquals(0,round.getMinNodeId());
                assertEquals(MAX_NODE_ID, round.getNodesPerRound());
                assertEquals(MAX_NODE_ID, round.getMaxNodeId());
                assertEquals(MAX_NODE_ID, round.getNodes().length);
            }

            @Override
            protected int[] loadArray(String fileName) throws IOException {
                throw new UnsupportedOperationException();
            }
        };
        runner.runAnalysis(null);
    }
}
