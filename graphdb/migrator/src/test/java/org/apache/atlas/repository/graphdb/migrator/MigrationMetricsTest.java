package org.apache.atlas.repository.graphdb.migrator;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class MigrationMetricsTest {

    private MigrationMetrics metrics;

    @BeforeMethod
    public void setUp() {
        metrics = new MigrationMetrics();
    }

    @Test
    public void testInitialValuesAreZero() {
        assertEquals(metrics.getVerticesScanned(), 0);
        assertEquals(metrics.getVerticesWritten(), 0);
        assertEquals(metrics.getEdgesWritten(), 0);
        assertEquals(metrics.getEsDocsIndexed(), 0);
        assertEquals(metrics.getDecodeErrors(), 0);
        assertEquals(metrics.getWriteErrors(), 0);
        assertEquals(metrics.getCqlRowsRead(), 0);
    }

    @Test
    public void testIncrVerticesScanned() {
        metrics.incrVerticesScanned();
        metrics.incrVerticesScanned();
        metrics.incrVerticesScanned();
        assertEquals(metrics.getVerticesScanned(), 3);
    }

    @Test
    public void testIncrVerticesWritten() {
        metrics.incrVerticesWritten();
        assertEquals(metrics.getVerticesWritten(), 1);
    }

    @Test
    public void testIncrEdgesWritten() {
        metrics.incrEdgesWritten(5);
        metrics.incrEdgesWritten(3);
        assertEquals(metrics.getEdgesWritten(), 8);
    }

    @Test
    public void testIncrEsDocsIndexed() {
        metrics.incrEsDocsIndexed(100);
        metrics.incrEsDocsIndexed(200);
        assertEquals(metrics.getEsDocsIndexed(), 300);
    }

    @Test
    public void testIncrDecodeErrors() {
        metrics.incrDecodeErrors();
        metrics.incrDecodeErrors();
        assertEquals(metrics.getDecodeErrors(), 2);
    }

    @Test
    public void testIncrWriteErrors() {
        metrics.incrWriteErrors();
        assertEquals(metrics.getWriteErrors(), 1);
    }

    @Test
    public void testIncrCqlRowsRead() {
        metrics.incrCqlRowsRead(1000);
        metrics.incrCqlRowsRead(500);
        assertEquals(metrics.getCqlRowsRead(), 1500);
    }

    @Test
    public void testSetTokenRangesTotal() {
        metrics.setTokenRangesTotal(32);
        // tokenRangesTotal has no getter in the public API, but logProgress uses it
        // The fact it doesn't throw is the test
    }

    @Test
    public void testIncrTokenRangesDone() {
        metrics.incrTokenRangesDone();
        metrics.incrTokenRangesDone();
        // Similar to above, no direct getter, but logProgress uses it
    }

    @Test
    public void testStartAndElapsedSeconds() {
        metrics.start();
        // elapsed should be >= 0 and very small
        double elapsed = metrics.getElapsedSeconds();
        assertTrue(elapsed >= 0);
        assertTrue(elapsed < 5.0); // Should be almost instant
    }

    @Test
    public void testSummaryFormat() {
        metrics.start();
        metrics.incrVerticesWritten();
        metrics.incrVerticesWritten();
        metrics.incrEdgesWritten(3);
        metrics.incrEsDocsIndexed(10);
        metrics.incrCqlRowsRead(100);

        String summary = metrics.summary();
        assertNotNull(summary);
        assertTrue(summary.contains("Vertices: 2"));
        assertTrue(summary.contains("Edges: 3"));
        assertTrue(summary.contains("ES docs: 10"));
        assertTrue(summary.contains("CQL rows: 100"));
        assertTrue(summary.contains("Decode errors: 0"));
        assertTrue(summary.contains("Write errors: 0"));
    }

    @Test
    public void testSummaryWithErrors() {
        metrics.start();
        metrics.incrDecodeErrors();
        metrics.incrWriteErrors();
        metrics.incrWriteErrors();

        String summary = metrics.summary();
        assertTrue(summary.contains("Decode errors: 1"));
        assertTrue(summary.contains("Write errors: 2"));
    }

    @Test
    public void testLogProgressDoesNotThrow() {
        metrics.start();
        metrics.setTokenRangesTotal(10);
        metrics.incrTokenRangesDone();
        metrics.incrVerticesScanned();
        metrics.incrCqlRowsRead(100);

        // Should not throw
        metrics.logProgress();
    }

    @Test
    public void testThreadSafety() throws Exception {
        int numThreads = 10;
        int incrementsPerThread = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            pool.submit(() -> {
                try {
                    for (int j = 0; j < incrementsPerThread; j++) {
                        metrics.incrVerticesScanned();
                        metrics.incrVerticesWritten();
                        metrics.incrEdgesWritten(1);
                        metrics.incrCqlRowsRead(1);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        long expected = (long) numThreads * incrementsPerThread;
        assertEquals(metrics.getVerticesScanned(), expected);
        assertEquals(metrics.getVerticesWritten(), expected);
        assertEquals(metrics.getEdgesWritten(), expected);
        assertEquals(metrics.getCqlRowsRead(), expected);
    }
}
