package org.apache.atlas.repository.graphdb.migrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe metrics tracking for the migration process.
 * Provides live progress reporting with rates, ETA, and queue depth.
 */
public class MigrationMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationMetrics.class);

    private final AtomicLong verticesScanned  = new AtomicLong(0);
    private final AtomicLong verticesSkipped  = new AtomicLong(0);
    private final AtomicLong verticesWritten  = new AtomicLong(0);
    private final AtomicLong edgesWritten     = new AtomicLong(0);
    private final AtomicLong indexesWritten   = new AtomicLong(0);
    private final AtomicLong typeDefsWritten  = new AtomicLong(0);
    private final AtomicLong esDocsIndexed    = new AtomicLong(0);
    private final AtomicLong decodeErrors     = new AtomicLong(0);
    private final AtomicLong writeErrors      = new AtomicLong(0);
    private final AtomicLong tokenRangesTotal = new AtomicLong(0);
    private final AtomicLong tokenRangesDone  = new AtomicLong(0);
    private final AtomicLong cqlRowsRead      = new AtomicLong(0);

    // For rate calculation (delta between log intervals)
    private volatile long prevVerticesWritten = 0;
    private volatile long prevEdgesWritten    = 0;
    private volatile long prevCqlRowsRead     = 0;
    private volatile long prevLogTimeMs       = 0;

    // Queue depth (set externally by the writer)
    private volatile int queueDepth = 0;
    private volatile int queueCapacity = 0;

    private volatile long startTimeMs;

    public void start() {
        this.startTimeMs = System.currentTimeMillis();
        this.prevLogTimeMs = startTimeMs;
    }

    public void incrVerticesScanned()           { verticesScanned.incrementAndGet(); }
    public void incrVerticesSkipped()            { verticesSkipped.incrementAndGet(); }
    public void incrVerticesWritten()            { verticesWritten.incrementAndGet(); }
    public void incrEdgesWritten(long count)     { edgesWritten.addAndGet(count); }
    public void incrIndexesWritten(long count)   { indexesWritten.addAndGet(count); }
    public void incrTypeDefsWritten()            { typeDefsWritten.incrementAndGet(); }
    public void incrEsDocsIndexed(long count)    { esDocsIndexed.addAndGet(count); }
    public void incrDecodeErrors()               { decodeErrors.incrementAndGet(); }
    public void incrWriteErrors()                { writeErrors.incrementAndGet(); }
    public void incrCqlRowsRead(long count)      { cqlRowsRead.addAndGet(count); }
    public void setTokenRangesTotal(long total)  { tokenRangesTotal.set(total); }
    public void incrTokenRangesDone()            { tokenRangesDone.incrementAndGet(); }
    public void setQueueDepth(int depth)         { this.queueDepth = depth; }
    public void setQueueCapacity(int capacity)   { this.queueCapacity = capacity; }

    public long getVerticesScanned()  { return verticesScanned.get(); }
    public long getVerticesSkipped()  { return verticesSkipped.get(); }
    public long getVerticesWritten()  { return verticesWritten.get(); }
    public long getEdgesWritten()     { return edgesWritten.get(); }
    public long getIndexesWritten()   { return indexesWritten.get(); }
    public long getTypeDefsWritten()  { return typeDefsWritten.get(); }
    public long getEsDocsIndexed()    { return esDocsIndexed.get(); }
    public long getDecodeErrors()     { return decodeErrors.get(); }
    public long getWriteErrors()      { return writeErrors.get(); }
    public long getCqlRowsRead()       { return cqlRowsRead.get(); }
    public long getTokenRangesDone()  { return tokenRangesDone.get(); }
    public long getTokenRangesTotal() { return tokenRangesTotal.get(); }

    public double getElapsedSeconds() {
        return (System.currentTimeMillis() - startTimeMs) / 1000.0;
    }

    public void logProgress() {
        long now = System.currentTimeMillis();
        double elapsed = (now - startTimeMs) / 1000.0;
        double intervalSec = (now - prevLogTimeMs) / 1000.0;
        if (intervalSec <= 0) intervalSec = 1;

        long curVerticesWritten = verticesWritten.get();
        long curEdgesWritten    = edgesWritten.get();
        long curCqlRows         = cqlRowsRead.get();
        long rangesDone         = tokenRangesDone.get();
        long rangesTotal        = tokenRangesTotal.get();

        // Interval rates (current throughput)
        double vertexRate = (curVerticesWritten - prevVerticesWritten) / intervalSec;
        double edgeRate   = (curEdgesWritten - prevEdgesWritten) / intervalSec;
        double rowRate    = (curCqlRows - prevCqlRowsRead) / intervalSec;

        // Overall average rates
        double avgVertexRate = elapsed > 0 ? curVerticesWritten / elapsed : 0;

        // ETA based on token range progress
        String eta = "unknown";
        if (rangesDone > 0 && rangesTotal > 0) {
            double pct = (double) rangesDone / rangesTotal * 100.0;
            double secsPerRange = elapsed / rangesDone;
            long remainingRanges = rangesTotal - rangesDone;
            long etaSec = (long) (remainingRanges * secsPerRange);
            eta = String.format("%.1f%% done, ~%s remaining", pct, formatDuration(etaSec));
        }

        LOG.info("=== Migration Progress [{} elapsed] ===", formatDuration((long) elapsed));
        LOG.info("  Token ranges: {}/{} ({})", rangesDone, rangesTotal, eta);
        LOG.info("  CQL rows read: {} (current: {}/s, avg: {}/s)",
                 format(curCqlRows), format((long) rowRate), format((long)(elapsed > 0 ? curCqlRows / elapsed : 0)));
        long skipped = verticesSkipped.get();
        if (skipped > 0) {
            LOG.info("  Vertices: scanned={}, written={}, skipped={} (current: {}/s, avg: {}/s)",
                     format(verticesScanned.get()), format(curVerticesWritten), format(skipped),
                     format((long) vertexRate), format((long) avgVertexRate));
        } else {
            LOG.info("  Vertices: scanned={}, written={} (current: {}/s, avg: {}/s)",
                     format(verticesScanned.get()), format(curVerticesWritten),
                     format((long) vertexRate), format((long) avgVertexRate));
        }
        LOG.info("  Edges written: {} (current: {}/s) | Indexes: {}",
                 format(curEdgesWritten), format((long) edgeRate), format(indexesWritten.get()));
        if (queueCapacity > 0) {
            LOG.info("  Writer queue: {}/{} ({}% full)",
                     queueDepth, queueCapacity,
                     queueCapacity > 0 ? (queueDepth * 100 / queueCapacity) : 0);
        }
        LOG.info("  ES docs indexed: {}", format(esDocsIndexed.get()));
        if (decodeErrors.get() > 0 || writeErrors.get() > 0) {
            LOG.warn("  Errors: decode={}, write={}", decodeErrors.get(), writeErrors.get());
        }

        prevVerticesWritten = curVerticesWritten;
        prevEdgesWritten    = curEdgesWritten;
        prevCqlRowsRead     = curCqlRows;
        prevLogTimeMs       = now;
    }

    /** Formatted summary string for the final report */
    public String summary() {
        double elapsed = getElapsedSeconds();
        double vertexRate = elapsed > 0 ? verticesWritten.get() / elapsed : 0;
        long skipped = verticesSkipped.get();
        String skipSuffix = skipped > 0 ? String.format(", Skipped: %s", format(skipped)) : "";
        return String.format(
            "Migration complete in %s — Vertices: %s%s, Edges: %s, Indexes: %s, TypeDefs: %s, ES docs: %s | " +
            "Avg rate: %s vertices/s | Decode errors: %d, Write errors: %d | CQL rows: %s",
            formatDuration((long) elapsed),
            format(verticesWritten.get()), skipSuffix, format(edgesWritten.get()),
            format(indexesWritten.get()), format(typeDefsWritten.get()), format(esDocsIndexed.get()),
            format((long) vertexRate),
            decodeErrors.get(), writeErrors.get(),
            format(cqlRowsRead.get()));
    }

    /** Format large numbers with commas: 1234567 -> "1,234,567" */
    private static String format(long value) {
        return String.format("%,d", value);
    }

    /** Format seconds into human-readable duration: 3661 -> "1h 1m 1s" */
    static String formatDuration(long totalSeconds) {
        if (totalSeconds < 60) return totalSeconds + "s";
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        if (hours > 0) return String.format("%dh %dm %ds", hours, minutes, seconds);
        return String.format("%dm %ds", minutes, seconds);
    }
}
