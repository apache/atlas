package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.RelationType;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Hybrid CQL Scanner: scans JanusGraph's edgestore table directly via CQL,
 * then uses JanusGraph's own EdgeSerializer to decode the binary data.
 *
 * This gives CQL-speed reads with correct JanusGraph decoding — validated by
 * JanusGraph's own OLAP framework (VertexProgramScanJob / VertexJobConverter).
 *
 * Architecture:
 *   CQL token-range scan  →  JG EdgeSerializer.parseRelation()  →  DecodedVertex
 *
 * Each edge is stored in both vertices' adjacency lists in JanusGraph.
 * When edgesOutOnly is enabled (default), we only process OUT-direction edges
 * to ensure exactly-once processing and halve edge mutations.
 */
public class JanusGraphScanner implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphScanner.class);

    private final MigratorConfig   config;
    private final MigrationMetrics metrics;
    private final CqlSession       sourceSession;

    // JanusGraph internals for decoding
    private final StandardJanusGraph   janusGraph;
    private final IDManager            idManager;
    private final EdgeSerializer       edgeSerializer;
    private final ThreadLocal<StandardJanusGraphTx> threadLocalTx;

    /** Prefix used by Atlas for JanusGraph properties in atlas-application.properties */
    private static final String ATLAS_GRAPH_PREFIX = "atlas.graph.";

    public JanusGraphScanner(MigratorConfig config, MigrationMetrics metrics, CqlSession sourceSession) {
        this.config        = config;
        this.metrics       = metrics;
        this.sourceSession = sourceSession;

        // Open JanusGraph instance ONLY for schema/type resolution (read-only).
        // Atlas stores JanusGraph config under "atlas.graph." prefix in atlas-application.properties.
        // JanusGraph expects keys without this prefix (e.g., "storage.backend" not "atlas.graph.storage.backend").
        // We strip the prefix and add Atlas's custom serializers, matching AtlasJanusGraphDatabase.getConfiguration().
        LOG.info("Opening JanusGraph for schema resolution from config: {}",
                 config.getSourceJanusGraphConfig());

        Configuration janusConfig = buildJanusGraphConfig(config.getSourceJanusGraphConfig());

        JanusGraph jg = JanusGraphFactory.open(janusConfig);
        this.janusGraph     = (StandardJanusGraph) jg;
        this.idManager      = janusGraph.getIDManager();
        this.edgeSerializer = janusGraph.getEdgeSerializer();

        // Each scanner thread gets its own read-only tx for thread-safe type resolution.
        // The tx caches type definitions after first lookup.
        this.threadLocalTx = ThreadLocal.withInitial(() ->
            (StandardJanusGraphTx) janusGraph.buildTransaction()
                .readOnly()
                .vertexCacheSize(200)
                .start()
        );
    }

    /**
     * Load a config file and produce a JanusGraph-compatible Configuration.
     *
     * Handles two formats:
     *   1) atlas-application.properties: keys prefixed with "atlas.graph." (e.g., atlas.graph.storage.backend=cql)
     *   2) Plain JanusGraph config: keys without prefix (e.g., storage.backend=cql)
     *
     * Detects format by checking if "atlas.graph.storage.backend" exists.
     * Then adds Atlas's 4 custom serializers (TypeCategory, ArrayList, BigInteger, BigDecimal).
     */
    private static Configuration buildJanusGraphConfig(String configPath) {
        Properties fileProps = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            fileProps.load(fis);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config: " + configPath, e);
        }

        BaseConfiguration janusConfig = new BaseConfiguration();

        // Detect whether this is an Atlas config (atlas.graph.* prefix) or plain JanusGraph config
        boolean isAtlasFormat = fileProps.containsKey("atlas.graph.storage.backend");

        if (isAtlasFormat) {
            LOG.info("Detected Atlas-format config (atlas.graph.* prefix), stripping prefix");
            for (String key : fileProps.stringPropertyNames()) {
                if (key.startsWith(ATLAS_GRAPH_PREFIX)) {
                    String janusKey = key.substring(ATLAS_GRAPH_PREFIX.length());
                    janusConfig.setProperty(janusKey, fileProps.getProperty(key));
                }
            }
        } else {
            LOG.info("Detected plain JanusGraph config (no prefix)");
            for (String key : fileProps.stringPropertyNames()) {
                janusConfig.setProperty(key, fileProps.getProperty(key));
            }
        }

        // Add Atlas's custom serializers — MUST match AtlasJanusGraphDatabase exactly (lines 108-119).
        // Using different class names corrupts JanusGraph's schema type ID resolution,
        // causing all property key names to be systematically wrong.
        //
        // Only set custom attributes if the config file didn't already include them
        // (i.e., if we stripped "atlas.graph." prefix and they came through).
        if (!janusConfig.containsKey("attributes.custom.attribute1.attribute-class")) {
            // Use the EXACT same classes as AtlasJanusGraphDatabase:
            //   attribute1 = org.apache.atlas.typesystem.types.DataTypes$TypeCategory
            //   attribute2 = java.util.ArrayList  (serialized with JanusGraph's own SerializableSerializer)
            //   attribute3 = java.math.BigInteger
            //   attribute4 = java.math.BigDecimal
            janusConfig.setProperty("attributes.custom.attribute1.attribute-class",
                "org.apache.atlas.typesystem.types.DataTypes$TypeCategory");
            janusConfig.setProperty("attributes.custom.attribute1.serializer-class",
                "org.apache.atlas.repository.graphdb.janus.serializer.TypeCategorySerializer");

            janusConfig.setProperty("attributes.custom.attribute2.attribute-class",
                ArrayList.class.getName());
            janusConfig.setProperty("attributes.custom.attribute2.serializer-class",
                "org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer");

            janusConfig.setProperty("attributes.custom.attribute3.attribute-class",
                BigInteger.class.getName());
            janusConfig.setProperty("attributes.custom.attribute3.serializer-class",
                "org.apache.atlas.repository.graphdb.janus.serializer.BigIntegerSerializer");

            janusConfig.setProperty("attributes.custom.attribute4.attribute-class",
                BigDecimal.class.getName());
            janusConfig.setProperty("attributes.custom.attribute4.serializer-class",
                "org.apache.atlas.repository.graphdb.janus.serializer.BigDecimalSerializer");
        } else {
            LOG.info("Custom attribute serializers already configured from config file — not overriding");
        }

        LOG.info("JanusGraph config: storage.backend={}, storage.cql.keyspace={}, storage.hostname={}",
                 janusConfig.getString("storage.backend"),
                 janusConfig.getString("storage.cql.keyspace"),
                 janusConfig.getString("storage.hostname"));

        return janusConfig;
    }

    /**
     * Split the Murmur3 token range [-2^63, 2^63-1] into N equal segments.
     */
    public List<long[]> splitTokenRanges(int numRanges) {
        BigInteger minToken = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger maxToken = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger totalRange = maxToken.subtract(minToken).add(BigInteger.ONE);
        BigInteger rangeSize  = totalRange.divide(BigInteger.valueOf(numRanges));

        List<long[]> ranges = new ArrayList<>(numRanges);
        BigInteger current = minToken;

        for (int i = 0; i < numRanges; i++) {
            BigInteger end = (i == numRanges - 1) ? maxToken : current.add(rangeSize).subtract(BigInteger.ONE);
            ranges.add(new long[]{current.longValueExact(), end.longValueExact()});
            current = end.add(BigInteger.ONE);
        }

        LOG.info("Split token range into {} segments", ranges.size());
        return ranges;
    }

    /**
     * Scan all token ranges in parallel, decode vertices, and feed them to the consumer.
     *
     * @param consumer    receives each decoded vertex
     * @param stateStore  for resume support (skip completed ranges)
     * @param phase       migration phase name for state tracking
     */
    public void scanAll(Consumer<DecodedVertex> consumer, MigrationStateStore stateStore, String phase) {
        List<long[]> tokenRanges = splitTokenRanges(config.getScannerThreads());
        metrics.setTokenRangesTotal(tokenRanges.size());

        Set<Long> completedRanges = config.isResume()
            ? stateStore.getCompletedRanges(phase)
            : Collections.emptySet();

        String edgestoreTable = config.getSourceCassandraKeyspace() + "." + config.getSourceEdgestoreTable();

        PreparedStatement scanStmt = sourceSession.prepare(
            "SELECT key, column1, value FROM " + edgestoreTable +
            " WHERE token(key) >= ? AND token(key) <= ?");

        ExecutorService scannerPool = Executors.newFixedThreadPool(config.getScannerThreads(),
            r -> {
                Thread t = new Thread(r);
                t.setName("scanner-" + t.getId());
                t.setDaemon(true);
                return t;
            });

        List<Future<?>> futures = new ArrayList<>();

        for (long[] range : tokenRanges) {
            long rangeStart = range[0];
            long rangeEnd   = range[1];

            if (completedRanges.contains(rangeStart)) {
                LOG.info("Skipping already-completed token range [{}, {}] (resume mode)", rangeStart, rangeEnd);
                metrics.incrTokenRangesDone();
                continue;
            }

            futures.add(scannerPool.submit(() -> {
                try {
                    scanTokenRange(scanStmt, rangeStart, rangeEnd, consumer, stateStore, phase);
                } catch (Exception e) {
                    LOG.error("Failed scanning token range [{}, {}]", rangeStart, rangeEnd, e);
                    stateStore.markRangeFailed(phase, rangeStart, rangeEnd);
                }
            }));
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Scanner interrupted", e);
            } catch (ExecutionException e) {
                LOG.error("Scanner task failed", e.getCause());
            }
        }

        scannerPool.shutdown();
        LOG.info("All {} token ranges scanned — total CQL rows: {}, vertices scanned: {}, decode errors: {}",
                 tokenRanges.size(),
                 String.format("%,d", metrics.getCqlRowsRead()),
                 String.format("%,d", metrics.getVerticesScanned()),
                 metrics.getDecodeErrors());
        LOG.info("DIAG SUMMARY: edges decoded: {}, properties decoded: {}, system relations skipped: {}, " +
                 "null-type relations skipped: {}, edge decode errors: {}",
                 edgeDecodeCount.get(), propertyDecodeCount.get(), edgeSkippedSystem.get(),
                 edgeSkippedNullType.get(), edgeDecodeErrors.get());
    }

    /**
     * Scan a single token range. Groups CQL rows by vertex key,
     * decodes each vertex's full adjacency list, and passes to consumer.
     */
    private void scanTokenRange(PreparedStatement scanStmt, long rangeStart, long rangeEnd,
                                 Consumer<DecodedVertex> consumer,
                                 MigrationStateStore stateStore, String phase) {
        stateStore.markRangeStarted(phase, rangeStart, rangeEnd);

        ResultSet rs = sourceSession.execute(
            scanStmt.bind(rangeStart, rangeEnd)
                    .setPageSize(config.getScanFetchSize()));

        ByteBuffer currentKey = null;
        long currentVertexId = -1;
        List<Entry> currentEntries = new ArrayList<>(64);
        long rangeVertices = 0;
        long rangeEdges = 0;
        long rowCount = 0;

        for (Row row : rs) {
            rowCount++;
            ByteBuffer keyBuf = row.getByteBuffer("key");
            ByteBuffer colBuf = row.getByteBuffer("column1");
            ByteBuffer valBuf = row.getByteBuffer("value");

            if (currentKey == null || !keyBuf.equals(currentKey)) {
                // New vertex — flush the previous one
                if (currentKey != null && !currentEntries.isEmpty()) {
                    DecodedVertex decoded = decodeVertex(currentVertexId, currentEntries);
                    if (decoded != null) {
                        if (shouldSkipVertex(decoded)) {
                            metrics.incrVerticesSkipped();
                        } else {
                            consumer.accept(decoded);
                            rangeVertices++;
                            rangeEdges += decoded.getOutEdges().size();
                        }
                    }
                }

                currentKey = keyBuf.duplicate();
                currentVertexId = extractVertexId(keyBuf);
                currentEntries.clear();
            }

            Entry entry = buildEntry(colBuf, valBuf);
            if (entry != null) {
                currentEntries.add(entry);
            }
        }

        // Process the last vertex in this range
        if (currentKey != null && !currentEntries.isEmpty()) {
            DecodedVertex decoded = decodeVertex(currentVertexId, currentEntries);
            if (decoded != null) {
                if (shouldSkipVertex(decoded)) {
                    metrics.incrVerticesSkipped();
                } else {
                    consumer.accept(decoded);
                    rangeVertices++;
                    rangeEdges += decoded.getOutEdges().size();
                }
            }
        }

        metrics.incrCqlRowsRead(rowCount);
        stateStore.markRangeCompleted(phase, rangeStart, rangeEnd, rangeVertices, rangeEdges);
        metrics.incrTokenRangesDone();

        LOG.info("Token range completed: [{}, {}] — {} CQL rows, {} vertices, {} edges (ranges done: {}/{})",
                  rangeStart, rangeEnd, rowCount, rangeVertices, rangeEdges,
                  metrics.getTokenRangesDone(), metrics.getTokenRangesTotal());
    }

    private long extractVertexId(ByteBuffer keyBuf) {
        byte[] bytes = new byte[keyBuf.remaining()];
        keyBuf.duplicate().get(bytes);
        StaticBuffer key = new StaticArrayBuffer(bytes);
        Object id = idManager.getKeyID(key);
        return ((Number) id).longValue();
    }

    /**
     * Combine column1 + value into a JanusGraph Entry (the format EdgeSerializer expects).
     * See: JanusGraph's VertexJobConverter and the Scala decoder gist.
     */
    private Entry buildEntry(ByteBuffer colBuf, ByteBuffer valBuf) {
        try {
            byte[] colBytes = new byte[colBuf.remaining()];
            colBuf.duplicate().get(colBytes);

            byte[] valBytes = new byte[valBuf.remaining()];
            valBuf.duplicate().get(valBytes);

            WriteByteBuffer wb = new WriteByteBuffer(colBytes.length + valBytes.length);
            wb.putBytes(colBytes);
            int valuePos = wb.getPosition();
            wb.putBytes(valBytes);

            return new StaticArrayEntry(wb.getStaticBuffer(), valuePos);
        } catch (Exception e) {
            LOG.trace("Failed to build entry", e);
            return null;
        }
    }

    // Counters for edge diagnostic logging (log first N edges at INFO level)
    private final java.util.concurrent.atomic.AtomicLong edgeDecodeCount = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong edgeSkippedSystem = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong edgeSkippedNullType = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong edgeDecodeErrors = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong propertyDecodeCount = new java.util.concurrent.atomic.AtomicLong(0);
    private static final int EDGE_SAMPLE_LOG_LIMIT = 20;

    /**
     * Check if a decoded vertex should be skipped based on config flags.
     * Classification vertices have __entityGuid (Constants.CLASSIFICATION_ENTITY_GUID).
     * Task vertices have __task_guid (Constants.TASK_GUID).
     */
    private boolean shouldSkipVertex(DecodedVertex vertex) {
        if (config.isSkipClassifications() && vertex.getProperties().containsKey("__entityGuid")) {
            return true;
        }
        if (config.isSkipTasks() && vertex.getProperties().containsKey("__task_guid")) {
            return true;
        }
        return false;
    }

    /**
     * Decode a vertex's full adjacency list from JanusGraph's binary format.
     * Extracts properties and edges.
     *
     * Each edge in JanusGraph is stored twice in the edgestore — once in each
     * endpoint's adjacency list. When edgesOutOnly is enabled (default), we skip
     * IN-direction edges since the same edge will be processed from the other
     * endpoint's OUT list. This halves edge mutations without losing data.
     */
    private DecodedVertex decodeVertex(long vertexId, List<Entry> entries) {
        // Skip invisible/internal JanusGraph system vertices
        try {
            if (IDManager.VertexIDType.Invisible.is(vertexId)) {
                return null;
            }
        } catch (Exception e) {
            // If check fails, process the vertex anyway
        }

        DecodedVertex vertex = new DecodedVertex(vertexId);
        StandardJanusGraphTx tx = threadLocalTx.get();
        int entryEdges = 0;
        int entryProps = 0;
        int entrySystem = 0;
        int entryNullType = 0;
        int entryErrors = 0;

        for (Entry entry : entries) {
            try {
                RelationCache rel = edgeSerializer.parseRelation(entry, false, tx);

                // Skip JanusGraph internal system relations (VertexExists, SchemaName, etc.)
                if (isSystemRelation(rel.typeId)) {
                    entrySystem++;
                    continue;
                }

                RelationType type = tx.getExistingRelationType(rel.typeId);
                if (type == null) {
                    entryNullType++;
                    if (edgeSkippedNullType.incrementAndGet() <= EDGE_SAMPLE_LOG_LIMIT) {
                        LOG.info("DIAG: Skipping relation with null type: typeId={}, vertexId={}, direction={}",
                                 rel.typeId, vertexId, rel.direction);
                    }
                    continue;
                }

                String relTypeName = normalizePropertyName(type.name());

                if (type.isPropertyKey()) {
                    Object value = rel.getValue();
                    if (value != null) {
                        vertex.addProperty(relTypeName, value);
                        entryProps++;
                    }
                } else {
                    // Edge processing. Each edge is stored twice in JG (once per
                    // endpoint). With OUT-only mode, skip IN edges — they'll be
                    // written when we process the other endpoint's OUT list.
                    long otherVertexId = ((Number) rel.getOtherVertexId()).longValue();
                    long relationId    = rel.relationId;
                    Direction dir      = rel.direction;

                    if (config.isEdgesOutOnly() && dir == Direction.IN) {
                        continue;
                    }

                    long outVertexId;
                    long inVertexId;

                    if (dir == Direction.IN) {
                        outVertexId = otherVertexId;
                        inVertexId  = vertexId;
                    } else {
                        outVertexId = vertexId;
                        inVertexId  = otherVertexId;
                    }

                    DecodedEdge edge = new DecodedEdge(
                        relationId, outVertexId, inVertexId, relTypeName);

                    // Extract edge properties via RelationCache iteration.
                    // RelationCache implements Iterable<LongObjectCursor<Object>>
                    // where key = property type ID, value = property value.
                    extractEdgeProperties(edge, rel, tx);

                    vertex.addOutEdge(edge);
                    entryEdges++;

                    // Log first N edges at INFO for diagnostics
                    long totalEdges = edgeDecodeCount.incrementAndGet();
                    if (totalEdges <= EDGE_SAMPLE_LOG_LIMIT) {
                        LOG.info("DIAG: Edge decoded #{}: {} -[{}]-> {} (relId={}, dir={}, props={})",
                                 totalEdges, outVertexId, relTypeName, inVertexId,
                                 relationId, dir, edge.getProperties().size());
                    }
                }
            } catch (Exception e) {
                entryErrors++;
                edgeDecodeErrors.incrementAndGet();
                metrics.incrDecodeErrors();
                // Log at WARN (not TRACE) to make decode errors visible
                if (edgeDecodeErrors.get() <= EDGE_SAMPLE_LOG_LIMIT) {
                    LOG.warn("Decode error for vertex {} entry: {}", vertexId, e.toString());
                } else {
                    LOG.trace("Decode error for vertex {} entry", vertexId, e);
                }
            }
        }

        // Log per-vertex breakdown for first few vertices that have edges
        if (entryEdges > 0 && edgeDecodeCount.get() <= EDGE_SAMPLE_LOG_LIMIT * 2) {
            LOG.info("DIAG: Vertex {} decoded: {} entries total, {} props, {} edges, {} system, {} nullType, {} errors",
                     vertexId, entries.size(), entryProps, entryEdges, entrySystem, entryNullType, entryErrors);
        }
        propertyDecodeCount.addAndGet(entryProps);
        edgeSkippedSystem.addAndGet(entrySystem);

        if (!vertex.getProperties().isEmpty() || !vertex.getOutEdges().isEmpty()) {
            metrics.incrVerticesScanned();
            return vertex;
        }

        return null;
    }

    /**
     * Extract edge properties from a RelationCache.
     * Uses iteration over the cache's internal property map.
     */
    private void extractEdgeProperties(DecodedEdge edge, RelationCache rel, StandardJanusGraphTx tx) {
        if (!rel.hasProperties()) {
            return;
        }
        try {
            // RelationCache implements Iterable over its property entries.
            // Each entry has: key (long typeId), value (Object propertyValue)
            for (Object cursor : rel) {
                // The cursor is a LongObjectCursor<Object> from HPPC.
                // We use reflection-free access via the Iterable contract.
                // In JanusGraph 1.0.x, RelationCache's iterator yields entries
                // with .key (long) and .value (Object) fields.
                try {
                    // Access via HPPC LongObjectCursor fields
                    java.lang.reflect.Field keyField = cursor.getClass().getField("key");
                    java.lang.reflect.Field valueField = cursor.getClass().getField("value");
                    long propTypeId = keyField.getLong(cursor);
                    Object propValue = valueField.get(cursor);

                    RelationType propType = tx.getExistingRelationType(propTypeId);
                    if (propType != null && propValue != null) {
                        edge.addProperty(normalizePropertyName(propType.name()), propValue);
                    }
                } catch (Exception e) {
                    // Skip individual property on error
                }
            }
        } catch (Exception e) {
            LOG.trace("Failed to extract edge properties for edge {}", edge.getEdgeId(), e);
        }
    }

    /**
     * Normalize JanusGraph property key names to Atlas attribute names.
     * JanusGraph stores some properties with type-qualified names:
     *   "Referenceable.qualifiedName" → "qualifiedName"
     *   "Asset.name" → "name"
     *
     * Properties starting with "__" are NEVER normalized — they are Atlas internal
     * properties (e.g., "__guid", "__typeName", "__type.atlas_operation").
     * The "__type." prefix is used by Atlas's TypeDef system (PROPERTY_PREFIX = "__type.")
     * and must be preserved.
     */
    static String normalizePropertyName(String name) {
        if (name == null) return null;

        // Properties starting with "__" are Atlas internal — never normalize.
        // This includes: __guid, __typeName, __state, __type, __type_name,
        // __type.atlas_operation, __type.atlas_operation.CREATE, etc.
        if (name.startsWith("__")) {
            return name;
        }

        // For type-qualified names like "Asset.name" or "Referenceable.qualifiedName",
        // strip the type prefix to get just the attribute name.
        int dotIndex = name.indexOf('.');
        if (dotIndex > 0 && dotIndex < name.length() - 1) {
            name = name.substring(dotIndex + 1);
        }

        return name;
    }

    private boolean isSystemRelation(long typeId) {
        try {
            return IDManager.isSystemRelationTypeId(typeId);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void close() {
        try {
            janusGraph.close();
        } catch (Exception e) {
            LOG.warn("Error closing JanusGraph instance", e);
        }
    }
}
