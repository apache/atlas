package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes decoded JanusGraph vertices and edges to the new Cassandra schema (atlas_graph).
 *
 * Thread-safe: accepts vertices from the scanner pipeline via a BlockingQueue,
 * processes them with a configurable writer thread pool.
 *
 * Write strategy (optimized):
 *   - All mutations for a vertex (vertex row + indexes + edges) are collected
 *     into a single UNLOGGED batch → 1 network roundtrip instead of ~6.
 *   - Batches are fired asynchronously with a Semaphore cap per thread,
 *     allowing pipelining without overwhelming the coordinator.
 *   - High-edge vertices (>maxEdgesPerBatch) are split into multiple batches.
 */
public class CassandraTargetWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraTargetWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MigratorConfig   config;
    private final MigrationMetrics metrics;
    private final CqlSession       targetSession;
    private final String           ks;

    private PreparedStatement insertVertexStmt;
    private PreparedStatement insertEdgeOutStmt;
    private PreparedStatement insertEdgeInStmt;
    private PreparedStatement insertEdgeByIdStmt;
    private PreparedStatement insertIndexStmt;
    private PreparedStatement insertPropertyIndexStmt;
    private PreparedStatement insertClaimStmt;
    private PreparedStatement selectClaimStmt;
    private PreparedStatement insertTypeDefStmt;
    private PreparedStatement insertTypeDefByCategoryStmt;

    // Pipeline: scanner threads enqueue, writer threads dequeue
    private final BlockingQueue<DecodedVertex> queue;
    private final ExecutorService writerPool;
    private volatile boolean scanningComplete = false;

    // Diagnostic counters
    private final AtomicLong writeAttempts = new AtomicLong(0);
    private final AtomicLong writeErrors   = new AtomicLong(0);
    private static final int WRITE_SAMPLE_LOG_LIMIT = 10;
    private final ConcurrentMap<Long, String> vertexIdOverrides = new ConcurrentHashMap<>();

    public CassandraTargetWriter(MigratorConfig config, MigrationMetrics metrics, CqlSession targetSession) {
        this.config        = config;
        this.metrics       = metrics;
        this.targetSession = targetSession;
        this.ks            = config.getTargetCassandraKeyspace();
        this.queue         = new LinkedBlockingQueue<>(config.getQueueCapacity());
        this.writerPool    = Executors.newFixedThreadPool(config.getWriterThreads(), r -> {
            Thread t = new Thread(r);
            t.setName("writer-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }

    public void init() {
        createSchema();
        prepareStatements();
    }

    private void createSchema() {
        String strategy = config.getTargetReplicationStrategy();
        int rf = config.getTargetReplicationFactor();
        String dc = config.getTargetCassandraDatacenter();

        String replication;
        if ("SimpleStrategy".equals(strategy)) {
            replication = "{'class': 'SimpleStrategy', 'replication_factor': " + rf + "}";
        } else {
            replication = "{'class': 'NetworkTopologyStrategy', '" + dc + "': " + rf + "}";
        }

        LOG.info("Creating keyspace '{}' with replication: {}", ks, replication);
        targetSession.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + ks +
            " WITH replication = " + replication);

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".vertices (" +
            "  vertex_id text PRIMARY KEY," +
            "  properties text," +
            "  vertex_label text," +
            "  type_name text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".edges_out (" +
            "  out_vertex_id text," +
            "  edge_label text," +
            "  edge_id text," +
            "  in_vertex_id text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp," +
            "  PRIMARY KEY ((out_vertex_id), edge_label, edge_id)" +
            ") WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".edges_in (" +
            "  in_vertex_id text," +
            "  edge_label text," +
            "  edge_id text," +
            "  out_vertex_id text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp," +
            "  PRIMARY KEY ((in_vertex_id), edge_label, edge_id)" +
            ") WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".edges_by_id (" +
            "  edge_id text PRIMARY KEY," +
            "  out_vertex_id text," +
            "  in_vertex_id text," +
            "  edge_label text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".vertex_index (" +
            "  index_name text," +
            "  index_value text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((index_name, index_value)))");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".vertex_property_index (" +
            "  index_name text," +
            "  index_value text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((index_name, index_value), vertex_id))");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".schema_registry (" +
            "  property_name text PRIMARY KEY," +
            "  property_class text," +
            "  cardinality text," +
            "  created_at timestamp)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".entity_claims (" +
            "  identity_key text PRIMARY KEY," +
            "  vertex_id text," +
            "  claimed_at timestamp," +
            "  source text)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".type_definitions (" +
            "  type_name text PRIMARY KEY," +
            "  type_category text," +
            "  vertex_id text," +
            "  created_at timestamp," +
            "  modified_at timestamp)");

        targetSession.execute(
            "CREATE TABLE IF NOT EXISTS " + ks + ".type_definitions_by_category (" +
            "  type_category text," +
            "  type_name text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((type_category), type_name)" +
            ") WITH CLUSTERING ORDER BY (type_name ASC)");

        LOG.info("Target schema created/verified in keyspace '{}'", ks);
    }

    private void prepareStatements() {
        insertVertexStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".vertices " +
            "(vertex_id, properties, vertex_label, type_name, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

        insertEdgeOutStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".edges_out " +
            "(out_vertex_id, edge_label, edge_id, in_vertex_id, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        insertEdgeInStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".edges_in " +
            "(in_vertex_id, edge_label, edge_id, out_vertex_id, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        insertEdgeByIdStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".edges_by_id " +
            "(edge_id, out_vertex_id, in_vertex_id, edge_label, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        insertIndexStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".vertex_index (index_name, index_value, vertex_id) VALUES (?, ?, ?)");

        insertPropertyIndexStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".vertex_property_index (index_name, index_value, vertex_id) VALUES (?, ?, ?)");

        insertClaimStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".entity_claims (identity_key, vertex_id, claimed_at, source) VALUES (?, ?, ?, ?) IF NOT EXISTS");

        selectClaimStmt = targetSession.prepare(
            "SELECT vertex_id FROM " + ks + ".entity_claims WHERE identity_key = ?");

        insertTypeDefStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".type_definitions " +
            "(type_name, type_category, vertex_id, created_at, modified_at) VALUES (?, ?, ?, ?, ?)");

        insertTypeDefByCategoryStmt = targetSession.prepare(
            "INSERT INTO " + ks + ".type_definitions_by_category " +
            "(type_category, type_name, vertex_id) VALUES (?, ?, ?)");
    }

    /**
     * Called by scanner threads to enqueue a decoded vertex for writing.
     * Blocks if the queue is full (backpressure).
     */
    public void enqueue(DecodedVertex vertex) {
        try {
            queue.put(vertex);
            metrics.setQueueDepth(queue.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while enqueuing vertex", e);
        }
    }

    public void startWriters() {
        metrics.setQueueCapacity(config.getQueueCapacity());
        for (int i = 0; i < config.getWriterThreads(); i++) {
            writerPool.submit(this::writerLoop);
        }
        LOG.info("Started {} writer threads (queue capacity: {}, maxInflight/thread: {}, maxEdges/batch: {})",
                 config.getWriterThreads(), config.getQueueCapacity(),
                 config.getMaxInflightPerThread(), config.getMaxEdgesPerBatch());
    }

    public void signalScanComplete() {
        this.scanningComplete = true;
    }

    public void awaitCompletion() throws InterruptedException {
        writerPool.shutdown();
        writerPool.awaitTermination(24, TimeUnit.HOURS);
        LOG.info("All writer threads completed");
        LOG.info("DIAG WRITER SUMMARY: write attempts: {}, write errors: {}",
                 writeAttempts.get(), writeErrors.get());
    }

    /**
     * Writer loop: drains vertices from the queue, builds UNLOGGED batches,
     * and fires them asynchronously with a Semaphore cap on in-flight requests.
     */
    private void writerLoop() {
        Semaphore inflight = new Semaphore(config.getMaxInflightPerThread());

        while (true) {
            DecodedVertex vertex;
            try {
                if (scanningComplete && queue.isEmpty()) break;
                vertex = queue.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (vertex == null) {
                if (scanningComplete && queue.isEmpty()) break;
                continue;
            }

            metrics.setQueueDepth(queue.size());

            try {
                writeVertexAsync(vertex, inflight);
            } catch (Exception e) {
                metrics.incrWriteErrors();
                LOG.error("Failed to process vertex {}", vertex.getVertexId(), e);
            }
        }

        // Drain: wait for all in-flight async writes to complete
        try {
            inflight.acquire(config.getMaxInflightPerThread());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Build batch(es) for a vertex and fire them asynchronously.
     * Each batch is a single UNLOGGED batch containing vertex + index + edge mutations.
     * The Semaphore limits how many batches are in-flight concurrently per thread.
     */
    private void writeVertexAsync(DecodedVertex vertex, Semaphore inflight) throws InterruptedException {
        List<BatchStatement> batches = buildVertexBatches(vertex);

        for (BatchStatement batch : batches) {
            inflight.acquire();  // blocks if too many in-flight

            writeAttempts.incrementAndGet();

            targetSession.executeAsync(batch).whenComplete((result, error) -> {
                inflight.release();
                if (error != null) {
                    metrics.incrWriteErrors();
                    long errCount = writeErrors.incrementAndGet();
                    if (errCount <= WRITE_SAMPLE_LOG_LIMIT) {
                        LOG.error("Async batch write failed for vertex {}: {}",
                                  vertex.getVertexId(), error.toString());
                    }
                }
            });
        }

        // Count metrics optimistically — async writes to local Cassandra nearly always succeed
        metrics.incrVerticesWritten();
        metrics.incrEdgesWritten(vertex.getOutEdges().size());
    }

    /**
     * Build all CQL mutations for a vertex into UNLOGGED batch(es).
     * For typical vertices (≤15 edges), everything fits in 1 batch = 1 roundtrip.
     * For high-edge vertices, edges are chunked to stay under batch_size_fail_threshold.
     */
    private List<BatchStatement> buildVertexBatches(DecodedVertex vertex) {
        String vertexId = resolveVertexId(vertex);
        Instant now = Instant.now();

        // JanusGraph already stores the correct TypeCategory values (CLASS, TRAIT, ENUM, STRUCT, etc.)
        // that match DataTypes.TypeCategory — no translation needed.
        Map<String, Object> props = vertex.getProperties();

        String propsJson;
        try {
            propsJson = MAPPER.writeValueAsString(props);
        } catch (Exception e) {
            propsJson = "{}";
            LOG.warn("Failed to serialize properties for vertex {}", vertexId, e);
        }

        List<BoundStatement> stmts = new ArrayList<>();

        // 1. Vertex INSERT
        stmts.add(insertVertexStmt.bind(
            vertexId, propsJson, vertex.getVertexLabel(),
            vertex.getTypeName(), vertex.getState(), now, now));

        // 2. Index INSERTs
        int indexCount = buildIndexStatements(vertex, stmts);
        metrics.incrIndexesWritten(indexCount);

        // 3. TypeDef table INSERTs (if this is a TypeDef vertex)
        String typeName = (String) props.get("__type_name");
        Object rawCategory = props.get("__type_category");
        String typeCategory = rawCategory != null ? String.valueOf(rawCategory) : null;
        if ("typeSystem".equals(vertex.getVertexLabel()) && typeName != null && typeCategory != null) {
            stmts.add(insertTypeDefStmt.bind(typeName, typeCategory, vertexId, now, now));
            stmts.add(insertTypeDefByCategoryStmt.bind(typeCategory, typeName, vertexId));
            metrics.incrTypeDefsWritten();
        }

        // 4. Edge INSERTs (3 statements per edge: out, in, by_id)
        List<DecodedEdge> edges = vertex.getOutEdges();
        int maxEdges = config.getMaxEdgesPerBatch();

        if (edges.size() <= maxEdges) {
            // Everything fits in one batch
            for (DecodedEdge edge : edges) {
            buildEdgeStatements(edge, now, stmts);
            }
            return Collections.singletonList(
                BatchStatement.newInstance(BatchType.UNLOGGED,
                    stmts.toArray(new BatchableStatement[0])));
        }

        // High-edge vertex: first batch gets vertex + indexes + first chunk of edges
        List<BatchStatement> batches = new ArrayList<>();

        int firstChunk = Math.min(maxEdges, edges.size());
        for (int i = 0; i < firstChunk; i++) {
            buildEdgeStatements(edges.get(i), now, stmts);
        }
        batches.add(BatchStatement.newInstance(BatchType.UNLOGGED,
            stmts.toArray(new BatchableStatement[0])));

        // Remaining edges in chunks
        for (int i = firstChunk; i < edges.size(); i += maxEdges) {
            List<BoundStatement> edgeStmts = new ArrayList<>();
            int end = Math.min(i + maxEdges, edges.size());
            for (int j = i; j < end; j++) {
                buildEdgeStatements(edges.get(j), now, edgeStmts);
            }
            batches.add(BatchStatement.newInstance(BatchType.UNLOGGED,
                edgeStmts.toArray(new BatchableStatement[0])));
        }

        return batches;
    }

    /**
     * Add the 3 edge table INSERTs (edges_out, edges_in, edges_by_id) to the statement list.
     */
    private void buildEdgeStatements(DecodedEdge edge, Instant now, List<BoundStatement> stmts) {
        String edgePropsJson;
        try {
            edgePropsJson = edge.getProperties().isEmpty() ? "{}" :
                MAPPER.writeValueAsString(edge.getProperties());
        } catch (Exception e) {
            edgePropsJson = "{}";
        }

        Object edgeState = edge.getProperties().get("__state");
        String state = edgeState != null ? edgeState.toString() : "ACTIVE";

        String outVertexId = resolveVertexId(edge.getOutVertexJgId());
        String inVertexId = resolveVertexId(edge.getInVertexJgId());
        String edgeId;
        if (config.getIdStrategy() == IdStrategy.HASH_IDENTITY) {
            edgeId = DeterministicIdUtil.edgeIdFromIdentity(outVertexId, edge.getLabel(), inVertexId);
        } else {
            edgeId = DeterministicIdUtil.edgeIdFromJg(edge.getJgRelationId(), config.getIdStrategy());
        }

        stmts.add(insertEdgeOutStmt.bind(
            outVertexId, edge.getLabel(), edgeId,
            inVertexId, edgePropsJson, state, now, now));
        stmts.add(insertEdgeInStmt.bind(
            inVertexId, edge.getLabel(), edgeId,
            outVertexId, edgePropsJson, state, now, now));
        stmts.add(insertEdgeByIdStmt.bind(
            edgeId, outVertexId, inVertexId,
            edge.getLabel(), edgePropsJson, state, now, now));
    }

    /**
     * Add composite index INSERTs to the statement list.
     * Returns the number of index entries added.
     */
    private int buildIndexStatements(DecodedVertex vertex, List<BoundStatement> stmts) {
        String vertexId = resolveVertexId(vertex);
        Map<String, Object> props = vertex.getProperties();
        int indexCount = 0;

        // 1:1 unique index: __guid → vertex_id
        String guid = vertex.getGuid();
        if (guid != null) {
            stmts.add(insertIndexStmt.bind("__guid_idx", guid, vertexId));
            indexCount++;
        }

        // 1:1 composite index: qualifiedName + typeName → vertex_id
        Object qn = props.get("qualifiedName");
        if (qn == null) qn = props.get("Referenceable.qualifiedName");
        String typeName = vertex.getTypeName();
        if (qn != null && typeName != null) {
            stmts.add(insertIndexStmt.bind("qn_type_idx", qn + ":" + typeName, vertexId));
            indexCount++;
        }

        // 1:N index: __typeName → vertex_id
        if (typeName != null) {
            stmts.add(insertPropertyIndexStmt.bind("type_typename_idx", typeName, vertexId));
            indexCount++;
        }

        // 1:N index: TypeDef category lookup
        Object typeVertexType = props.get("__type");
        Object typeCategory = props.get("__type_category");
        if (typeVertexType != null && typeCategory != null) {
            stmts.add(insertPropertyIndexStmt.bind(
                "type_category_idx", typeVertexType + ":" + typeCategory, vertexId));
            indexCount++;
        }

        // 1:1 index: TypeDef name lookup
        if (typeVertexType != null) {
            Object typeDefName = props.get("__type_name");
            if (typeDefName == null) typeDefName = props.get("__type.name");
            if (typeDefName != null) {
                stmts.add(insertIndexStmt.bind(
                    "type_typename_idx", typeVertexType + ":" + typeDefName, vertexId));
                indexCount++;
            }
        }

        return indexCount;
    }

    private String resolveVertexId(DecodedVertex vertex) {
        String computed;
        if (config.getIdStrategy() == IdStrategy.HASH_IDENTITY) {
            // Try identity-based ID (matches runtime GraphIdUtil)
            Object qnObj = vertex.getProperties().get("qualifiedName");
            if (qnObj == null) qnObj = vertex.getProperties().get("Referenceable.qualifiedName");
            if (qnObj == null) {
                Object hierarchy = vertex.getProperties().get("__qualifiedNameHierarchy");
                if (hierarchy instanceof List && !((List<?>) hierarchy).isEmpty()) {
                    qnObj = ((List<?>) hierarchy).get(0);
                }
            }
            String identityId = DeterministicIdUtil.vertexIdFromIdentity(
                    vertex.getTypeName(), qnObj != null ? String.valueOf(qnObj) : null);
            computed = identityId != null ? identityId
                    : DeterministicIdUtil.vertexIdFromJg(vertex.getJgVertexId(), IdStrategy.HASH_JG);
        } else {
            computed = DeterministicIdUtil.vertexIdFromJg(vertex.getJgVertexId(), config.getIdStrategy());
        }

        // Always populate the override map so edge endpoint resolution works
        vertexIdOverrides.put(vertex.getJgVertexId(), computed);

        if (!config.isClaimEnabled()) {
            return computed;
        }

        Object qnObj = vertex.getProperties().get("qualifiedName");
        if (qnObj == null) {
            qnObj = vertex.getProperties().get("Referenceable.qualifiedName");
        }
        if (qnObj == null) {
            Object hierarchy = vertex.getProperties().get("__qualifiedNameHierarchy");
            if (hierarchy instanceof List && !((List<?>) hierarchy).isEmpty()) {
                qnObj = ((List<?>) hierarchy).get(0);
            }
        }
        String identityKey = DeterministicIdUtil.buildIdentityKey(vertex.getTypeName(),
                                                                   qnObj != null ? String.valueOf(qnObj) : null);
        if (identityKey == null) {
            return computed;
        }

        String claimedVertexId = claimVertexId(identityKey, computed);
        if (!claimedVertexId.equals(computed)) {
            vertexIdOverrides.put(vertex.getJgVertexId(), claimedVertexId);
            LOG.info("Vertex claim redirect: jgVertexId={} computedId={} claimedId={} identityKey={}",
                    vertex.getJgVertexId(), computed, claimedVertexId, identityKey);
        }

        return claimedVertexId;
    }

    private String resolveVertexId(long jgVertexId) {
        String overridden = vertexIdOverrides.get(jgVertexId);
        if (overridden != null) {
            return overridden;
        }
        // Fallback: for HASH_IDENTITY, the in-vertex may not be processed yet.
        // Use HASH_JG as a stable fallback — the edge will still land on a deterministic ID,
        // just not the identity-based one. Log a warning so we can track how often this happens.
        if (config.getIdStrategy() == IdStrategy.HASH_IDENTITY) {
            LOG.warn("Edge endpoint jgVertexId={} not in override map; falling back to HASH_JG", jgVertexId);
        }
        return DeterministicIdUtil.vertexIdFromJg(jgVertexId,
                config.getIdStrategy() == IdStrategy.HASH_IDENTITY ? IdStrategy.HASH_JG : config.getIdStrategy());
    }

    private String claimVertexId(String identityKey, String candidateVertexId) {
        try {
            Row appliedRow = targetSession.execute(insertClaimStmt.bind(
                    identityKey, candidateVertexId, Instant.now(), "migrator")).one();
            if (appliedRow != null && appliedRow.getBoolean("[applied]")) {
                return candidateVertexId;
            }
        } catch (Exception e) {
            LOG.warn("claimVertexId insert failed for identityKey={} candidateVertexId={}: {}",
                    identityKey, candidateVertexId, e.getMessage());
        }

        try {
            ResultSet rs = targetSession.execute(selectClaimStmt.bind(identityKey));
            Row row = rs.one();
            if (row != null && row.getString("vertex_id") != null) {
                return row.getString("vertex_id");
            }
        } catch (Exception e) {
            LOG.warn("claimVertexId select failed for identityKey={}: {}", identityKey, e.getMessage());
        }

        return candidateVertexId;
    }

    @Override
    public void close() {
        if (!writerPool.isShutdown()) {
            writerPool.shutdownNow();
        }
    }
}
