package org.apache.atlas.repository.graphdb.cassandra;

// CassandraGraph: direct Cassandra + ES graph backend implementation
import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ESAliasRequestBuilder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CassandraGraph implements AtlasGraph<CassandraVertex, CassandraEdge> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraGraph.class);

    /** Tracks ES indexes that have been verified/created during this JVM session. */
    private static final Set<String> VERIFIED_ES_INDEXES = ConcurrentHashMap.newKeySet();

    /**
     * Max vertices per LOGGED batch in commit(). Prevents exceeding Cassandra's
     * batch_size_fail_threshold (default 50KB). During TypeDef bootstrap, a single
     * transaction can contain 900+ TypeDef vertices — without chunking, the batch
     * exceeds 50KB and Cassandra rejects it.
     *
     * Each vertex generates ~4 statements  (1 INSERT + 2-3 index inserts).
     * Normal entity vertices are 1-2KB each, so 20 per batch (~40KB) fits
     * comfortably within the limit and matches typical entity bulk size.
     * TypeDef bootstrap mixes large entity defs (30-60KB) with small enums/structs,
     * so worst-case batches may approach the threshold but stay within 200KB
     * (the recommended production batch_size_fail_threshold).
     */
    private static final int MAX_VERTICES_PER_BATCH = 20;

    /**
     * Cached set of property names eligible for ES indexing, built from the in-memory
     * vertex mixed index registry. Mirrors JanusGraph's mixed index — only properties
     * explicitly registered via addMixedIndex() during GraphBackedSearchIndexer initialization
     * and typedef processing are included.
     *
     * Rebuilt lazily when the graph index changes (new typedefs added).
     * Falls back to blacklist + value-type filtering if the index hasn't been initialized yet.
     */
    private volatile Set<String> cachedESEligibleProperties = null;
    private volatile int         cachedESEligibleFieldCount = -1;

    private final CqlSession          session;
    private final VertexRepository    vertexRepository;
    private final EdgeRepository      edgeRepository;
    private final IndexRepository     indexRepository;
    private final TypeDefRepository   typeDefRepository;
    private final TypeDefCache        typeDefCache;
    private final ClaimRepository     claimRepository;
    private final ESOutboxRepository  esOutboxRepository;
    private final JobLeaseManager     leaseManager;
    private final ESOutboxProcessor   esOutboxProcessor;
    private final RepairJobScheduler  repairJobScheduler;
    private final Set<String>         multiProperties;
    private final RuntimeIdStrategy   idStrategy;
    private final boolean             claimEnabled;

    // Transaction buffer: accumulates changes in memory, flushed on commit
    private final ThreadLocal<TransactionBuffer> txBuffer = ThreadLocal.withInitial(TransactionBuffer::new);

    // Vertex cache for the current thread/transaction
    private final ThreadLocal<Map<String, CassandraVertex>> vertexCache = ThreadLocal.withInitial(ConcurrentHashMap::new);

    // Schema registry (in-memory)
    private final Map<String, CassandraPropertyKey> propertyKeys = new ConcurrentHashMap<>();
    private final Map<String, CassandraEdgeLabel>   edgeLabels   = new ConcurrentHashMap<>();
    private final Map<String, CassandraGraphIndex>  graphIndexes = new ConcurrentHashMap<>();

    public CassandraGraph(CqlSession session) {
        this(session, RuntimeIdStrategy.LEGACY, false);
    }

    public CassandraGraph(CqlSession session, RuntimeIdStrategy idStrategy, boolean claimEnabled) {
        this.session             = session;
        this.vertexRepository    = new VertexRepository(session);
        this.edgeRepository      = new EdgeRepository(session);
        this.indexRepository     = new IndexRepository(session);
        this.typeDefRepository   = new TypeDefRepository(session);
        this.typeDefCache        = new TypeDefCache(typeDefRepository);
        this.claimRepository     = new ClaimRepository(session);
        this.esOutboxRepository  = new ESOutboxRepository(session);
        this.leaseManager        = new JobLeaseManager(session);
        this.esOutboxProcessor   = new ESOutboxProcessor(esOutboxRepository, leaseManager);
        this.repairJobScheduler  = new RepairJobScheduler(session, this, leaseManager, esOutboxRepository);
        this.multiProperties     = ConcurrentHashMap.newKeySet();
        this.idStrategy          = idStrategy != null ? idStrategy : RuntimeIdStrategy.LEGACY;
        this.claimEnabled        = claimEnabled;

        // Start background processors
        this.esOutboxProcessor.start();
        this.repairJobScheduler.start();
    }

    // ---- Vertex operations ----

    @Override
    public AtlasVertex<CassandraVertex, CassandraEdge> addVertex() {
        String vertexId = UUID.randomUUID().toString();
        CassandraVertex vertex = new CassandraVertex(vertexId, this);
        txBuffer.get().addVertex(vertex);
        vertexCache.get().put(vertexId, vertex);
        return vertex;
    }

    @Override
    public AtlasVertex<CassandraVertex, CassandraEdge> getVertex(String vertexId) {
        if (vertexId == null) {
            return null;
        }

        // Check transaction buffer first
        CassandraVertex cached = vertexCache.get().get(vertexId);
        if (cached != null) {
            return cached;
        }

        // Then read from Cassandra
        CassandraVertex vertex = vertexRepository.getVertex(vertexId, this);
        if (vertex != null) {
            vertexCache.get().put(vertexId, vertex);
        }
        return vertex;
    }

    @Override
    public Set<AtlasVertex> getVertices(String... vertexIds) {
        if (vertexIds == null || vertexIds.length == 0) {
            return Collections.emptySet();
        }

        // Separate cached from uncached
        Set<AtlasVertex> result = new LinkedHashSet<>();
        List<String> uncachedIds = new ArrayList<>();
        for (String id : vertexIds) {
            CassandraVertex cached = vertexCache.get().get(id);
            if (cached != null) {
                result.add(cached);
            } else {
                uncachedIds.add(id);
            }
        }

        // Fetch uncached vertices in parallel using async queries
        if (!uncachedIds.isEmpty()) {
            Map<String, CassandraVertex> fetched = vertexRepository.getVerticesAsync(uncachedIds, this);
            for (CassandraVertex v : fetched.values()) {
                vertexCache.get().put(v.getIdString(), v);
                result.add(v);
            }
        }

        return result;
    }

    /**
     * Bulk fetch all edges for multiple vertices concurrently.
     * Returns a map of vertexId → list of all edges (both directions, all labels).
     * Edges from the transaction buffer are merged in; removed edges are excluded.
     */
    @SuppressWarnings("unchecked")
    public Map<String, List<CassandraEdge>> getAllEdgesForVertices(Collection<String> vertexIds) {
        // Fetch persisted edges for all vertices in parallel
        Map<String, List<CassandraEdge>> persisted =
            edgeRepository.getEdgesForVerticesAsync(vertexIds, AtlasEdgeDirection.BOTH, this);

        // Merge with transaction buffer
        Map<String, List<CassandraEdge>> result = new LinkedHashMap<>();
        for (String vertexId : vertexIds) {
            List<CassandraEdge> persistedEdges = persisted.getOrDefault(vertexId, Collections.emptyList());
            List<CassandraEdge> bufferedEdges = txBuffer.get().getEdgesForVertex(vertexId, AtlasEdgeDirection.BOTH, null);

            Map<String, CassandraEdge> merged = new LinkedHashMap<>();
            for (CassandraEdge e : persistedEdges) {
                if (!txBuffer.get().isEdgeRemoved(e.getIdString())) {
                    merged.put(e.getIdString(), e);
                }
            }
            for (CassandraEdge e : bufferedEdges) {
                merged.put(e.getIdString(), e);
            }
            result.put(vertexId, new ArrayList<>(merged.values()));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, List<AtlasEdge<CassandraVertex, CassandraEdge>>> getEdgesForVertices(
            Collection<String> vertexIds) {
        Map<String, List<CassandraEdge>> raw = getAllEdgesForVertices(vertexIds);
        Map<String, List<AtlasEdge<CassandraVertex, CassandraEdge>>> result = new LinkedHashMap<>();
        for (Map.Entry<String, List<CassandraEdge>> entry : raw.entrySet()) {
            result.put(entry.getKey(), (List) entry.getValue());
        }
        return result;
    }

    /**
     * Bulk fetch edges for multiple vertices, filtered by specific edge labels.
     * Uses per-label Cassandra queries (partition + clustering key) for efficiency,
     * avoiding fetching all edges when only specific relationships are needed.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, List<AtlasEdge<CassandraVertex, CassandraEdge>>> getEdgesForVertices(
            Collection<String> vertexIds, Set<String> edgeLabels) {
        if (edgeLabels == null || edgeLabels.isEmpty()) {
            return getEdgesForVertices(vertexIds);
        }

        // Fetch persisted edges filtered by labels
        Map<String, List<CassandraEdge>> persisted =
            edgeRepository.getEdgesForVerticesByLabelsAsync(vertexIds, edgeLabels, AtlasEdgeDirection.BOTH, this);

        // Merge with transaction buffer (filtered by labels)
        Map<String, List<AtlasEdge<CassandraVertex, CassandraEdge>>> result = new LinkedHashMap<>();
        for (String vertexId : vertexIds) {
            List<CassandraEdge> persistedEdges = persisted.getOrDefault(vertexId, Collections.emptyList());

            Map<String, CassandraEdge> merged = new LinkedHashMap<>();
            for (CassandraEdge e : persistedEdges) {
                if (!txBuffer.get().isEdgeRemoved(e.getIdString())) {
                    merged.put(e.getIdString(), e);
                }
            }
            // Add buffered edges that match the requested labels
            List<CassandraEdge> bufferedEdges = txBuffer.get().getEdgesForVertex(vertexId, AtlasEdgeDirection.BOTH, null);
            for (CassandraEdge e : bufferedEdges) {
                if (edgeLabels.contains(e.getLabel())) {
                    merged.put(e.getIdString(), e);
                }
            }
            result.put(vertexId, (List) new ArrayList<>(merged.values()));
        }
        return result;
    }

    /**
     * Label-filtered edge fetch with per-label LIMIT pushed to Cassandra.
     * Prevents reading thousands of rows for high-cardinality relationships
     * (e.g. __Table.columns with 5000+ edges) when only ~100 are needed.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, List<AtlasEdge<CassandraVertex, CassandraEdge>>> getEdgesForVertices(
            Collection<String> vertexIds, Set<String> edgeLabels, int limitPerLabel) {
        if (edgeLabels == null || edgeLabels.isEmpty()) {
            return getEdgesForVertices(vertexIds);
        }
        if (limitPerLabel <= 0) {
            return getEdgesForVertices(vertexIds, edgeLabels);
        }

        Map<String, List<CassandraEdge>> persisted =
            edgeRepository.getEdgesForVerticesByLabelsAsync(vertexIds, edgeLabels, AtlasEdgeDirection.BOTH, this, limitPerLabel);

        Map<String, List<AtlasEdge<CassandraVertex, CassandraEdge>>> result = new LinkedHashMap<>();
        for (String vertexId : vertexIds) {
            List<CassandraEdge> persistedEdges = persisted.getOrDefault(vertexId, Collections.emptyList());

            Map<String, CassandraEdge> merged = new LinkedHashMap<>();
            for (CassandraEdge e : persistedEdges) {
                if (!txBuffer.get().isEdgeRemoved(e.getIdString())) {
                    merged.put(e.getIdString(), e);
                }
            }
            List<CassandraEdge> bufferedEdges = txBuffer.get().getEdgesForVertex(vertexId, AtlasEdgeDirection.BOTH, null);
            for (CassandraEdge e : bufferedEdges) {
                if (edgeLabels.contains(e.getLabel())) {
                    merged.put(e.getIdString(), e);
                }
            }
            result.put(vertexId, (List) new ArrayList<>(merged.values()));
        }
        return result;
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> getVertices(String key, Object value) {
        // Try composite index lookup first
        String indexValue = String.valueOf(value);

        // Check known composite indexes
        String vertexId = indexRepository.lookupVertex(key + "_idx", indexValue);
        if (vertexId != null) {
            AtlasVertex<CassandraVertex, CassandraEdge> vertex = getVertex(vertexId);
            if (vertex != null) {
                return Collections.singletonList(vertex);
            }
        }

        // Fallback: scan transaction buffer
        List<AtlasVertex<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
        for (CassandraVertex v : vertexCache.get().values()) {
            Object propVal = v.getProperty(key, Object.class);
            if (propVal != null && propVal.equals(value)) {
                result.add(v);
            }
        }

        return result;
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> getVertices() {
        // This is a potentially expensive full scan - used rarely
        LOG.warn("getVertices() called without parameters - this performs a full table scan");
        return new ArrayList<>(vertexCache.get().values());
    }

    @Override
    public void removeVertex(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
        if (vertex == null) {
            return;
        }
        CassandraVertex cv = (CassandraVertex) vertex;
        cv.markDeleted();
        txBuffer.get().removeVertex(cv);
        vertexCache.get().remove(cv.getIdString());
    }

    // ---- Edge operations ----

    @Override
    public AtlasEdge<CassandraVertex, CassandraEdge> addEdge(AtlasVertex<CassandraVertex, CassandraEdge> outVertex,
                                                               AtlasVertex<CassandraVertex, CassandraEdge> inVertex,
                                                               String label) throws AtlasBaseException {
        String outId = ((CassandraVertex) outVertex).getIdString();
        String inId = ((CassandraVertex) inVertex).getIdString();

        if (outId.equals(inId)) {
            LOG.error("Attempting to create a self-loop relationship on vertex {}", outId);
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_CREATE_INVALID_PARAMS, outId);
        }
        String edgeId = idStrategy == RuntimeIdStrategy.DETERMINISTIC
                ? GraphIdUtil.deterministicEdgeId(outId, label, inId)
                : UUID.randomUUID().toString();
        CassandraEdge edge = new CassandraEdge(edgeId,
                outId,
                inId,
                label, this);
        txBuffer.get().addEdge(edge);
        return edge;
    }

    RuntimeIdStrategy getIdStrategy() {
        return idStrategy;
    }

    /**
     * Called by CassandraVertex when it eagerly computes its deterministic ID.
     * Updates the vertex cache and transaction buffer to reflect the new ID.
     */
    void notifyVertexIdChanged(String oldId, String newId, CassandraVertex vertex) {
        vertexCache.get().remove(oldId);
        vertexCache.get().put(newId, vertex);
        txBuffer.get().notifyVertexIdChanged(oldId, newId, vertex);
    }

    @Override
    public AtlasEdge<CassandraVertex, CassandraEdge> getEdgeBetweenVertices(AtlasVertex fromVertex,
                                                                             AtlasVertex toVertex,
                                                                             String relationshipLabel) {
        if (fromVertex == null || toVertex == null) {
            return null;
        }

        String fromId = ((CassandraVertex) fromVertex).getIdString();
        String toId   = ((CassandraVertex) toVertex).getIdString();

        // O(1) lookup when using deterministic edge IDs — compute edge ID from endpoints + label
        if (RuntimeIdStrategy.DETERMINISTIC == idStrategy) {
            String edgeId = GraphIdUtil.deterministicEdgeId(fromId, relationshipLabel, toId);
            return edgeRepository.getEdge(edgeId, this);
        }

        // Fallback: O(n) scan for LEGACY mode (UUID-based edge IDs)
        List<CassandraEdge> outEdges = edgeRepository.getEdgesForVertex(fromId, AtlasEdgeDirection.OUT, relationshipLabel, this);
        for (CassandraEdge edge : outEdges) {
            if (edge.getInVertexId().equals(toId)) {
                return edge;
            }
        }
        return null;
    }

    @Override
    public AtlasEdge<CassandraVertex, CassandraEdge> getEdge(String edgeId) {
        if (edgeId == null) {
            return null;
        }
        return edgeRepository.getEdge(edgeId, this);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> getEdges() {
        LOG.warn("getEdges() called without parameters - this is not efficient");
        return Collections.emptyList();
    }

    @Override
    public void removeEdge(AtlasEdge<CassandraVertex, CassandraEdge> edge) {
        if (edge == null) {
            return;
        }
        CassandraEdge ce = (CassandraEdge) edge;
        ce.markDeleted();
        txBuffer.get().removeEdge(ce);
    }

    @SuppressWarnings("unchecked")
    Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> getEdgesForVertex(String vertexId,
                                                                            AtlasEdgeDirection direction,
                                                                            String edgeLabel) {
        // Check buffer for new edges first
        List<CassandraEdge> bufferedEdges = txBuffer.get().getEdgesForVertex(vertexId, direction, edgeLabel);
        List<CassandraEdge> persistedEdges = edgeRepository.getEdgesForVertex(vertexId, direction, edgeLabel, this);

        // Merge, avoiding duplicates
        Map<String, CassandraEdge> merged = new LinkedHashMap<>();
        for (CassandraEdge e : persistedEdges) {
            if (!txBuffer.get().isEdgeRemoved(e.getIdString())) {
                merged.put(e.getIdString(), e);
            }
        }
        for (CassandraEdge e : bufferedEdges) {
            merged.put(e.getIdString(), e);
        }

        return (Iterable) new ArrayList<>(merged.values());
    }

    /**
     * Server-side COUNT of persisted edges via CQL COUNT(*).
     * Does NOT include uncommitted buffer — caller must adjust separately.
     */
    long countEdgesForVertex(String vertexId, AtlasEdgeDirection direction, String edgeLabel) {
        return edgeRepository.countEdges(vertexId, direction, edgeLabel);
    }

    /**
     * Calculate the buffer adjustment for edge counts.
     * Returns: (new buffered edges matching criteria) - (removed buffered edges that exist in Cassandra).
     *
     * For removed edges: they are still counted by Cassandra's COUNT(*), so we subtract them.
     * For new edges: they are NOT in Cassandra yet, so we add them.
     */
    long countBufferedEdgeAdjustment(String vertexId, AtlasEdgeDirection direction, String edgeLabel) {
        TransactionBuffer buffer = txBuffer.get();

        // Count new buffered edges matching the criteria
        List<CassandraEdge> bufferedNew = buffer.getEdgesForVertex(vertexId, direction, edgeLabel);
        long newCount = bufferedNew.size();

        // Count removed edges matching the criteria (these are still in Cassandra's COUNT)
        long removedCount = 0;
        for (CassandraEdge removed : buffer.getRemovedEdges()) {
            if (edgeLabel != null && !edgeLabel.equals(removed.getLabel())) {
                continue;
            }
            boolean matches = switch (direction) {
                case OUT -> removed.getOutVertexId().equals(vertexId);
                case IN -> removed.getInVertexId().equals(vertexId);
                case BOTH -> removed.getOutVertexId().equals(vertexId) || removed.getInVertexId().equals(vertexId);
            };
            if (matches) {
                removedCount++;
            }
        }

        return newCount - removedCount;
    }

    /**
     * Check if the transaction buffer has any new (uncommitted) edges matching the criteria.
     */
    boolean hasBufferedEdges(String vertexId, AtlasEdgeDirection direction, String edgeLabel) {
        List<CassandraEdge> buffered = txBuffer.get().getEdgesForVertex(vertexId, direction, edgeLabel);
        return !buffered.isEmpty();
    }

    /**
     * Server-side existence check via CQL LIMIT 1.
     * Does NOT check uncommitted buffer — caller must check buffer separately.
     */
    boolean hasEdgesForVertex(String vertexId, AtlasEdgeDirection direction, String edgeLabel) {
        return edgeRepository.hasEdges(vertexId, direction, edgeLabel);
    }

    // ---- Query operations ----

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> query() {
        return new CassandraGraphQuery(this);
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> V(Object... vertexIds) {
        return new CassandraGraphTraversal(this, vertexIds);
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> E(Object... edgeIds) {
        return new CassandraGraphTraversal(this, true, edgeIds);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> indexQuery(String indexName, String queryString) {
        return new CassandraIndexQuery(this, indexName, queryString, 0);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> indexQuery(String indexName, String queryString, int offset) {
        return new CassandraIndexQuery(this, indexName, queryString, offset);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> indexQuery(GraphIndexQueryParameters indexQueryParameters) {
        return new CassandraIndexQuery(this, indexQueryParameters);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> elasticsearchQuery(String indexName, SearchSourceBuilder sourceBuilder) {
        ensureESIndexExists(indexName);
        return new CassandraIndexQuery(this, indexName, sourceBuilder);
    }

    @Override
    public AtlasIndexQuery<CassandraVertex, CassandraEdge> elasticsearchQuery(String indexName, SearchParams searchParams) throws AtlasBaseException {
        ensureESIndexExists(indexName);
        return new CassandraIndexQuery(this, indexName, searchParams);
    }

    @Override
    public void createOrUpdateESAlias(ESAliasRequestBuilder aliasRequestBuilder) throws AtlasBaseException {
        // TODO: implement ES alias management
        LOG.debug("createOrUpdateESAlias called - delegating to ES client");
    }

    @Override
    public void deleteESAlias(String indexName, String aliasName) throws AtlasBaseException {
        // TODO: implement ES alias deletion
        LOG.debug("deleteESAlias called for index={}, alias={}", indexName, aliasName);
    }

    @Override
    public AtlasIndexQuery elasticsearchQuery(String indexName) throws AtlasBaseException {
        ensureESIndexExists(indexName);
        return new CassandraIndexQuery(this, indexName, (SearchSourceBuilder) null);
    }

    /**
     * Ensures the given ES index exists, creating it if necessary.
     * Called lazily from elasticsearchQuery() methods, which guarantees the ES client
     * is available (unlike during startup when GraphBackedSearchIndexer runs before
     * AtlasElasticsearchDatabase is initialized).
     */
    static void ensureESIndexExists(String indexName) {
        if (indexName == null) {
            return;
        }
        // Normalize: callers may pass unprefixed names like "search_logs" but the actual
        // ES index is prefixed (e.g. "atlas_graph_search_logs"). Must match what
        // CassandraIndexQuery.normalizeIndexName() will resolve to.
        if (!indexName.startsWith(Constants.INDEX_PREFIX)) {
            indexName = Constants.INDEX_PREFIX + indexName;
        }
        if (VERIFIED_ES_INDEXES.contains(indexName)) {
            return;
        }
        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                LOG.warn("ES client not available, cannot ensure index {} exists", indexName);
                return;
            }

            // HEAD check - does the index exist?
            Request headReq = new Request("HEAD", "/" + indexName);
            Response headResp = client.performRequest(headReq);
            if (headResp.getStatusLine().getStatusCode() == 200) {
                VERIFIED_ES_INDEXES.add(indexName);
                return;
            }
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                createESIndex(indexName);
            } else {
                LOG.warn("Failed to check ES index {}: {}", indexName, e.getMessage());
            }
        } catch (Exception e) {
            LOG.warn("Failed to check ES index {}: {}", indexName, e.getMessage());
        }
    }

    private static void createESIndex(String indexName) {
        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                return;
            }

            String settings = "{\n" +
                "  \"settings\": {\n" +
                "    \"number_of_shards\": 1,\n" +
                "    \"number_of_replicas\": 0,\n" +
                "    \"index.mapping.total_fields.limit\": 10000\n" +
                "  },\n" +
                "  \"mappings\": {\n" +
                "    \"dynamic\": true\n" +
                "  }\n" +
                "}";

            Request req = new Request("PUT", "/" + indexName);
            req.setEntity(new StringEntity(settings, ContentType.APPLICATION_JSON));
            Response resp = client.performRequest(req);

            int status = resp.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
                LOG.info("Created ES index: {}", indexName);
                VERIFIED_ES_INDEXES.add(indexName);
            } else {
                LOG.warn("Failed to create ES index {}: status={}", indexName, status);
            }
        } catch (Exception e) {
            LOG.warn("Failed to create ES index {}: {}", indexName, e.getMessage());
        }
    }

    // ---- Transaction operations ----

    @Override
    public void commit() {
        TransactionBuffer buffer = txBuffer.get();

        try {
            List<CassandraVertex> originalNewVertices = buffer.getNewVertices();
            Set<String> claimLostIds = new LinkedHashSet<>();
            Map<String, String> vertexIdRewrite = resolveClaimedVertexIds(originalNewVertices, claimLostIds);

            List<CassandraVertex> newVertices = new ArrayList<>();
            for (CassandraVertex v : originalNewVertices) {
                if (claimLostIds.contains(v.getIdString())) {
                    // Case 1: Claim lost — skip this vertex entirely (duplicate)
                    LOG.info("commit: duplicate logical vertex detected; skipping insert for tempVertexId={} claimedVertexId={}",
                            v.getIdString(), vertexIdRewrite.get(v.getIdString()));
                    continue;
                }

                String newId = vertexIdRewrite.get(v.getIdString());
                if (newId != null && !newId.equals(v.getIdString())) {
                    // Case 2: Deterministic ID remap — create replacement vertex with new ID
                    CassandraVertex replacement = new CassandraVertex(newId, v.getVertexLabel(),
                            new LinkedHashMap<>(v.getProperties()), this);
                    newVertices.add(replacement);
                    vertexCache.get().remove(v.getIdString());
                    vertexCache.get().put(newId, replacement);
                    LOG.info("commit: remapped vertex {} -> {} (deterministic)", v.getIdString(), newId);
                } else {
                    // Case 3: No change — use as-is
                    newVertices.add(v);
                }
            }

            List<CassandraEdge> newEdges = rewriteEdgesForClaimedVertices(buffer.getNewEdges(), vertexIdRewrite);

            // Flush new vertices + their index entries in LOGGED batches.
            // Each batch keeps per-vertex atomicity (vertex + its indexes together),
            // eliminating the W2 window where a vertex could exist without its indexes.
            // Batches are chunked to avoid exceeding Cassandra's batch_size_fail_threshold
            // (default 50KB), which can happen during TypeDef bootstrap (952+ vertices).
            if (!newVertices.isEmpty()) {
                int batchCount = 0;
                for (int i = 0; i < newVertices.size(); i += MAX_VERTICES_PER_BATCH) {
                    com.datastax.oss.driver.api.core.cql.BatchStatementBuilder atomicBatch =
                            com.datastax.oss.driver.api.core.cql.BatchStatement.builder(
                                    com.datastax.oss.driver.api.core.cql.DefaultBatchType.LOGGED);

                    int end = Math.min(i + MAX_VERTICES_PER_BATCH, newVertices.size());
                    for (int j = i; j < end; j++) {
                        CassandraVertex v = newVertices.get(j);
                        atomicBatch.addStatement(vertexRepository.bindInsertVertex(v));

                        List<IndexRepository.IndexEntry> uniqueEntries = new ArrayList<>();
                        List<IndexRepository.IndexEntry> propertyEntries = new ArrayList<>();
                        buildIndexEntries(v, uniqueEntries, propertyEntries);

                        for (IndexRepository.IndexEntry e : uniqueEntries) {
                            atomicBatch.addStatement(indexRepository.bindInsertIndex(e.indexName, e.indexValue, e.vertexId));
                        }
                        for (IndexRepository.IndexEntry e : propertyEntries) {
                            atomicBatch.addStatement(indexRepository.bindInsertPropertyIndex(e.indexName, e.indexValue, e.vertexId));
                        }
                    }

                    session.execute(atomicBatch.build());
                    batchCount++;
                }
                LOG.info("commit: wrote {} new vertices with their index entries in {} batch(es)", newVertices.size(), batchCount);
            }

            // Update dirty vertices in LOGGED batches (chunked to avoid batch size limits)
            List<CassandraVertex> dirtyVertices = buffer.getDirtyVertices();
            if (!dirtyVertices.isEmpty()) {
                int dirtyBatchCount = 0;
                for (int i = 0; i < dirtyVertices.size(); i += MAX_VERTICES_PER_BATCH) {
                    com.datastax.oss.driver.api.core.cql.BatchStatementBuilder dirtyBatch =
                            com.datastax.oss.driver.api.core.cql.BatchStatement.builder(
                                    com.datastax.oss.driver.api.core.cql.DefaultBatchType.LOGGED);
                    int end = Math.min(i + MAX_VERTICES_PER_BATCH, dirtyVertices.size());
                    for (int j = i; j < end; j++) {
                        dirtyBatch.addStatement(vertexRepository.bindInsertVertex(dirtyVertices.get(j)));
                    }
                    session.execute(dirtyBatch.build());
                    dirtyBatchCount++;
                }
                LOG.info("commit: updated {} dirty vertices in {} batch(es)", dirtyVertices.size(), dirtyBatchCount);
            }

            // Flush new edges
            if (!newEdges.isEmpty()) {
                edgeRepository.batchInsertEdges(newEdges);
            }

            // Flush dirty edges (property updates on existing edges, e.g., soft-delete state change)
            List<CassandraEdge> dirtyEdges = buffer.getDirtyEdges();
            for (CassandraEdge e : dirtyEdges) {
                edgeRepository.updateEdge(e);
            }

            // Build edge index entries (e.g., relationship GUID → edge_id)
            List<IndexRepository.EdgeIndexEntry> edgeIndexEntries = new ArrayList<>();
            for (CassandraEdge e : newEdges) {
                buildEdgeIndexEntries(e, edgeIndexEntries);
            }
            if (!edgeIndexEntries.isEmpty()) {
                indexRepository.batchAddEdgeIndexes(edgeIndexEntries);
                LOG.info("commit: wrote {} edge index entries to Cassandra", edgeIndexEntries.size());
            }

            // Process removals — batched for performance
            List<CassandraEdge> removedEdges = buffer.getRemovedEdges();
            List<CassandraVertex> removedVertices = buffer.getRemovedVertices();

            // 1. Batch-delete explicitly removed edges + their index entries
            if (!removedEdges.isEmpty()) {
                edgeRepository.batchDeleteEdges(removedEdges);

                List<IndexRepository.EdgeIndexEntry> edgeIndexRemovals = new ArrayList<>();
                for (CassandraEdge edge : removedEdges) {
                    Object relGuid = edge.getProperty(Constants.RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
                    if (relGuid != null) {
                        edgeIndexRemovals.add(new IndexRepository.EdgeIndexEntry(
                                "_r__guid_idx", String.valueOf(relGuid), edge.getIdString()));
                    }
                }
                if (!edgeIndexRemovals.isEmpty()) {
                    indexRepository.batchRemoveEdgeIndexes(edgeIndexRemovals);
                }
                LOG.info("commit: batch-deleted {} edges, {} edge index entries",
                        removedEdges.size(), edgeIndexRemovals.size());
            }

            // 2. For removed vertices: cascade-delete their edges, then the vertex + its indexes
            if (!removedVertices.isEmpty()) {
                for (CassandraVertex vertex : removedVertices) {
                    // Cascade-delete all edges using paginated approach (bounded memory)
                    edgeRepository.deleteEdgesForVertexPaginated(vertex.getIdString(), this);
                    vertexRepository.deleteVertex(vertex.getIdString());
                }

                // Clean up vertex index entries (prevents orphaned indexes)
                List<IndexRepository.IndexEntry> vertexIndexRemovals = new ArrayList<>();
                List<IndexRepository.IndexEntry> vertexPropertyIndexRemovals = new ArrayList<>();
                for (CassandraVertex vertex : removedVertices) {
                    String vertexId = vertex.getIdString();

                    // 1:1 unique indexes (vertex_index table)
                    Object guid = vertex.getProperty("__guid", String.class);
                    if (guid != null) {
                        vertexIndexRemovals.add(new IndexRepository.IndexEntry(
                                "__guid_idx", String.valueOf(guid), vertexId));
                    }

                    Object qn = vertex.getProperty("qualifiedName", String.class);
                    Object typeName = vertex.getProperty("__typeName", String.class);
                    if (qn != null && typeName != null) {
                        vertexIndexRemovals.add(new IndexRepository.IndexEntry(
                                "qn_type_idx", qn + ":" + typeName, vertexId));
                    }

                    Object vertexType = vertex.getProperty(Constants.VERTEX_TYPE_PROPERTY_KEY, String.class);
                    Object typeDefName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);
                    if (vertexType != null && typeDefName != null) {
                        vertexIndexRemovals.add(new IndexRepository.IndexEntry(
                                "type_typename_idx", vertexType + ":" + typeDefName, vertexId));
                    }

                    // 1:N property indexes (vertex_property_index table)
                    Object typeCategory = vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, String.class);
                    if (vertexType != null && typeCategory != null) {
                        vertexPropertyIndexRemovals.add(new IndexRepository.IndexEntry(
                                "type_category_idx", vertexType + ":" + typeCategory, vertexId));
                    }
                }

                if (!vertexIndexRemovals.isEmpty()) {
                    indexRepository.batchRemoveIndexes(vertexIndexRemovals);
                }
                if (!vertexPropertyIndexRemovals.isEmpty()) {
                    indexRepository.batchRemovePropertyIndexes(vertexPropertyIndexRemovals);
                }

                LOG.info("commit: removed {} vertices, {} unique index entries, {} property index entries",
                        removedVertices.size(), vertexIndexRemovals.size(), vertexPropertyIndexRemovals.size());
            }

            // Update index entries for dirty vertices (new vertex indexes were already written atomically above)
            List<IndexRepository.IndexEntry> dirtyUniqueIndexEntries   = new ArrayList<>();
            List<IndexRepository.IndexEntry> dirtyPropertyIndexEntries = new ArrayList<>();
            for (CassandraVertex v : dirtyVertices) {
                buildIndexEntries(v, dirtyUniqueIndexEntries, dirtyPropertyIndexEntries);
            }
            LOG.info("commit: {} new vertices (atomic), {} dirty vertices, {} dirty unique index entries, {} dirty property index entries",
                    newVertices.size(), dirtyVertices.size(), dirtyUniqueIndexEntries.size(), dirtyPropertyIndexEntries.size());
            if (!dirtyUniqueIndexEntries.isEmpty()) {
                indexRepository.batchAddIndexes(dirtyUniqueIndexEntries);
                LOG.info("commit: wrote {} dirty unique index entries to Cassandra", dirtyUniqueIndexEntries.size());
            }
            if (!dirtyPropertyIndexEntries.isEmpty()) {
                indexRepository.batchAddPropertyIndexes(dirtyPropertyIndexEntries);
            }

            // Sync TypeDef vertices to the dedicated type_definitions tables.
            // Wrapped in try-catch to prevent TypeDefCache write failures from crashing the
            // entire commit() — which would cause TypeRegistryUpdateHook.onComplete(false) and
            // discard the in-memory TypeRegistry. The TypeDef data is already persisted in the
            // vertices table (written atomically above), so the generic index fallback path
            // (vertex_index + vertex_property_index) will still work for TypeDef lookups.
            try {
                syncTypeDefsToCache(newVertices, dirtyVertices, removedVertices);
            } catch (Exception e) {
                LOG.error("Failed to sync TypeDefs to dedicated cache tables (TypeDef data is still in vertices table): {}", e.getMessage(), e);
            }

            // Sync vertices to Elasticsearch (replaces JanusGraph's mixed index sync)
            syncVerticesToElasticsearch(newVertices, dirtyVertices, removedVertices);

            // Mark all elements as persisted
            for (CassandraVertex v : originalNewVertices) {
                v.markPersisted();
            }
            for (CassandraVertex v : dirtyVertices) {
                v.markPersisted();
            }
            for (CassandraEdge e : newEdges) {
                e.markPersisted();
            }
            for (CassandraEdge e : dirtyEdges) {
                e.markPersisted();
            }

        } catch (Exception e) {
            LOG.error("CassandraGraph.commit() FAILED: {}. TypeRegistryUpdateHook will receive isSuccess=false, " +
                    "discarding any transient type registry updates. Cassandra writes that already flushed " +
                    "are NOT rolled back.", e.getMessage(), e);
            throw e;
        } finally {
            buffer.clear();
            // Clear vertex cache so the next transaction on this thread reads fresh from Cassandra.
            // Without this, a thread reused from the pool could return stale cached vertices.
            vertexCache.get().clear();
        }
    }

    @Override
    public void rollback() {
        TransactionBuffer buffer = txBuffer.get();
        buffer.clear();
        // Clear vertex cache so the next transaction reads fresh from Cassandra
        vertexCache.get().clear();
    }

    private Map<String, String> resolveClaimedVertexIds(List<CassandraVertex> newVertices,
                                                         Set<String> claimLostIds) {
        if ((!claimEnabled && idStrategy != RuntimeIdStrategy.DETERMINISTIC)
                || newVertices == null || newVertices.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> rewrites = new LinkedHashMap<>();
        for (CassandraVertex vertex : newVertices) {
            String qn = extractQualifiedName(vertex);
            String typeName = vertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
            String identityKey = GraphIdUtil.buildIdentityKey(typeName, qn);
            if (identityKey == null) {
                continue;
            }

            String candidateVertexId = vertex.getIdString();

            // Step 1: Compute deterministic vertex ID from typeName + qualifiedName
            if (idStrategy == RuntimeIdStrategy.DETERMINISTIC) {
                String deterministicId = GraphIdUtil.deterministicVertexId(identityKey);
                if (!deterministicId.equals(candidateVertexId)) {
                    rewrites.put(candidateVertexId, deterministicId);
                    candidateVertexId = deterministicId;
                }
            }

            // Step 2: LWT claim (only for legacy UUID mode — deterministic IDs are their own dedup)
            if (claimEnabled && idStrategy != RuntimeIdStrategy.DETERMINISTIC) {
                String resolvedVertexId = claimRepository.claimOrGet(identityKey, candidateVertexId, "runtime");
                if (!candidateVertexId.equals(resolvedVertexId)) {
                    // Claim lost — another process already owns this entity
                    claimLostIds.add(vertex.getIdString());
                    rewrites.put(vertex.getIdString(), resolvedVertexId);
                }
            }
        }

        return rewrites;
    }

    private List<CassandraEdge> rewriteEdgesForClaimedVertices(List<CassandraEdge> original,
                                                               Map<String, String> vertexIdRewrite) {
        if (original == null || original.isEmpty() || vertexIdRewrite == null || vertexIdRewrite.isEmpty()) {
            return original != null ? original : Collections.emptyList();
        }

        List<CassandraEdge> rewritten = new ArrayList<>(original.size());
        for (CassandraEdge edge : original) {
            String outVertexId = vertexIdRewrite.getOrDefault(edge.getOutVertexId(), edge.getOutVertexId());
            String inVertexId = vertexIdRewrite.getOrDefault(edge.getInVertexId(), edge.getInVertexId());

            if (outVertexId.equals(edge.getOutVertexId()) && inVertexId.equals(edge.getInVertexId())) {
                rewritten.add(edge);
                continue;
            }

            String edgeId = idStrategy == RuntimeIdStrategy.DETERMINISTIC
                    ? GraphIdUtil.deterministicEdgeId(outVertexId, edge.getLabel(), inVertexId)
                    : edge.getIdString();

            Map<String, Object> props = new LinkedHashMap<>(edge.getProperties());
            CassandraEdge re = new CassandraEdge(edgeId, outVertexId, inVertexId, edge.getLabel(), props, this);
            rewritten.add(re);
        }

        return rewritten;
    }

    private String extractQualifiedName(CassandraVertex vertex) {
        String qn = vertex.getProperty("qualifiedName", String.class);
        if (qn == null) {
            qn = vertex.getProperty("Referenceable.qualifiedName", String.class);
        }
        return qn;
    }

    private void buildIndexEntries(CassandraVertex vertex,
                                   List<IndexRepository.IndexEntry> uniqueEntries,
                                   List<IndexRepository.IndexEntry> propertyEntries) {
        // ---- 1:1 unique indexes (vertex_index table) ----

        // Index by __guid
        Object guid = vertex.getProperty("__guid", String.class);
        if (guid != null) {
            String guidStr = String.valueOf(guid);
            uniqueEntries.add(new IndexRepository.IndexEntry("__guid_idx", guidStr, vertex.getIdString()));
            LOG.info("buildIndexEntries: __guid_idx [{}] -> vertexId [{}]", guidStr, vertex.getIdString());
        }

        // Index by qualifiedName + __typeName (entity lookup)
        Object qn = vertex.getProperty("qualifiedName", String.class);
        Object entityTypeName = vertex.getProperty("__typeName", String.class);
        if (qn != null && entityTypeName != null) {
            String indexVal = qn + ":" + entityTypeName;
            uniqueEntries.add(new IndexRepository.IndexEntry("qn_type_idx", indexVal, vertex.getIdString()));
            LOG.info("buildIndexEntries: qn_type_idx [{}] -> vertexId [{}]", indexVal, vertex.getIdString());
        }

        // Index by VERTEX_TYPE + TYPENAME (TypeDef lookup: findTypeVertexByName)
        Object vertexType = vertex.getProperty(Constants.VERTEX_TYPE_PROPERTY_KEY, String.class);
        Object typeDefName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);
        if (vertexType != null && typeDefName != null) {
            uniqueEntries.add(new IndexRepository.IndexEntry("type_typename_idx",
                    String.valueOf(vertexType) + ":" + String.valueOf(typeDefName), vertex.getIdString()));
        }

        // ---- 1:N property indexes (vertex_property_index table) ----

        // Index by VERTEX_TYPE + TYPE_CATEGORY (for findTypeVerticesByCategory / getAll)
        Object typeCategory = vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, String.class);
        if (vertexType != null && typeCategory != null) {
            propertyEntries.add(new IndexRepository.IndexEntry("type_category_idx",
                    String.valueOf(vertexType) + ":" + String.valueOf(typeCategory), vertex.getIdString()));
        }

        if (guid == null && qn == null && vertexType == null) {
            LOG.warn("buildIndexEntries: vertex [{}] has no indexable properties! Keys: {}",
                    vertex.getIdString(), vertex.getPropertyKeys());
        }
    }

    private void buildEdgeIndexEntries(CassandraEdge edge, List<IndexRepository.EdgeIndexEntry> entries) {
        // Index by relationship GUID (_r__guid) so edges can be looked up by relationship GUID
        Object relGuid = edge.getProperty(Constants.RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
        if (relGuid != null) {
            entries.add(new IndexRepository.EdgeIndexEntry("_r__guid_idx", String.valueOf(relGuid), edge.getIdString()));
        }
    }

    // ---- TypeDef sync (dedicated Cassandra table + Caffeine cache) ----

    /**
     * Syncs TypeDef vertices to the dedicated type_definitions table and Caffeine cache.
     * TypeDef vertices are identified by having VERTEX_TYPE_PROPERTY_KEY = "typeSystem".
     */
    private void syncTypeDefsToCache(List<CassandraVertex> newVertices,
                                     List<CassandraVertex> dirtyVertices,
                                     List<CassandraVertex> removedVertices) {
        for (CassandraVertex v : newVertices) {
            putTypeDefIfApplicable(v);
        }
        for (CassandraVertex v : dirtyVertices) {
            putTypeDefIfApplicable(v);
        }
        for (CassandraVertex v : removedVertices) {
            Object typeName = v.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);
            Object vertexType = v.getProperty(Constants.VERTEX_TYPE_PROPERTY_KEY, String.class);
            if ("typeSystem".equals(String.valueOf(vertexType)) && typeName != null) {
                typeDefCache.remove(String.valueOf(typeName));
            }
        }
    }

    private void putTypeDefIfApplicable(CassandraVertex v) {
        Object vertexType = v.getProperty(Constants.VERTEX_TYPE_PROPERTY_KEY, String.class);
        if (!"typeSystem".equals(String.valueOf(vertexType))) {
            return;
        }

        Object typeName = v.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);
        Object typeCategory = v.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, String.class);
        if (typeName != null && typeCategory != null) {
            typeDefCache.put(String.valueOf(typeName), String.valueOf(typeCategory), v.getIdString());
        }
    }

    // ---- Elasticsearch sync (replaces JanusGraph's mixed index) ----

    /**
     * Syncs vertex properties to Elasticsearch after Cassandra commit.
     * This replaces JanusGraph's automatic mixed-index sync.
     * Uses the ES bulk API for efficiency.
     */
    private static final int    ES_SYNC_MAX_RETRIES  = 3;
    private static final long[] ES_SYNC_RETRY_DELAYS = {100, 300, 1000}; // ms

    private void syncVerticesToElasticsearch(List<CassandraVertex> newVertices,
                                             List<CassandraVertex> dirtyVertices,
                                             List<CassandraVertex> removedVertices) {
        try {
            String indexName = Constants.VERTEX_INDEX_NAME;

            // Collect entity vertices to sync, keyed by vertex ID for retry tracking
            Map<String, CassandraVertex> vertexMap = new LinkedHashMap<>();
            // Pre-computed filtered JSON for each vertex (used for both ES sync and outbox)
            Map<String, String> vertexJsonMap = new LinkedHashMap<>();

            int skipped = 0;
            for (CassandraVertex v : newVertices) {
                if (isEntityVertex(v)) {
                    vertexMap.put(v.getIdString(), v);
                    vertexJsonMap.put(v.getIdString(), AtlasType.toJson(filterPropertiesForES(v.getProperties())));
                    LOG.info("syncVerticesToElasticsearch: NEW vertex _id='{}', __typeName='{}', propCount={}",
                            v.getIdString(), v.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class),
                            v.getProperties().size());
                } else {
                    skipped++;
                }
            }

            for (CassandraVertex v : dirtyVertices) {
                if (isEntityVertex(v)) {
                    vertexMap.put(v.getIdString(), v);
                    vertexJsonMap.put(v.getIdString(), AtlasType.toJson(filterPropertiesForES(v.getProperties())));
                    LOG.info("syncVerticesToElasticsearch: DIRTY vertex _id='{}', __typeName='{}', propCount={}",
                            v.getIdString(), v.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class),
                            v.getProperties().size());
                } else {
                    skipped++;
                }
            }

            if (skipped > 0) {
                LOG.info("syncVerticesToElasticsearch: skipped {} non-entity vertices (no __typeName)", skipped);
            }

            // Track vertex IDs to delete
            List<String> deleteIds = new ArrayList<>();
            for (CassandraVertex v : removedVertices) {
                deleteIds.add(v.getIdString());
            }

            if (vertexMap.isEmpty() && deleteIds.isEmpty()) {
                LOG.info("syncVerticesToElasticsearch: no entity vertices to sync ({} new, {} dirty skipped)",
                        newVertices.size(), dirtyVertices.size());
                return;
            }

            // Write outbox entries to Cassandra BEFORE attempting ES sync.
            // This guarantees we can retry even if the process crashes during ES sync.
            writeOutboxEntries(vertexMap, vertexJsonMap, deleteIds);

            // Try synchronous ES sync (happy path)
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                LOG.warn("ES client not available, {} index + {} delete operations written to outbox for background retry",
                        vertexMap.size(), deleteIds.size());
                return;
            }

            // Attempt sync with retries for transient failures
            Set<String> pendingIndexIds  = new LinkedHashSet<>(vertexMap.keySet());
            Set<String> pendingDeleteIds = new LinkedHashSet<>(deleteIds);
            Set<String> permanentlyFailed = new LinkedHashSet<>();

            for (int attempt = 0; attempt <= ES_SYNC_MAX_RETRIES && (!pendingIndexIds.isEmpty() || !pendingDeleteIds.isEmpty()); attempt++) {
                if (attempt > 0) {
                    long delay = ES_SYNC_RETRY_DELAYS[Math.min(attempt - 1, ES_SYNC_RETRY_DELAYS.length - 1)];
                    LOG.warn("ES sync retry attempt {}/{} after {}ms for {} index + {} delete operations",
                            attempt, ES_SYNC_MAX_RETRIES, delay, pendingIndexIds.size(), pendingDeleteIds.size());
                    Thread.sleep(delay);
                }

                StringBuilder bulkBody = new StringBuilder();
                for (String vid : pendingIndexIds) {
                    bulkBody.append("{\"index\":{\"_index\":\"").append(indexName)
                            .append("\",\"_id\":\"").append(vid).append("\"}}\n");
                    bulkBody.append(vertexJsonMap.get(vid)).append("\n");
                }
                for (String vid : pendingDeleteIds) {
                    bulkBody.append("{\"delete\":{\"_index\":\"").append(indexName)
                            .append("\",\"_id\":\"").append(vid).append("\"}}\n");
                }

                LOG.info("syncVerticesToElasticsearch: sending bulk request to ES index '{}', body length={}, attempt={}",
                        indexName, bulkBody.length(), attempt);

                try {
                    Request bulkReq = new Request("POST", "/_bulk");
                    bulkReq.setEntity(new StringEntity(bulkBody.toString(), ContentType.APPLICATION_JSON));
                    Response resp = client.performRequest(bulkReq);
                    int status = resp.getStatusLine().getStatusCode();
                    String respBody = org.apache.http.util.EntityUtils.toString(resp.getEntity());

                    if (status >= 200 && status < 300) {
                        if (respBody != null && respBody.contains("\"errors\":true")) {
                            // Parse per-item results to find which items failed and why
                            Set<String> retryableIds = new LinkedHashSet<>();
                            processESBulkResponse(respBody, retryableIds, permanentlyFailed);
                            // Only retry items with transient (5xx) failures
                            pendingIndexIds.retainAll(retryableIds);
                            pendingDeleteIds.retainAll(retryableIds);
                        } else {
                            // All items succeeded
                            LOG.info("Synced {} index + {} delete to ES index '{}', status={}, attempt={}",
                                    pendingIndexIds.size(), pendingDeleteIds.size(), indexName, status, attempt);
                            pendingIndexIds.clear();
                            pendingDeleteIds.clear();
                        }
                    } else if (status >= 500) {
                        // Entire request failed with server error — retry everything
                        LOG.warn("ES bulk sync returned status {} on attempt {}, will retry", status, attempt);
                    } else {
                        // 4xx — non-retryable request-level error
                        LOG.error("ES bulk sync returned non-retryable status {} on attempt {}. Response: {}",
                                status, attempt, respBody != null ? respBody.substring(0, Math.min(2000, respBody.length())) : "null");
                        break;
                    }
                } catch (org.elasticsearch.client.ResponseException re) {
                    int errStatus = re.getResponse().getStatusLine().getStatusCode();
                    if (errStatus >= 500) {
                        LOG.warn("ES sync ResponseException (status={}) on attempt {}: {}", errStatus, attempt, re.getMessage());
                        // Retry on server error
                    } else {
                        LOG.error("ES sync non-retryable ResponseException (status={}) on attempt {}: {}",
                                errStatus, attempt, re.getMessage());
                        break;
                    }
                } catch (java.io.IOException ioe) {
                    LOG.warn("ES sync IOException on attempt {}/{}: {}", attempt, ES_SYNC_MAX_RETRIES, ioe.getMessage());
                    // Connection error — retry
                }
            }

            // Determine which vertices succeeded and clean up their outbox entries
            Set<String> allPending = new LinkedHashSet<>(vertexMap.keySet());
            allPending.addAll(deleteIds);
            Set<String> stillFailing = new LinkedHashSet<>(pendingIndexIds);
            stillFailing.addAll(pendingDeleteIds);
            allPending.removeAll(stillFailing);
            allPending.removeAll(permanentlyFailed);

            if (!allPending.isEmpty()) {
                esOutboxRepository.batchMarkDone(new ArrayList<>(allPending));
                LOG.info("ES sync: cleaned up {} outbox entries for successfully synced vertices", allPending.size());
            }

            // Log final state of any failures (outbox entries remain PENDING for background retry)
            if (!stillFailing.isEmpty()) {
                LOG.error("ES sync FAILED after {} retries. {} operations still pending (outbox will retry). "
                        + "Vertex IDs: {}",
                        ES_SYNC_MAX_RETRIES, stillFailing.size(),
                        stillFailing.stream().limit(50).collect(java.util.stream.Collectors.joining(", ")));
            }
            if (!permanentlyFailed.isEmpty()) {
                LOG.error("ES sync had {} permanently failed items (non-retryable mapping/data errors). "
                        + "Vertex IDs: {}",
                        permanentlyFailed.size(),
                        permanentlyFailed.stream().limit(50).collect(java.util.stream.Collectors.joining(", ")));
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("ES sync interrupted during retry backoff: {}", ie.getMessage());
        } catch (Exception e) {
            LOG.error("Failed to sync vertices to ES (outbox entries preserved for background retry): {}", e.getMessage(), e);
        }
    }

    /**
     * Writes outbox entries to Cassandra for all vertices that need ES sync.
     * These entries act as a durable record so the background ESOutboxProcessor
     * can retry any that fail during synchronous sync.
     */
    private void writeOutboxEntries(Map<String, CassandraVertex> vertexMap,
                                     Map<String, String> vertexJsonMap,
                                     List<String> deleteIds) {
        try {
            // Collect all statements, then chunk to avoid exceeding Cassandra batch size limits
            List<com.datastax.oss.driver.api.core.cql.BoundStatement> statements = new ArrayList<>();
            for (Map.Entry<String, String> entry : vertexJsonMap.entrySet()) {
                statements.add(esOutboxRepository.bindInsert(
                        entry.getKey(), ESOutboxRepository.ACTION_INDEX, entry.getValue()));
            }
            for (String deleteId : deleteIds) {
                statements.add(esOutboxRepository.bindInsert(
                        deleteId, ESOutboxRepository.ACTION_DELETE, null));
            }

            for (int i = 0; i < statements.size(); i += MAX_VERTICES_PER_BATCH) {
                com.datastax.oss.driver.api.core.cql.BatchStatementBuilder outboxBatch =
                        com.datastax.oss.driver.api.core.cql.BatchStatement.builder(
                                com.datastax.oss.driver.api.core.cql.DefaultBatchType.LOGGED);
                int end = Math.min(i + MAX_VERTICES_PER_BATCH, statements.size());
                for (int j = i; j < end; j++) {
                    outboxBatch.addStatement(statements.get(j));
                }
                session.execute(outboxBatch.build());
            }

            LOG.info("writeOutboxEntries: wrote {} index + {} delete outbox entries",
                    vertexJsonMap.size(), deleteIds.size());
        } catch (Exception e) {
            LOG.error("writeOutboxEntries: failed to write outbox entries: {}", e.getMessage(), e);
        }
    }

    /**
     * Processes an ES bulk response with errors, separating retryable (5xx) from
     * permanently failed (4xx) items.
     *
     * @param respBody        the raw ES bulk response JSON
     * @param retryableIds    output: vertex IDs that failed with retryable errors (5xx)
     * @param permanentlyFailed output: vertex IDs that failed with non-retryable errors (4xx)
     */
    private void processESBulkResponse(String respBody, Set<String> retryableIds, Set<String> permanentlyFailed) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> bulkResp = AtlasType.fromJson(respBody, Map.class);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> items = (List<Map<String, Object>>) bulkResp.get("items");
            if (items == null) return;

            int succeeded = 0;
            int retryable = 0;
            int permanent = 0;

            for (Map<String, Object> item : items) {
                @SuppressWarnings("unchecked")
                Map<String, Object> action = (Map<String, Object>) item.values().iterator().next();
                if (action == null) continue;

                String docId = String.valueOf(action.get("_id"));
                Object statusObj = action.get("status");
                int itemStatus = (statusObj instanceof Number) ? ((Number) statusObj).intValue() : 0;

                if (action.containsKey("error")) {
                    if (itemStatus >= 500 || itemStatus == 429) {
                        // Retryable: server error or rate-limited
                        retryableIds.add(docId);
                        retryable++;
                        LOG.warn("ES bulk item retryable failure: _id='{}', status={}", docId, itemStatus);
                    } else {
                        // Permanent: mapping error, bad request, etc.
                        permanentlyFailed.add(docId);
                        permanent++;
                        LOG.error("ES bulk item FAILED (non-retryable): _id='{}', status={}, error={}",
                                docId, itemStatus, AtlasType.toJson(action.get("error")));
                    }
                } else {
                    succeeded++;
                }
            }

            LOG.info("ES bulk response: {} succeeded, {} retryable failures, {} permanent failures",
                    succeeded, retryable, permanent);
        } catch (Exception parseEx) {
            LOG.warn("Could not parse ES bulk response for item errors: {}. Raw (truncated): {}",
                    parseEx.getMessage(), respBody.substring(0, Math.min(4000, respBody.length())));
        }
    }

    private void appendESIndexAction(StringBuilder bulkBody, String indexName, CassandraVertex v) {
        bulkBody.append("{\"index\":{\"_index\":\"").append(indexName)
                .append("\",\"_id\":\"").append(v.getIdString()).append("\"}}\n");
        bulkBody.append(AtlasType.toJson(filterPropertiesForES(v.getProperties()))).append("\n");
    }

    /**
     * Filters vertex properties before sending to Elasticsearch.
     *
     * Two layers of filtering:
     *
     * 1. Property name blacklist:
     *    - __type_* (typedef metadata — ~2500+ unique fields)
     *    - __type.* (typedef attribute metadata with dot notation)
     *    - endDef1, endDef2, relationshipCategory, relationshipLabel, tagPropagation
     *
     * 2. Value type exclusion (matches JanusGraph's isIndexApplicable):
     *    - Map values: map-type attributes stored via setProperty(key, HashMap) —
     *      JanusGraph deliberately does NOT register these in the mixed index
     *    - BigDecimal/BigInteger: excluded by JanusGraph's INDEX_EXCLUSION_CLASSES
     *
     * Additionally, JSON array strings from setJsonProperty() are parsed back to arrays
     * so ES receives the correct type. JSON object strings are NOT parsed (they're mapped
     * as "text" in ES — parsing them to Map causes mapper_parsing_exception).
     */
    private Map<String, Object> filterPropertiesForES(Map<String, Object> props) {
        Map<String, Object> filtered = new LinkedHashMap<>(props.size());

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            // --- Property name blacklist ---

            // Skip typedef attribute metadata — these create ~2500+ fields in ES
            if (key.startsWith("__type_") || key.startsWith("__type.")) {
                continue;
            }

            // Skip relationship typedef properties
            if (key.equals("endDef1") || key.equals("endDef2") ||
                key.equals("relationshipCategory") || key.equals("relationshipLabel") ||
                key.equals("tagPropagation")) {
                continue;
            }

            // --- Value type exclusion ---

            // Skip Map values — map-type attributes that JanusGraph doesn't index.
            // These are stored via setProperty(key, new HashMap<>(..)) in EntityGraphMapper.mapMapValue().
            // JanusGraph's createIndexForAttribute() creates a propertyKey for maps but deliberately
            // does NOT call createVertexIndex()/addMixedIndex() for them.
            if (value instanceof Map) {
                continue;
            }

            // Skip BigDecimal/BigInteger — excluded by JanusGraph's INDEX_EXCLUSION_CLASSES
            if (value instanceof BigDecimal || value instanceof BigInteger) {
                continue;
            }

            // Fix double-encoded JSON array strings from setJsonProperty().
            // Only parse arrays ([...]), NOT objects ({...}) — see __customAttributes bug.
            if (value instanceof String) {
                String strVal = (String) value;
                if (strVal.length() > 1 && strVal.charAt(0) == '[' && strVal.charAt(strVal.length() - 1) == ']') {
                    try {
                        value = AtlasType.fromJson(strVal, Object.class);
                    } catch (Exception e) {
                        // Not valid JSON — keep as plain string
                    }
                }
            }

            filtered.put(key, value);
        }

        return filtered;
    }

    /**
     * Returns true if this vertex represents a data entity that should be
     * indexed in Elasticsearch. Only vertices with __typeName (e.g., "Table",
     * "Column", "Connection") are entity vertices.
     *
     * TypeDef vertices (__type = "typeSystem") are NOT indexed — they have
     * hundreds of unique properties across 952 types that would exceed ES's
     * default 2000-field limit. TypeDef lookups go through Cassandra indexes.
     *
     * System vertices (patches, index recovery) lack both __typeName and __type
     * and are also excluded.
     */
    private boolean isEntityVertex(CassandraVertex v) {
        Object typeName = v.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
        return typeName != null;
    }

    // ---- Reindex (repair) operations ----

    /**
     * Reads vertices from Cassandra by GUID, filters their properties, and bulk-indexes
     * them into Elasticsearch. Used by RepairIndex to recover entities that are in
     * Cassandra but missing from (or stale in) ES.
     *
     * Each GUID is looked up via the __guid index. Vertices that can't be found or
     * aren't entity vertices are logged and skipped. The bulk write uses the same
     * retry logic as commit-time ES sync.
     *
     * @param guids GUIDs of entities to reindex
     * @return number of vertices successfully sent to ES
     */
    public int reindexVertices(Collection<String> guids) {
        if (guids == null || guids.isEmpty()) {
            return 0;
        }

        RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
        if (client == null) {
            LOG.error("reindexVertices: ES client not available, cannot reindex {} entities", guids.size());
            return 0;
        }

        String indexName = Constants.VERTEX_INDEX_NAME;
        ensureESIndexExists(indexName);

        // Look up each GUID → vertex, filter to entity vertices
        Map<String, CassandraVertex> vertexMap = new LinkedHashMap<>();
        int notFound = 0;
        int notEntity = 0;

        for (String guid : guids) {
            if (guid == null) continue;

            // Look up vertex by GUID via index
            String vertexId = indexRepository.lookupVertex("__guid_idx", guid);
            if (vertexId == null) {
                notFound++;
                LOG.warn("reindexVertices: no vertex found for GUID '{}'", guid);
                continue;
            }

            CassandraVertex vertex = vertexRepository.getVertex(vertexId, this);
            if (vertex == null) {
                notFound++;
                LOG.warn("reindexVertices: vertex '{}' (GUID '{}') not found in Cassandra", vertexId, guid);
                continue;
            }

            if (!isEntityVertex(vertex)) {
                notEntity++;
                continue;
            }

            vertexMap.put(vertex.getIdString(), vertex);
        }

        if (!vertexMap.isEmpty()) {
            LOG.info("reindexVertices: resolved {}/{} GUIDs to entity vertices ({} not found, {} non-entity)",
                    vertexMap.size(), guids.size(), notFound, notEntity);
        } else {
            LOG.warn("reindexVertices: no entity vertices resolved from {} GUIDs ({} not found, {} non-entity)",
                    guids.size(), notFound, notEntity);
            return 0;
        }

        // Bulk-write to ES with retry (same logic as commit-time sync)
        Set<String> pendingIds = new LinkedHashSet<>(vertexMap.keySet());
        Set<String> permanentlyFailed = new LinkedHashSet<>();

        try {
            for (int attempt = 0; attempt <= ES_SYNC_MAX_RETRIES && !pendingIds.isEmpty(); attempt++) {
                if (attempt > 0) {
                    long delay = ES_SYNC_RETRY_DELAYS[Math.min(attempt - 1, ES_SYNC_RETRY_DELAYS.length - 1)];
                    LOG.info("reindexVertices: retry attempt {}/{} after {}ms for {} vertices",
                            attempt, ES_SYNC_MAX_RETRIES, delay, pendingIds.size());
                    Thread.sleep(delay);
                }

                StringBuilder bulkBody = new StringBuilder();
                for (String vid : pendingIds) {
                    CassandraVertex v = vertexMap.get(vid);
                    appendESIndexAction(bulkBody, indexName, v);
                }

                LOG.info("reindexVertices: sending bulk request to ES index '{}', {} vertices, body length={}, attempt={}",
                        indexName, pendingIds.size(), bulkBody.length(), attempt);

                try {
                    Request bulkReq = new Request("POST", "/_bulk");
                    bulkReq.setEntity(new StringEntity(bulkBody.toString(), ContentType.APPLICATION_JSON));
                    Response resp = client.performRequest(bulkReq);
                    int status = resp.getStatusLine().getStatusCode();
                    String respBody = org.apache.http.util.EntityUtils.toString(resp.getEntity());

                    if (status >= 200 && status < 300) {
                        if (respBody != null && respBody.contains("\"errors\":true")) {
                            Set<String> retryableIds = new LinkedHashSet<>();
                            processESBulkResponse(respBody, retryableIds, permanentlyFailed);
                            pendingIds.retainAll(retryableIds);
                        } else {
                            LOG.info("reindexVertices: successfully indexed {} vertices to ES", pendingIds.size());
                            pendingIds.clear();
                        }
                    } else if (status >= 500) {
                        LOG.warn("reindexVertices: ES returned status {} on attempt {}, will retry", status, attempt);
                    } else {
                        LOG.error("reindexVertices: ES returned non-retryable status {} on attempt {}. Response: {}",
                                status, attempt, respBody != null ? respBody.substring(0, Math.min(2000, respBody.length())) : "null");
                        break;
                    }
                } catch (ResponseException re) {
                    int errStatus = re.getResponse().getStatusLine().getStatusCode();
                    if (errStatus >= 500) {
                        LOG.warn("reindexVertices: ES ResponseException (status={}) on attempt {}: {}", errStatus, attempt, re.getMessage());
                    } else {
                        LOG.error("reindexVertices: ES non-retryable ResponseException (status={}) on attempt {}: {}",
                                errStatus, attempt, re.getMessage());
                        break;
                    }
                } catch (IOException ioe) {
                    LOG.warn("reindexVertices: ES IOException on attempt {}/{}: {}", attempt, ES_SYNC_MAX_RETRIES, ioe.getMessage());
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("reindexVertices: interrupted during retry backoff: {}", ie.getMessage());
        }

        int succeeded = vertexMap.size() - pendingIds.size() - permanentlyFailed.size();

        if (!pendingIds.isEmpty()) {
            LOG.error("reindexVertices: {} vertices still pending after {} retries. Vertex IDs: {}",
                    pendingIds.size(), ES_SYNC_MAX_RETRIES,
                    pendingIds.stream().limit(50).collect(Collectors.joining(", ")));
        }
        if (!permanentlyFailed.isEmpty()) {
            LOG.error("reindexVertices: {} vertices permanently failed. Vertex IDs: {}",
                    permanentlyFailed.size(),
                    permanentlyFailed.stream().limit(50).collect(Collectors.joining(", ")));
        }

        LOG.info("reindexVertices: completed — {} succeeded, {} failed, {} permanently failed out of {} GUIDs",
                succeeded, pendingIds.size(), permanentlyFailed.size(), guids.size());

        return succeeded;
    }

    /**
     * Scans the entire Cassandra vertices table, finds all entities whose qualifiedName
     * starts with the given prefix (typically a connectionQualifiedName), and reindexes
     * them to Elasticsearch in batches.
     *
     * Uses Cassandra driver's token-range paging to stream rows without loading the
     * full table into memory. A fast string pre-filter on the raw JSON avoids
     * deserializing non-matching rows.
     *
     * @param qualifiedNamePrefix  e.g., "default/snowflake/1772139790"
     * @param fetchSize            Cassandra page size (default 1000)
     * @param reindexBatchSize     batch size for ES bulk writes (default 500)
     * @return total number of vertices reindexed to ES
     */
    /**
     * @param progressCallback optional callback invoked after each batch with the cumulative count reindexed so far
     */
    public int reindexByQualifiedNamePrefix(String qualifiedNamePrefix, int fetchSize, int reindexBatchSize,
                                             java.util.function.IntConsumer progressCallback) {
        if (qualifiedNamePrefix == null || qualifiedNamePrefix.isEmpty()) {
            LOG.error("reindexByQualifiedNamePrefix: prefix is null/empty");
            return 0;
        }

        LOG.info("reindexByQualifiedNamePrefix: starting full table scan for prefix '{}' (fetchSize={}, batchSize={})",
                qualifiedNamePrefix, fetchSize, reindexBatchSize);
        long startTime = System.currentTimeMillis();

        int[] totalReindexed = {0};

        int totalMatched = vertexRepository.scanVerticesByQualifiedNamePrefix(
                qualifiedNamePrefix, fetchSize, reindexBatchSize,
                guidBatch -> {
                    LOG.info("reindexByQualifiedNamePrefix: reindexing batch of {} GUIDs", guidBatch.size());
                    int count = reindexVertices(guidBatch);
                    totalReindexed[0] += count;
                    if (progressCallback != null) {
                        progressCallback.accept(totalReindexed[0]);
                    }
                });

        long elapsed = System.currentTimeMillis() - startTime;
        LOG.info("reindexByQualifiedNamePrefix: completed — {} matched, {} reindexed to ES in {} ms (prefix='{}')",
                totalMatched, totalReindexed[0], elapsed, qualifiedNamePrefix);

        return totalReindexed[0];
    }

    // ---- Management operations ----

    @Override
    public AtlasGraphManagement getManagementSystem() {
        return new CassandraGraphManagement(this);
    }

    @Override
    public Set<String> getEdgeIndexKeys() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getVertexIndexKeys() {
        Set<String> eligible = getESEligiblePropertyNames();
        return eligible != null ? eligible : Collections.emptySet();
    }

    /**
     * Returns the set of property names registered in the vertex mixed index.
     * Built from the in-memory graph index registry (populated by GraphBackedSearchIndexer
     * via CassandraGraphManagement.addMixedIndex() during initialization and typedef changes).
     *
     * Returns null if the vertex index hasn't been initialized yet.
     */
    Set<String> getESEligiblePropertyNames() {
        CassandraGraphIndex vertexIndex = graphIndexes.get(Constants.VERTEX_INDEX);
        if (vertexIndex == null) {
            LOG.info("getESEligiblePropertyNames: vertex index not found in graphIndexes map (keys={})", graphIndexes.keySet());
            return null;
        }

        Set<AtlasPropertyKey> fieldKeys = vertexIndex.getFieldKeys();
        int currentCount = fieldKeys.size();

        // Rebuild cache if the field count changed (new typedefs registered)
        if (cachedESEligibleProperties == null || currentCount != cachedESEligibleFieldCount) {
            Set<String> names = new HashSet<>(currentCount);
            for (AtlasPropertyKey key : fieldKeys) {
                names.add(key.getName());
            }
            cachedESEligibleProperties = Collections.unmodifiableSet(names);
            cachedESEligibleFieldCount = currentCount;

            LOG.info("Rebuilt ES eligible properties cache: {} properties registered in vertex index. Contains qualifiedName={}, name={}, sample={}",
                    currentCount, names.contains("qualifiedName"), names.contains("name"),
                    names.stream().limit(20).collect(Collectors.joining(", ")));
        }

        return cachedESEligibleProperties;
    }

    // ---- Utility operations ----

    @Override
    public boolean isMultiProperty(String name) {
        return multiProperties.contains(name);
    }

    public void addMultiProperty(String name) {
        multiProperties.add(name);
    }

    @Override
    public AtlasIndexQueryParameter indexQueryParameter(String parameterName, String parameterValue) {
        return new CassandraIndexQueryParameter(parameterName, parameterValue);
    }

    @Override
    public AtlasGraphIndexClient getGraphIndexClient() throws AtlasException {
        return new CassandraGraphIndexClient(this);
    }

    @Override
    public void shutdown() {
        LOG.info("CassandraGraph shutdown initiated — stopping background processors");
        try {
            esOutboxProcessor.stop();
        } catch (Exception e) {
            LOG.warn("Error stopping ESOutboxProcessor: {}", e.getMessage());
        }
        try {
            repairJobScheduler.stop();
        } catch (Exception e) {
            LOG.warn("Error stopping RepairJobScheduler: {}", e.getMessage());
        }
        LOG.info("Background processors stopped — closing Cassandra session");
        CassandraSessionProvider.shutdown();
    }

    @Override
    public void clear() {
        LOG.warn("clear() called - this will truncate all graph tables");
        session.execute("TRUNCATE vertices");
        session.execute("TRUNCATE edges_out");
        session.execute("TRUNCATE edges_in");
        session.execute("TRUNCATE edges_by_id");
        session.execute("TRUNCATE vertex_index");
        session.execute("TRUNCATE vertex_property_index");
        session.execute("TRUNCATE entity_claims");
        session.execute("TRUNCATE type_definitions");
        session.execute("TRUNCATE type_definitions_by_category");
        typeDefCache.invalidateAll();
        vertexCache.get().clear();
        txBuffer.get().clear();
    }

    @Override
    public Set getOpenTransactions() {
        return Collections.emptySet();
    }

    @Override
    public void exportToGson(OutputStream os) throws IOException {
        throw new UnsupportedOperationException("exportToGson is not supported in Cassandra graph backend");
    }

    // ---- Gremlin methods (not supported in Cassandra backend) ----

    @Override
    public GroovyExpression generatePersisentToLogicalConversionExpression(GroovyExpression valueExpr, AtlasType type) {
        throw new UnsupportedOperationException("Gremlin is not supported in Cassandra graph backend");
    }

    @Override
    public boolean isPropertyValueConversionNeeded(AtlasType type) {
        return false;
    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {
        return GremlinVersion.THREE;
    }

    @Override
    public boolean requiresInitialIndexedPredicate() {
        return false;
    }

    @Override
    public GroovyExpression getInitialIndexedPredicate(GroovyExpression parent) {
        throw new UnsupportedOperationException("Gremlin is not supported in Cassandra graph backend");
    }

    @Override
    public GroovyExpression addOutputTransformationPredicate(GroovyExpression expr, boolean isSelect, boolean isPath) {
        throw new UnsupportedOperationException("Gremlin is not supported in Cassandra graph backend");
    }

    @Override
    public javax.script.ScriptEngine getGremlinScriptEngine() throws AtlasBaseException {
        throw new UnsupportedOperationException("Gremlin is not supported in Cassandra graph backend");
    }

    @Override
    public void releaseGremlinScriptEngine(javax.script.ScriptEngine scriptEngine) {
        // no-op
    }

    @Override
    public Object executeGremlinScript(String query, boolean isPath) throws AtlasBaseException {
        throw new UnsupportedOperationException("Gremlin is not supported in Cassandra graph backend");
    }

    @Override
    public Object executeGremlinScript(javax.script.ScriptEngine scriptEngine,
                                       java.util.Map<? extends String, ? extends Object> bindings,
                                       String query, boolean isPath) throws javax.script.ScriptException {
        throw new UnsupportedOperationException("Gremlin is not supported in Cassandra graph backend");
    }

    // ---- Internal accessors ----

    public CqlSession getSession() {
        return session;
    }

    public VertexRepository getVertexRepository() {
        return vertexRepository;
    }

    public EdgeRepository getEdgeRepository() {
        return edgeRepository;
    }

    public IndexRepository getIndexRepository() {
        return indexRepository;
    }

    public TypeDefCache getTypeDefCache() {
        return typeDefCache;
    }

    public Map<String, CassandraPropertyKey> getPropertyKeysMap() {
        return propertyKeys;
    }

    public Map<String, CassandraEdgeLabel> getEdgeLabelsMap() {
        return edgeLabels;
    }

    public Map<String, CassandraGraphIndex> getGraphIndexesMap() {
        return graphIndexes;
    }

    void notifyVertexDirty(CassandraVertex vertex) {
        txBuffer.get().markVertexDirty(vertex);
    }

    void notifyEdgeDirty(CassandraEdge edge) {
        txBuffer.get().markEdgeDirty(edge);
    }

    void clearVertexCache() {
        vertexCache.get().clear();
    }
}
