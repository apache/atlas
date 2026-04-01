package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class VertexRepository {

    private static final Logger LOG = LoggerFactory.getLogger(VertexRepository.class);

    private final CqlSession session;
    private PreparedStatement insertVertexStmt;
    private PreparedStatement selectVertexStmt;
    private PreparedStatement deleteVertexStmt;
    private PreparedStatement selectVerticesByPropertyStmt;

    public VertexRepository(CqlSession session) {
        this.session = session;
        prepareStatements();
    }

    private void prepareStatements() {
        insertVertexStmt = session.prepare(
            "INSERT INTO vertices (vertex_id, properties, vertex_label, type_name, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        );

        selectVertexStmt = session.prepare(
            "SELECT vertex_id, properties, vertex_label, type_name, state, created_at, modified_at " +
            "FROM vertices WHERE vertex_id = ?"
        );

        deleteVertexStmt = session.prepare(
            "DELETE FROM vertices WHERE vertex_id = ?"
        );
    }

    public void insertVertex(CassandraVertex vertex) {
        Map<String, Object> props = vertex.getProperties();
        String typeName = props.containsKey("__typeName") ? String.valueOf(props.get("__typeName")) : null;
        String state    = props.containsKey("__state") ? String.valueOf(props.get("__state")) : "ACTIVE";
        Instant now     = Instant.now();

        session.execute(insertVertexStmt.bind(
            vertex.getIdString(),
            AtlasType.toJson(props),
            vertex.getVertexLabel(),
            typeName,
            state,
            now,
            now
        ));
    }

    public void updateVertex(CassandraVertex vertex) {
        // For simplicity, update = full overwrite
        insertVertex(vertex);
    }

    public CassandraVertex getVertex(String vertexId, CassandraGraph graph) {
        ResultSet rs = session.execute(selectVertexStmt.bind(vertexId));
        Row row = rs.one();

        if (row == null) {
            return null;
        }

        return rowToVertex(row, graph);
    }

    /**
     * Checks vertex existence using LOCAL_QUORUM consistency. Use for correctness-critical
     * checks (e.g., orphan edge cleanup) where a recently-written vertex must be visible.
     */
    public boolean vertexExistsQuorum(String vertexId) {
        SimpleStatement stmt = SimpleStatement.builder("SELECT vertex_id FROM vertices WHERE vertex_id = ?")
                .addPositionalValue(vertexId)
                .setConsistencyLevel(com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM)
                .build();
        ResultSet rs = session.execute(stmt);
        return rs.one() != null;
    }

    /**
     * Fetch multiple vertices concurrently using async Cassandra queries.
     * All queries are fired in parallel and results collected, reducing
     * wall-clock time from N sequential round-trips to ~1 round-trip.
     */
    public Map<String, CassandraVertex> getVerticesAsync(Collection<String> vertexIds, CassandraGraph graph) {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Fire all queries concurrently
        Map<String, CompletionStage<AsyncResultSet>> futures = new LinkedHashMap<>();
        for (String vertexId : vertexIds) {
            futures.put(vertexId, session.executeAsync(selectVertexStmt.bind(vertexId)));
        }

        // Collect results
        Map<String, CassandraVertex> results = new LinkedHashMap<>();
        for (Map.Entry<String, CompletionStage<AsyncResultSet>> entry : futures.entrySet()) {
            try {
                AsyncResultSet rs = entry.getValue().toCompletableFuture().join();
                Row row = rs.one();
                if (row != null) {
                    CassandraVertex vertex = rowToVertex(row, graph);
                    results.put(vertex.getIdString(), vertex);
                }
            } catch (Exception e) {
                LOG.warn("Failed to fetch vertex {}", entry.getKey(), e);
            }
        }

        return results;
    }

    public List<CassandraVertex> getVertices(Collection<String> vertexIds, CassandraGraph graph) {
        return new ArrayList<>(getVerticesAsync(vertexIds, graph).values());
    }

    public void deleteVertex(String vertexId) {
        session.execute(deleteVertexStmt.bind(vertexId));
    }

    public void batchInsertVertices(List<CassandraVertex> vertices) {
        if (vertices.isEmpty()) {
            return;
        }

        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);

        for (CassandraVertex vertex : vertices) {
            batchBuilder.addStatement(bindInsertVertex(vertex));
        }

        session.execute(batchBuilder.build());
    }

    /**
     * Returns a bound INSERT statement for the given vertex without executing it.
     * Used by CassandraGraph.commit() to combine vertex + index writes in a single LOGGED batch.
     */
    public BoundStatement bindInsertVertex(CassandraVertex vertex) {
        Map<String, Object> props = vertex.getProperties();
        String typeName = props.containsKey("__typeName") ? String.valueOf(props.get("__typeName")) : null;
        String state    = props.containsKey("__state") ? String.valueOf(props.get("__state")) : "ACTIVE";
        Instant now     = Instant.now();

        return insertVertexStmt.bind(
            vertex.getIdString(),
            AtlasType.toJson(props),
            vertex.getVertexLabel(),
            typeName,
            state,
            now,
            now
        );
    }

    @SuppressWarnings("unchecked")
    private CassandraVertex rowToVertex(Row row, CassandraGraph graph) {
        String vertexId    = row.getString("vertex_id");
        String propsJson   = row.getString("properties");
        String vertexLabel = row.getString("vertex_label");

        Map<String, Object> props = new LinkedHashMap<>();
        if (propsJson != null && !propsJson.isEmpty()) {
            Map<String, Object> rawProps = AtlasType.fromJson(propsJson, Map.class);
            if (rawProps != null) {
                // Normalize property names: strip JanusGraph type-qualified prefixes.
                // Migrated data may have keys like "__type.Asset.certificateUpdatedAt"
                // or "Referenceable.qualifiedName" — Atlas expects just "certificateUpdatedAt"
                // and "qualifiedName".
                for (Map.Entry<String, Object> entry : rawProps.entrySet()) {
                    String key = normalizePropertyName(entry.getKey());
                    props.put(key, entry.getValue());
                }
            } else {
                LOG.warn("rowToVertex: AtlasType.fromJson returned null for vertex_id={}. propsJson length={}",
                         vertexId, propsJson.length());
            }
        } else {
            LOG.warn("rowToVertex: empty/null properties JSON for vertex_id={}", vertexId);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("rowToVertex: vertex_id={}, label={}, propCount={}, keys={}",
                      vertexId, vertexLabel, props.size(), props.keySet());
        }

        CassandraVertex vertex = new CassandraVertex(vertexId, vertexLabel, props, graph);
        return vertex;
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
    /**
     * Streams all vertices in the table, filtering by qualifiedName prefix.
     * Uses Cassandra driver's automatic paging to avoid loading the full table into memory.
     *
     * For each page of rows:
     * 1. Fast pre-filter: check if raw JSON contains the prefix string (skips ~99% of rows)
     * 2. Deserialize only matching rows and verify qualifiedName properly
     * 3. Invoke the callback with each batch of matching GUIDs
     *
     * @param qualifiedNamePrefix  the prefix to match (e.g., "default/snowflake/1772139790")
     * @param fetchSize            Cassandra page size (rows per network round-trip)
     * @param reindexBatchSize     how many matching GUIDs to collect before calling the callback
     * @param batchCallback        receives batches of matching GUIDs for reindex
     * @return total number of matching vertices found
     */
    public int scanVerticesByQualifiedNamePrefix(String qualifiedNamePrefix, int fetchSize,
                                                  int reindexBatchSize,
                                                  java.util.function.Consumer<List<String>> batchCallback) {
        // Full table scan — use SimpleStatement with page size to stream rows
        com.datastax.oss.driver.api.core.cql.SimpleStatement stmt =
                com.datastax.oss.driver.api.core.cql.SimpleStatement.builder(
                        "SELECT vertex_id, properties FROM vertices")
                .setPageSize(fetchSize)
                .build();

        ResultSet rs = session.execute(stmt);

        int totalScanned = 0;
        int totalMatched = 0;
        List<String> batch = new ArrayList<>(reindexBatchSize);

        // The driver fetches rows in pages automatically — we iterate without loading all into memory
        for (Row row : rs) {
            totalScanned++;

            String propsJson = row.getString("properties");
            if (propsJson == null || propsJson.isEmpty()) {
                continue;
            }

            // Fast pre-filter: raw string search avoids JSON deserialization for non-matching rows
            if (!propsJson.contains(qualifiedNamePrefix)) {
                continue;
            }

            // Deserialize and verify qualifiedName properly
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> props = AtlasType.fromJson(propsJson, Map.class);
                if (props == null) continue;

                // Check both raw and normalized property names
                Object qnObj = props.get("qualifiedName");
                if (qnObj == null) qnObj = props.get("Referenceable.qualifiedName");
                if (qnObj == null) {
                    // The prefix matched inside the JSON but not in qualifiedName — skip
                    continue;
                }

                String qn = String.valueOf(qnObj);
                if (!qn.startsWith(qualifiedNamePrefix)) {
                    continue;
                }

                // Extract GUID
                Object guidObj = props.get("__guid");
                if (guidObj == null) continue;

                batch.add(String.valueOf(guidObj));
                totalMatched++;

                if (batch.size() >= reindexBatchSize) {
                    batchCallback.accept(new ArrayList<>(batch));
                    batch.clear();
                }
            } catch (Exception e) {
                LOG.warn("scanVerticesByQualifiedNamePrefix: failed to parse properties for vertex {}",
                        row.getString("vertex_id"), e);
            }

            if (totalScanned % 10000 == 0) {
                LOG.info("scanVerticesByQualifiedNamePrefix: scanned {} vertices, {} matched so far (prefix='{}')",
                        totalScanned, totalMatched, qualifiedNamePrefix);
            }
        }

        // Flush remaining batch
        if (!batch.isEmpty()) {
            batchCallback.accept(new ArrayList<>(batch));
        }

        LOG.info("scanVerticesByQualifiedNamePrefix: completed — scanned {} vertices, {} matched prefix '{}'",
                totalScanned, totalMatched, qualifiedNamePrefix);

        return totalMatched;
    }

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
}
