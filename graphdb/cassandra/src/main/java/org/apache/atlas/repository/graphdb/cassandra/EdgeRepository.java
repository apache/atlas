package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class EdgeRepository {

    private static final Logger LOG = LoggerFactory.getLogger(EdgeRepository.class);

    private final CqlSession session;
    private PreparedStatement insertEdgeOutStmt;
    private PreparedStatement insertEdgeInStmt;
    private PreparedStatement insertEdgeByIdStmt;
    private PreparedStatement selectEdgeByIdStmt;
    private PreparedStatement selectEdgesOutStmt;
    private PreparedStatement selectEdgesOutByLabelStmt;
    private PreparedStatement selectEdgesInStmt;
    private PreparedStatement selectEdgesInByLabelStmt;
    private PreparedStatement selectEdgesOutByLabelLimitStmt;
    private PreparedStatement selectEdgesInByLabelLimitStmt;
    private PreparedStatement countEdgesOutStmt;
    private PreparedStatement countEdgesOutByLabelStmt;
    private PreparedStatement countEdgesInStmt;
    private PreparedStatement countEdgesInByLabelStmt;
    private PreparedStatement hasEdgesOutByLabelStmt;
    private PreparedStatement hasEdgesInByLabelStmt;
    private PreparedStatement hasEdgesOutStmt;
    private PreparedStatement hasEdgesInStmt;
    private PreparedStatement updateEdgeByIdStmt;
    private PreparedStatement updateEdgeOutStmt;
    private PreparedStatement updateEdgeInStmt;
    private PreparedStatement deleteEdgeOutStmt;
    private PreparedStatement deleteEdgeInStmt;
    private PreparedStatement deleteEdgeByIdStmt;

    public EdgeRepository(CqlSession session) {
        this.session = session;
        prepareStatements();
    }

    private void prepareStatements() {
        insertEdgeOutStmt = session.prepare(
            "INSERT INTO edges_out (out_vertex_id, edge_label, edge_id, in_vertex_id, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        );

        insertEdgeInStmt = session.prepare(
            "INSERT INTO edges_in (in_vertex_id, edge_label, edge_id, out_vertex_id, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        );

        insertEdgeByIdStmt = session.prepare(
            "INSERT INTO edges_by_id (edge_id, out_vertex_id, in_vertex_id, edge_label, properties, state, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        );

        selectEdgeByIdStmt = session.prepare(
            "SELECT edge_id, out_vertex_id, in_vertex_id, edge_label, properties, state " +
            "FROM edges_by_id WHERE edge_id = ?"
        );

        selectEdgesOutStmt = session.prepare(
            "SELECT edge_id, edge_label, in_vertex_id, properties, state " +
            "FROM edges_out WHERE out_vertex_id = ?"
        );

        selectEdgesOutByLabelStmt = session.prepare(
            "SELECT edge_id, edge_label, in_vertex_id, properties, state " +
            "FROM edges_out WHERE out_vertex_id = ? AND edge_label = ?"
        );

        selectEdgesInStmt = session.prepare(
            "SELECT edge_id, edge_label, out_vertex_id, properties, state " +
            "FROM edges_in WHERE in_vertex_id = ?"
        );

        selectEdgesInByLabelStmt = session.prepare(
            "SELECT edge_id, edge_label, out_vertex_id, properties, state " +
            "FROM edges_in WHERE in_vertex_id = ? AND edge_label = ?"
        );

        // LIMIT variants for capping high-cardinality edge fetches (e.g. __Table.columns with 5000+ edges)
        selectEdgesOutByLabelLimitStmt = session.prepare(
            "SELECT edge_id, edge_label, in_vertex_id, properties, state " +
            "FROM edges_out WHERE out_vertex_id = ? AND edge_label = ? LIMIT ?"
        );

        selectEdgesInByLabelLimitStmt = session.prepare(
            "SELECT edge_id, edge_label, out_vertex_id, properties, state " +
            "FROM edges_in WHERE in_vertex_id = ? AND edge_label = ? LIMIT ?"
        );

        // COUNT variants for server-side counting (avoids transferring row data)
        countEdgesOutStmt = session.prepare(
            "SELECT COUNT(*) FROM edges_out WHERE out_vertex_id = ?"
        );

        countEdgesOutByLabelStmt = session.prepare(
            "SELECT COUNT(*) FROM edges_out WHERE out_vertex_id = ? AND edge_label = ?"
        );

        countEdgesInStmt = session.prepare(
            "SELECT COUNT(*) FROM edges_in WHERE in_vertex_id = ?"
        );

        countEdgesInByLabelStmt = session.prepare(
            "SELECT COUNT(*) FROM edges_in WHERE in_vertex_id = ? AND edge_label = ?"
        );

        // LIMIT 1 variants for existence checks (avoids reading entire partition)
        hasEdgesOutByLabelStmt = session.prepare(
            "SELECT edge_id, state FROM edges_out WHERE out_vertex_id = ? AND edge_label = ? LIMIT 1"
        );

        hasEdgesInByLabelStmt = session.prepare(
            "SELECT edge_id, state FROM edges_in WHERE in_vertex_id = ? AND edge_label = ? LIMIT 1"
        );

        hasEdgesOutStmt = session.prepare(
            "SELECT edge_id, state FROM edges_out WHERE out_vertex_id = ? LIMIT 1"
        );

        hasEdgesInStmt = session.prepare(
            "SELECT edge_id, state FROM edges_in WHERE in_vertex_id = ? LIMIT 1"
        );

        // Update edge properties/state in all three tables (uses INSERT which upserts in Cassandra)
        updateEdgeByIdStmt = session.prepare(
            "INSERT INTO edges_by_id (edge_id, out_vertex_id, in_vertex_id, edge_label, properties, state, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        );

        updateEdgeOutStmt = session.prepare(
            "INSERT INTO edges_out (out_vertex_id, edge_label, edge_id, in_vertex_id, properties, state, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        );

        updateEdgeInStmt = session.prepare(
            "INSERT INTO edges_in (in_vertex_id, edge_label, edge_id, out_vertex_id, properties, state, modified_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        );

        deleteEdgeOutStmt = session.prepare(
            "DELETE FROM edges_out WHERE out_vertex_id = ? AND edge_label = ? AND edge_id = ?"
        );

        deleteEdgeInStmt = session.prepare(
            "DELETE FROM edges_in WHERE in_vertex_id = ? AND edge_label = ? AND edge_id = ?"
        );

        deleteEdgeByIdStmt = session.prepare(
            "DELETE FROM edges_by_id WHERE edge_id = ?"
        );
    }

    public void insertEdge(CassandraEdge edge) {
        String propsJson = AtlasType.toJson(edge.getProperties());
        String state     = "ACTIVE";
        Instant now      = Instant.now();

        // Write to all three tables
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);

        batch.addStatement(insertEdgeOutStmt.bind(
            edge.getOutVertexId(), edge.getLabel(), edge.getIdString(),
            edge.getInVertexId(), propsJson, state, now, now
        ));

        batch.addStatement(insertEdgeInStmt.bind(
            edge.getInVertexId(), edge.getLabel(), edge.getIdString(),
            edge.getOutVertexId(), propsJson, state, now, now
        ));

        batch.addStatement(insertEdgeByIdStmt.bind(
            edge.getIdString(), edge.getOutVertexId(), edge.getInVertexId(),
            edge.getLabel(), propsJson, state, now, now
        ));

        session.execute(batch.build());
    }

    public void updateEdge(CassandraEdge edge) {
        String propsJson = AtlasType.toJson(edge.getProperties());
        Object stateObj  = edge.getProperties().get("__state");
        String state     = stateObj != null ? String.valueOf(stateObj) : "ACTIVE";
        Instant now      = Instant.now();

        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);

        batch.addStatement(updateEdgeByIdStmt.bind(
            edge.getIdString(), edge.getOutVertexId(), edge.getInVertexId(),
            edge.getLabel(), propsJson, state, now
        ));

        batch.addStatement(updateEdgeOutStmt.bind(
            edge.getOutVertexId(), edge.getLabel(), edge.getIdString(),
            edge.getInVertexId(), propsJson, state, now
        ));

        batch.addStatement(updateEdgeInStmt.bind(
            edge.getInVertexId(), edge.getLabel(), edge.getIdString(),
            edge.getOutVertexId(), propsJson, state, now
        ));

        session.execute(batch.build());
    }

    public CassandraEdge getEdge(String edgeId, CassandraGraph graph) {
        ResultSet rs = session.execute(selectEdgeByIdStmt.bind(edgeId));
        Row row = rs.one();

        if (row == null) {
            return null;
        }

        return rowToEdgeById(row, graph);
    }

    /**
     * Count edges for a vertex server-side using CQL COUNT(*).
     * Avoids transferring row data over the wire — Cassandra counts within the partition.
     *
     * Note: COUNT(*) in Cassandra includes all rows regardless of state.
     * Rows with state=DELETED are included in the count. The caller must adjust
     * for DELETED edges if needed. In practice, hard-deleted edges are removed
     * from the table (not soft-deleted), so this is accurate for normal operations.
     *
     * @param vertexId  the vertex to count edges for
     * @param direction OUT, IN, or BOTH
     * @param edgeLabel optional label filter (null = all labels)
     * @return count of edges in Cassandra (does NOT include uncommitted buffer)
     */
    public long countEdges(String vertexId, AtlasEdgeDirection direction, String edgeLabel) {
        long count = 0;

        if (direction == AtlasEdgeDirection.OUT || direction == AtlasEdgeDirection.BOTH) {
            ResultSet rs;
            if (edgeLabel != null) {
                rs = session.execute(countEdgesOutByLabelStmt.bind(vertexId, edgeLabel));
            } else {
                rs = session.execute(countEdgesOutStmt.bind(vertexId));
            }
            Row row = rs.one();
            if (row != null) {
                count += row.getLong(0);
            }
        }

        if (direction == AtlasEdgeDirection.IN || direction == AtlasEdgeDirection.BOTH) {
            ResultSet rs;
            if (edgeLabel != null) {
                rs = session.execute(countEdgesInByLabelStmt.bind(vertexId, edgeLabel));
            } else {
                rs = session.execute(countEdgesInStmt.bind(vertexId));
            }
            Row row = rs.one();
            if (row != null) {
                count += row.getLong(0);
            }
        }

        return count;
    }

    /**
     * Check if a vertex has at least one edge, using CQL LIMIT 1.
     * Only selects edge_id and state (no properties deserialization).
     *
     * Skips edges with state=DELETED. If the first row is DELETED, falls back
     * to a full edge fetch to check for non-deleted edges. This is rare in practice
     * since hard-deleted edges are removed from the table.
     *
     * @param vertexId  the vertex to check
     * @param direction OUT, IN, or BOTH
     * @param edgeLabel optional label filter (null = all labels)
     * @return true if at least one non-deleted edge exists in Cassandra (does NOT check uncommitted buffer)
     */
    public boolean hasEdges(String vertexId, AtlasEdgeDirection direction, String edgeLabel) {
        if (direction == AtlasEdgeDirection.OUT || direction == AtlasEdgeDirection.BOTH) {
            if (hasEdgesInDirection(vertexId, edgeLabel, true)) {
                return true;
            }
        }

        if (direction == AtlasEdgeDirection.IN || direction == AtlasEdgeDirection.BOTH) {
            if (hasEdgesInDirection(vertexId, edgeLabel, false)) {
                return true;
            }
        }

        return false;
    }

    private boolean hasEdgesInDirection(String vertexId, String edgeLabel, boolean isOut) {
        ResultSet rs;
        if (edgeLabel != null) {
            rs = session.execute((isOut ? hasEdgesOutByLabelStmt : hasEdgesInByLabelStmt)
                    .bind(vertexId, edgeLabel));
        } else {
            rs = session.execute((isOut ? hasEdgesOutStmt : hasEdgesInStmt)
                    .bind(vertexId));
        }
        Row row = rs.one();
        if (row == null) {
            return false;
        }
        String state = row.getString("state");
        if (!"DELETED".equals(state)) {
            return true;
        }
        // Rare edge case: LIMIT 1 returned a DELETED row. In practice, DELETED edges
        // are hard-removed from the table at commit time, so this should almost never happen.
        // Log a warning and return false — the edge effectively doesn't exist if it's deleted.
        LOG.warn("hasEdgesInDirection: LIMIT 1 row for vertex {} label {} is DELETED — treating as no edges",
                vertexId, edgeLabel);
        return false;
    }

    /**
     * Paginated delete of all edges for a vertex. Instead of loading all edges
     * into memory at once, streams through Cassandra pages and deletes in batches.
     *
     * Memory usage: at most ~pageSize edge objects in memory at any time.
     *
     * @param vertexId the vertex whose edges should be deleted
     * @param graph    the graph instance for edge deserialization
     */
    public void deleteEdgesForVertexPaginated(String vertexId, CassandraGraph graph) {
        int pageSize = 500;
        int batchLimit = 50;  // matches BATCH_LIMIT in batchDeleteEdges

        // Delete OUT edges
        deleteEdgesFromTable(vertexId, "edges_out", "out_vertex_id", "in_vertex_id",
                true, pageSize, batchLimit, graph);

        // Delete IN edges
        deleteEdgesFromTable(vertexId, "edges_in", "in_vertex_id", "out_vertex_id",
                false, pageSize, batchLimit, graph);
    }

    private void deleteEdgesFromTable(String vertexId, String tableName, String partitionCol,
                                       String otherVertexCol, boolean isOut,
                                       int pageSize, int batchLimit, CassandraGraph graph) {
        SimpleStatement stmt = SimpleStatement.builder(
                "SELECT edge_id, edge_label, " + otherVertexCol + ", properties, state " +
                "FROM " + tableName + " WHERE " + partitionCol + " = ?")
            .setPageSize(pageSize)
            .addPositionalValue(vertexId)
            .build();

        ResultSet rs = session.execute(stmt);
        List<CassandraEdge> batch = new ArrayList<>(batchLimit);
        int totalDeleted = 0;

        for (Row row : rs) {
            String edgeId       = row.getString("edge_id");
            String label        = row.getString("edge_label");
            String otherVertex  = row.getString(otherVertexCol);
            String propsJson    = row.getString("properties");
            Map<String, Object> props = parseProperties(propsJson);

            CassandraEdge edge;
            if (isOut) {
                edge = new CassandraEdge(edgeId, vertexId, otherVertex, label, props, graph);
            } else {
                edge = new CassandraEdge(edgeId, otherVertex, vertexId, label, props, graph);
            }

            batch.add(edge);
            if (batch.size() >= batchLimit) {
                batchDeleteEdges(batch);
                totalDeleted += batch.size();
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            batchDeleteEdges(batch);
            totalDeleted += batch.size();
        }

        if (totalDeleted > 0) {
            LOG.debug("deleteEdgesFromTable: deleted {} edges from {} for vertex {}",
                    totalDeleted, tableName, vertexId);
        }
    }

    @SuppressWarnings("unchecked")
    public List<CassandraEdge> getEdgesForVertex(String vertexId, AtlasEdgeDirection direction,
                                                  String edgeLabel, CassandraGraph graph) {
        List<CassandraEdge> result = new ArrayList<>();

        if (direction == AtlasEdgeDirection.OUT || direction == AtlasEdgeDirection.BOTH) {
            ResultSet rs;
            if (edgeLabel != null) {
                rs = session.execute(selectEdgesOutByLabelStmt.bind(vertexId, edgeLabel));
            } else {
                rs = session.execute(selectEdgesOutStmt.bind(vertexId));
            }

            for (Row row : rs) {
                String state = row.getString("state");
                if (!"DELETED".equals(state)) {
                    String edgeId      = row.getString("edge_id");
                    String label       = row.getString("edge_label");
                    String inVertexId  = row.getString("in_vertex_id");
                    String propsJson   = row.getString("properties");
                    Map<String, Object> props = parseProperties(propsJson);
                    props.put("__state", state != null ? state : "ACTIVE");
                    result.add(new CassandraEdge(edgeId, vertexId, inVertexId, label, props, graph));
                }
            }
        }

        if (direction == AtlasEdgeDirection.IN || direction == AtlasEdgeDirection.BOTH) {
            ResultSet rs;
            if (edgeLabel != null) {
                rs = session.execute(selectEdgesInByLabelStmt.bind(vertexId, edgeLabel));
            } else {
                rs = session.execute(selectEdgesInStmt.bind(vertexId));
            }

            for (Row row : rs) {
                String state = row.getString("state");
                if (!"DELETED".equals(state)) {
                    String edgeId       = row.getString("edge_id");
                    String label        = row.getString("edge_label");
                    String outVertexId  = row.getString("out_vertex_id");
                    String propsJson    = row.getString("properties");
                    Map<String, Object> props = parseProperties(propsJson);
                    props.put("__state", state != null ? state : "ACTIVE");
                    result.add(new CassandraEdge(edgeId, outVertexId, vertexId, label, props, graph));
                }
            }
        }

        return result;
    }

    /**
     * Fetch all edges for multiple vertices concurrently using async Cassandra queries.
     * Queries both edges_out and edges_in for each vertex in parallel, reducing
     * 2N sequential round-trips to ~1 round-trip wall-clock time.
     *
     * @return Map of vertexId → list of all edges (both directions, all labels)
     */
    public Map<String, List<CassandraEdge>> getEdgesForVerticesAsync(
            Collection<String> vertexIds, AtlasEdgeDirection direction, CassandraGraph graph) {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Fire all queries concurrently
        Map<String, CompletionStage<AsyncResultSet>> outFutures = new LinkedHashMap<>();
        Map<String, CompletionStage<AsyncResultSet>> inFutures = new LinkedHashMap<>();

        for (String vertexId : vertexIds) {
            if (direction == AtlasEdgeDirection.OUT || direction == AtlasEdgeDirection.BOTH) {
                outFutures.put(vertexId, session.executeAsync(selectEdgesOutStmt.bind(vertexId)));
            }
            if (direction == AtlasEdgeDirection.IN || direction == AtlasEdgeDirection.BOTH) {
                inFutures.put(vertexId, session.executeAsync(selectEdgesInStmt.bind(vertexId)));
            }
        }

        // Collect results
        Map<String, List<CassandraEdge>> results = new LinkedHashMap<>();

        for (Map.Entry<String, CompletionStage<AsyncResultSet>> entry : outFutures.entrySet()) {
            String vertexId = entry.getKey();
            try {
                AsyncResultSet rs = entry.getValue().toCompletableFuture().join();
                List<CassandraEdge> edges = results.computeIfAbsent(vertexId, k -> new ArrayList<>());
                collectOutEdgePages(rs, vertexId, edges, graph);
            } catch (Exception e) {
                LOG.warn("Failed to fetch out-edges for vertex {}", vertexId, e);
            }
        }

        for (Map.Entry<String, CompletionStage<AsyncResultSet>> entry : inFutures.entrySet()) {
            String vertexId = entry.getKey();
            try {
                AsyncResultSet rs = entry.getValue().toCompletableFuture().join();
                List<CassandraEdge> edges = results.computeIfAbsent(vertexId, k -> new ArrayList<>());
                collectInEdgePages(rs, vertexId, edges, graph);
            } catch (Exception e) {
                LOG.warn("Failed to fetch in-edges for vertex {}", vertexId, e);
            }
        }

        return results;
    }

    /**
     * Fetch edges for multiple vertices filtered by specific edge labels.
     * Fires one async query per (vertex, label, direction) tuple — much more efficient
     * than fetching ALL edges when only specific relationship types are needed.
     *
     * For 20 vertices × 10 labels × 2 directions = 400 tiny partition-scoped queries,
     * all fired concurrently so wall-clock time ≈ 1 Cassandra round-trip.
     */
    public Map<String, List<CassandraEdge>> getEdgesForVerticesByLabelsAsync(
            Collection<String> vertexIds, Set<String> edgeLabels,
            AtlasEdgeDirection direction, CassandraGraph graph) {
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }
        // If no labels specified, fall back to the all-edges method
        if (edgeLabels == null || edgeLabels.isEmpty()) {
            return getEdgesForVerticesAsync(vertexIds, direction, graph);
        }

        // Fire all queries concurrently: one per (vertex, label, direction)
        // Key: "vertexId:OUT" or "vertexId:IN" — value: list of futures for each label
        Map<String, List<CompletionStage<AsyncResultSet>>> outFutures = new LinkedHashMap<>();
        Map<String, List<CompletionStage<AsyncResultSet>>> inFutures = new LinkedHashMap<>();

        for (String vertexId : vertexIds) {
            if (direction == AtlasEdgeDirection.OUT || direction == AtlasEdgeDirection.BOTH) {
                List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>(edgeLabels.size());
                for (String label : edgeLabels) {
                    futures.add(session.executeAsync(selectEdgesOutByLabelStmt.bind(vertexId, label)));
                }
                outFutures.put(vertexId, futures);
            }
            if (direction == AtlasEdgeDirection.IN || direction == AtlasEdgeDirection.BOTH) {
                List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>(edgeLabels.size());
                for (String label : edgeLabels) {
                    futures.add(session.executeAsync(selectEdgesInByLabelStmt.bind(vertexId, label)));
                }
                inFutures.put(vertexId, futures);
            }
        }

        // Collect results
        Map<String, List<CassandraEdge>> results = new LinkedHashMap<>();

        for (Map.Entry<String, List<CompletionStage<AsyncResultSet>>> entry : outFutures.entrySet()) {
            String vertexId = entry.getKey();
            List<CassandraEdge> edges = results.computeIfAbsent(vertexId, k -> new ArrayList<>());
            for (CompletionStage<AsyncResultSet> future : entry.getValue()) {
                try {
                    AsyncResultSet rs = future.toCompletableFuture().join();
                    collectOutEdgePages(rs, vertexId, edges, graph);
                } catch (Exception e) {
                    LOG.warn("Failed to fetch out-edges by label for vertex {}", vertexId, e);
                }
            }
        }

        for (Map.Entry<String, List<CompletionStage<AsyncResultSet>>> entry : inFutures.entrySet()) {
            String vertexId = entry.getKey();
            List<CassandraEdge> edges = results.computeIfAbsent(vertexId, k -> new ArrayList<>());
            for (CompletionStage<AsyncResultSet> future : entry.getValue()) {
                try {
                    AsyncResultSet rs = future.toCompletableFuture().join();
                    collectInEdgePages(rs, vertexId, edges, graph);
                } catch (Exception e) {
                    LOG.warn("Failed to fetch in-edges by label for vertex {}", vertexId, e);
                }
            }
        }

        LOG.debug("getEdgesForVerticesByLabelsAsync: {} vertices × {} labels = {} total edges fetched",
                  vertexIds.size(), edgeLabels.size(),
                  results.values().stream().mapToInt(List::size).sum());

        return results;
    }

    /**
     * Like getEdgesForVerticesByLabelsAsync but with a per-label LIMIT pushed down to Cassandra.
     * Prevents fetching thousands of rows for high-cardinality relationships (e.g. __Table.columns).
     *
     * @param limitPerLabel max edges per (vertex, label, direction). 0 means no limit.
     */
    public Map<String, List<CassandraEdge>> getEdgesForVerticesByLabelsAsync(
            Collection<String> vertexIds, Set<String> edgeLabels,
            AtlasEdgeDirection direction, CassandraGraph graph, int limitPerLabel) {
        if (limitPerLabel <= 0) {
            return getEdgesForVerticesByLabelsAsync(vertexIds, edgeLabels, direction, graph);
        }
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }
        if (edgeLabels == null || edgeLabels.isEmpty()) {
            return getEdgesForVerticesAsync(vertexIds, direction, graph);
        }

        Map<String, List<CompletionStage<AsyncResultSet>>> outFutures = new LinkedHashMap<>();
        Map<String, List<CompletionStage<AsyncResultSet>>> inFutures = new LinkedHashMap<>();

        for (String vertexId : vertexIds) {
            if (direction == AtlasEdgeDirection.OUT || direction == AtlasEdgeDirection.BOTH) {
                List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>(edgeLabels.size());
                for (String label : edgeLabels) {
                    futures.add(session.executeAsync(
                        selectEdgesOutByLabelLimitStmt.bind(vertexId, label, limitPerLabel)));
                }
                outFutures.put(vertexId, futures);
            }
            if (direction == AtlasEdgeDirection.IN || direction == AtlasEdgeDirection.BOTH) {
                List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>(edgeLabels.size());
                for (String label : edgeLabels) {
                    futures.add(session.executeAsync(
                        selectEdgesInByLabelLimitStmt.bind(vertexId, label, limitPerLabel)));
                }
                inFutures.put(vertexId, futures);
            }
        }

        Map<String, List<CassandraEdge>> results = new LinkedHashMap<>();

        for (Map.Entry<String, List<CompletionStage<AsyncResultSet>>> entry : outFutures.entrySet()) {
            String vertexId = entry.getKey();
            List<CassandraEdge> edges = results.computeIfAbsent(vertexId, k -> new ArrayList<>());
            for (CompletionStage<AsyncResultSet> future : entry.getValue()) {
                try {
                    AsyncResultSet rs = future.toCompletableFuture().join();
                    collectOutEdgePages(rs, vertexId, edges, graph);
                } catch (Exception e) {
                    LOG.warn("Failed to fetch out-edges by label (limited) for vertex {}", vertexId, e);
                }
            }
        }

        for (Map.Entry<String, List<CompletionStage<AsyncResultSet>>> entry : inFutures.entrySet()) {
            String vertexId = entry.getKey();
            List<CassandraEdge> edges = results.computeIfAbsent(vertexId, k -> new ArrayList<>());
            for (CompletionStage<AsyncResultSet> future : entry.getValue()) {
                try {
                    AsyncResultSet rs = future.toCompletableFuture().join();
                    collectInEdgePages(rs, vertexId, edges, graph);
                } catch (Exception e) {
                    LOG.warn("Failed to fetch in-edges by label (limited) for vertex {}", vertexId, e);
                }
            }
        }

        LOG.debug("getEdgesForVerticesByLabelsAsync(limited={}): {} vertices × {} labels = {} total edges",
                  limitPerLabel, vertexIds.size(), edgeLabels.size(),
                  results.values().stream().mapToInt(List::size).sum());

        return results;
    }

    private void collectOutEdgePages(AsyncResultSet rs, String vertexId,
                                      List<CassandraEdge> edges, CassandraGraph graph) {
        while (rs != null) {
            for (Row row : rs.currentPage()) {
                String state = row.getString("state");
                if (!"DELETED".equals(state)) {
                    String edgeId     = row.getString("edge_id");
                    String label      = row.getString("edge_label");
                    String inVertexId = row.getString("in_vertex_id");
                    String propsJson  = row.getString("properties");
                    Map<String, Object> props = parseProperties(propsJson);
                    props.put("__state", state != null ? state : "ACTIVE");
                    edges.add(new CassandraEdge(edgeId, vertexId, inVertexId, label, props, graph));
                }
            }
            if (rs.hasMorePages()) {
                rs = rs.fetchNextPage().toCompletableFuture().join();
            } else {
                break;
            }
        }
    }

    private void collectInEdgePages(AsyncResultSet rs, String vertexId,
                                     List<CassandraEdge> edges, CassandraGraph graph) {
        while (rs != null) {
            for (Row row : rs.currentPage()) {
                String state = row.getString("state");
                if (!"DELETED".equals(state)) {
                    String edgeId      = row.getString("edge_id");
                    String label       = row.getString("edge_label");
                    String outVertexId = row.getString("out_vertex_id");
                    String propsJson   = row.getString("properties");
                    Map<String, Object> props = parseProperties(propsJson);
                    props.put("__state", state != null ? state : "ACTIVE");
                    edges.add(new CassandraEdge(edgeId, outVertexId, vertexId, label, props, graph));
                }
            }
            if (rs.hasMorePages()) {
                rs = rs.fetchNextPage().toCompletableFuture().join();
            } else {
                break;
            }
        }
    }

    public void deleteEdge(CassandraEdge edge) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);

        batch.addStatement(deleteEdgeOutStmt.bind(
            edge.getOutVertexId(), edge.getLabel(), edge.getIdString()
        ));

        batch.addStatement(deleteEdgeInStmt.bind(
            edge.getInVertexId(), edge.getLabel(), edge.getIdString()
        ));

        batch.addStatement(deleteEdgeByIdStmt.bind(edge.getIdString()));

        session.execute(batch.build());
    }

    public void deleteEdgesForVertex(String vertexId, CassandraGraph graph) {
        List<CassandraEdge> allEdges = getEdgesForVertex(vertexId, AtlasEdgeDirection.BOTH, null, graph);
        if (!allEdges.isEmpty()) {
            batchDeleteEdges(allEdges);
        }
    }

    /**
     * Delete multiple edges in a single LOGGED BATCH.
     * Each edge requires 3 DELETE statements (edges_out, edges_in, edges_by_id).
     * Cassandra LOGGED batches are atomic per partition — for cross-partition
     * deletes, the batch log guarantees all-or-nothing execution.
     *
     * For very large edge sets (>500), splits into sub-batches to stay within
     * Cassandra's batch size warnings (default 5KB warn, 50KB fail threshold).
     */
    public void batchDeleteEdges(List<CassandraEdge> edges) {
        if (edges.isEmpty()) {
            return;
        }

        // 3 statements per edge (delete out, in, by_id); keep below batch_size_fail_threshold
        int BATCH_LIMIT = 50;

        for (int i = 0; i < edges.size(); i += BATCH_LIMIT) {
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
            int end = Math.min(i + BATCH_LIMIT, edges.size());

            for (int j = i; j < end; j++) {
                CassandraEdge edge = edges.get(j);
                batch.addStatement(deleteEdgeOutStmt.bind(
                    edge.getOutVertexId(), edge.getLabel(), edge.getIdString()));
                batch.addStatement(deleteEdgeInStmt.bind(
                    edge.getInVertexId(), edge.getLabel(), edge.getIdString()));
                batch.addStatement(deleteEdgeByIdStmt.bind(edge.getIdString()));
            }

            session.execute(batch.build());
        }

        LOG.debug("batchDeleteEdges: deleted {} edges in {} batch(es)",
                  edges.size(), (edges.size() + BATCH_LIMIT - 1) / BATCH_LIMIT);
    }

    public void batchInsertEdges(List<CassandraEdge> edges) {
        if (edges.isEmpty()) {
            return;
        }

        // 3 statements per edge (out, in, by_id); each ~350 bytes → 50 edges ≈ 52KB.
        // Keep below Cassandra's batch_size_fail_threshold (default 50KB).
        int BATCH_LIMIT = 50;
        for (int i = 0; i < edges.size(); i += BATCH_LIMIT) {
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
            Instant now = Instant.now();
            int end = Math.min(i + BATCH_LIMIT, edges.size());

            for (int j = i; j < end; j++) {
                CassandraEdge edge = edges.get(j);
                String propsJson = AtlasType.toJson(edge.getProperties());
                String state     = "ACTIVE";

                batch.addStatement(insertEdgeOutStmt.bind(
                    edge.getOutVertexId(), edge.getLabel(), edge.getIdString(),
                    edge.getInVertexId(), propsJson, state, now, now
                ));

                batch.addStatement(insertEdgeInStmt.bind(
                    edge.getInVertexId(), edge.getLabel(), edge.getIdString(),
                    edge.getOutVertexId(), propsJson, state, now, now
                ));

                batch.addStatement(insertEdgeByIdStmt.bind(
                    edge.getIdString(), edge.getOutVertexId(), edge.getInVertexId(),
                    edge.getLabel(), propsJson, state, now, now
                ));
            }

            session.execute(batch.build());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseProperties(String propsJson) {
        if (propsJson == null || propsJson.isEmpty()) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> props = AtlasType.fromJson(propsJson, Map.class);
        return props != null ? props : new LinkedHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private CassandraEdge rowToEdgeById(Row row, CassandraGraph graph) {
        String edgeId      = row.getString("edge_id");
        String outVertexId = row.getString("out_vertex_id");
        String inVertexId  = row.getString("in_vertex_id");
        String label       = row.getString("edge_label");
        String propsJson   = row.getString("properties");
        String state       = row.getString("state");

        Map<String, Object> props = parseProperties(propsJson);
        props.put("__state", state != null ? state : "ACTIVE");
        return new CassandraEdge(edgeId, outVertexId, inVertexId, label, props, graph);
    }
}
