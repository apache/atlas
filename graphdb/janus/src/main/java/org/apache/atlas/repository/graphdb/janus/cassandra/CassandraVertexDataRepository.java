package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


/**
 * Enhanced Cassandra implementation for vertex data repository with advanced
 * features like connection pooling, retry mechanisms, and better error handling.
 */
class CassandraVertexDataRepository implements VertexDataRepository {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraVertexDataRepository.class);

    // Maximum number of items in an IN clause for Cassandra
    // make it configurable
    private static final int MAX_IN_CLAUSE_ITEMS = 100;
    private final CqlSession session;
    private final String keyspace;
    private final String tableName;
    private final PreparedStatement insertVertexStatement;
    private final PreparedStatement deleteVertexStatement;
    private final PreparedStatement selectByIdStatement;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new enhanced Cassandra repository.
     *
     * @param session   The Cassandra session
     */
    @Inject
    public CassandraVertexDataRepository(CqlSession session) {
        this.session = session;
        this.keyspace = AtlasConfiguration.ATLAS_CASSANDRA_VANILLA_KEYSPACE.getString();
        this.tableName = AtlasConfiguration.ATLAS_CASSANDRA_VERTEX_TABLE.getString();
        this.insertVertexStatement = session.prepare(String.format(
                "INSERT INTO %s.%s (id, json_data, updated_at) VALUES (?, ?, ?)",
                keyspace,
                tableName));
        this.deleteVertexStatement = session.prepare(String.format(
                "DELETE FROM %s.%s WHERE id = ?",
                keyspace,
                tableName));
        this.selectByIdStatement = session.prepare(String.format(
                "SELECT id, json_data FROM %s.%s WHERE id = ?",
                keyspace,
                tableName));

        this.objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule("NumbersAsStringModule");
        module.addDeserializer(Object.class, new NumbersAsStringObjectDeserializer());
        objectMapper.registerModule(module);
    }

    @Override
    public void insertVertices(Map<String, String> serialisedVertices) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("insertVertices");

        try {
            if (serialisedVertices.size() > 1) {
                BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
                Instant updatedAt = Instant.ofEpochMilli(RequestContext.get().getRequestTime());

                for (Map.Entry<String, String> entry : serialisedVertices.entrySet()) {
                    BoundStatement insertStatement = insertVertexStatement.bind(entry.getKey(), entry.getValue(), updatedAt);
                    batchBuilder.addStatement(insertStatement);
                }

                BatchStatement batchStatement = batchBuilder.build();
                session.execute(batchStatement);
                return;
            }

            if (!serialisedVertices.isEmpty()) {
                Map.Entry<String, String> entry = serialisedVertices.entrySet().iterator().next();
                Instant updatedAt = Instant.ofEpochMilli(RequestContext.get().getRequestTime());
                BoundStatement insertStatement = insertVertexStatement.bind(entry.getKey(), entry.getValue(), updatedAt);
                session.execute(insertStatement);
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void dropVertices(List<String> vertexIds) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("dropVertices");
        try {
            BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
            for (String vertexId : vertexIds) {
                batchBuilder.addStatement(deleteVertexStatement.bind(vertexId));
            }
            BatchStatement batchStatement = batchBuilder.build();
            session.execute(batchStatement);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /**
     * Fetches vertices directly as DynamicVertex objects without intermediate JSON serialization/deserialization.
     * This is the most efficient method for retrieving vertices from the database.
     *
     * @param vertexIds List of vertex IDs to fetch
     * @return Map of vertex ID to DynamicVertex object
     */
    @Override
    public Map<String, DynamicVertex> fetchVerticesDirectly(List<String> vertexIds) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("fetchVerticesDirectly");
        if (vertexIds == null || vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Filter out blank IDs
        List<String> sanitizedIds = new ArrayList<>();
        for (String id : vertexIds) {
            if (StringUtils.isNotBlank(id)) {
                sanitizedIds.add(id);
            }
        }

        if (sanitizedIds.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            return fetchSingleBatchDirectly(sanitizedIds);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /**
     * Fetches a single batch of vertices directly as DynamicVertex objects using concurrent single-partition queries.
     * This avoids large IN clauses and aligns with Cassandra best practices.
     */
    private Map<String, DynamicVertex> fetchSingleBatchDirectly(List<String> vertexIdsInBatch) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder mainRecorder = RequestContext.get().startMetricRecord("fetchSingleBatchDirectly");
        Map<String, DynamicVertex> results = new HashMap<>();

        if (vertexIdsInBatch == null || vertexIdsInBatch.isEmpty()) {
            RequestContext.get().endMetricRecord(mainRecorder);
            return Collections.emptyMap();
        }

        Set<String> uniqueSanitizedVertexIdsInBatch = new LinkedHashSet<>();
        for (String vertexId : vertexIdsInBatch) {
            if (StringUtils.isNotBlank(vertexId)) {
                uniqueSanitizedVertexIdsInBatch.add(vertexId);
            }
        }

        if (uniqueSanitizedVertexIdsInBatch.isEmpty()) {
            RequestContext.get().endMetricRecord(mainRecorder);
            return Collections.emptyMap();
        }

        try {
            LOG.debug("Executing concurrent Cassandra calls for batch: IDs={}", uniqueSanitizedVertexIdsInBatch.size());

            List<CompletableFuture<List<Row>>> futures = new ArrayList<>(uniqueSanitizedVertexIdsInBatch.size());
            for (String vertexId : uniqueSanitizedVertexIdsInBatch) {
                BoundStatement boundStatement = selectByIdStatement.bind(vertexId)
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
                CompletableFuture<List<Row>> future = session.executeAsync(boundStatement)
                        .toCompletableFuture()
                        .thenCompose(this::fetchAllRows);
                futures.add(future);
            }

            CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            try {
                allOf.join();
            } catch (CompletionException e) {
                LOG.warn("One or more Cassandra queries failed in batch; continuing to process remaining results.", e);
            }

            // 3. Process results.
            for (CompletableFuture<List<Row>> future : futures) {
                List<Row> rows;
                try {
                    rows = future.join();
                } catch (CompletionException e) {
                    LOG.warn("Failed to fetch vertex data in concurrent batch.", e);
                    continue;
                }
                for (Row row : rows) {
                    String id = row.getString("id");
                    String jsonData = row.getString("json_data");
                    try {
                        AtlasPerfMetrics.MetricRecorder deserializeRecorder = RequestContext.get().startMetricRecord("fetchSingleBatchDirectly_DeserializeJson");
                        Map<String, Object> props = objectMapper.readValue(jsonData, Map.class);
                        RequestContext.get().endMetricRecord(deserializeRecorder);

                        DynamicVertex vertex = new DynamicVertex(props);
                        if (!vertex.hasProperty("id")) { // Ensure ID is present
                            vertex.setProperty("id", id);
                        }
                        results.put(id, vertex);
                    } catch (JsonProcessingException e) {
                        LOG.warn("Failed to parse JSON for DynamicVertex ID {}: {}", id, e.getMessage());
                    } catch (Exception e) { // Catch broader exceptions during DynamicVertex creation
                        LOG.warn("Failed to convert or process data for DynamicVertex ID {}: {}", id, e.getMessage());
                    }
                }
            }

            return results;

        } catch (QueryValidationException e) {
            LOG.error("Invalid query error during concurrent batch fetch strategy:  Error=\'{}\'.", e.getMessage(), e);
            throw new AtlasBaseException("Invalid query for concurrent batch fetch: " + e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Unexpected error during concurrent batch fetch strategy", e);
            throw new AtlasBaseException("Failed to fetch vertex data in concurrent batch strategy: " + e.getMessage(), e);
        } finally {
            RequestContext.get().endMetricRecord(mainRecorder);
        }
    }

    private CompletableFuture<List<Row>> fetchAllRows(AsyncResultSet resultSet) {
        List<Row> rows = new ArrayList<>();
        if (!resultSet.hasMorePages()) {
            return CompletableFuture.completedFuture(rows);
        }

        return resultSet.fetchNextPage()
                .toCompletableFuture()
                .thenCompose(next -> fetchAllRows(next)
                        .thenApply(nextRows -> {
                            rows.addAll(nextRows);
                            return rows;
                        }));
    }
}