package org.apache.atlas.repository.graphdb.janus.cassandra;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Enhanced Cassandra implementation for vertex data repository with advanced
 * features like connection pooling, retry mechanisms, and better error handling.
 */
class CassandraVertexDataRepository implements VertexDataRepository {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraVertexDataRepository.class);

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
            if (serialisedVertices == null || serialisedVertices.isEmpty()) {
                return;
            }

            BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
            Instant updatedAt = Instant.ofEpochMilli(RequestContext.get().getRequestTime());
            for (Map.Entry<String, String> entry : serialisedVertices.entrySet()) {
                BoundStatement boundStatement = insertVertexStatement.bind(entry.getKey(), entry.getValue(), updatedAt);
                batchBuilder.addStatement(boundStatement);
            }

            BatchStatement batchStatement = batchBuilder.build();
            session.execute(batchStatement);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void dropVertices(List<String> vertexIds) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("dropVertices");
        try {
            if (vertexIds == null || vertexIds.isEmpty()) {
                return;
            }

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

    @Override
    public CompletableFuture<DynamicVertex> fetchVertexAsync(String vertexId) {
        if (StringUtils.isBlank(vertexId)) {
            return CompletableFuture.completedFuture(null);
        }

        BoundStatement boundStatement = selectByIdStatement.bind(vertexId)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        return session.executeAsync(boundStatement)
                .toCompletableFuture()
                .thenApply(this::fetchSingleRow)
                .thenApply(row -> row == null ? null : buildDynamicVertex(row.getString("id"), row.getString("json_data")))
                .exceptionally(e -> {
                    LOG.warn("Failed to fetch DynamicVertex asynchronously for id {}", vertexId, e);
                    return null;
                });
    }

    private Row fetchSingleRow(AsyncResultSet resultSet) {
        for (Row row : resultSet.currentPage()) {
            return row;
        }
        return null;
    }

    private DynamicVertex buildDynamicVertex(String id, String jsonData) {
        try {
            AtlasPerfMetrics.MetricRecorder deserializeRecorder = RequestContext.get().startMetricRecord("fetchSingleBatchDirectly_DeserializeJson");
            Map<String, Object> props = objectMapper.readValue(jsonData, Map.class);
            RequestContext.get().endMetricRecord(deserializeRecorder);

            DynamicVertex vertex = new DynamicVertex(props);
            vertex.setProperty("_atlas_id", id);

            return vertex;
        } catch (JsonProcessingException e) {
            LOG.warn("Failed to parse JSON for DynamicVertex ID {}: {}", id, e.getMessage());
        } catch (Exception e) {
            LOG.warn("Failed to convert or process data for DynamicVertex ID {}: {}", id, e.getMessage());
        }
        return null;
    }
}
