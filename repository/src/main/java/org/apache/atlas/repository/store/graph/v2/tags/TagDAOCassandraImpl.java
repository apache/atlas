package org.apache.atlas.repository.store.graph.v2.tags;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.Tag;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasConfiguration.CLASSIFICATION_PROPAGATION_DEFAULT;
import static org.apache.atlas.repository.store.graph.v2.tags.CassandraTagConfig.*;

/**
 * Data Access Object for tag operations in Cassandra.

 * This implementation uses a two-table design to optimize for different query patterns
 * and avoid the use of 'ALLOW FILTERING', ensuring scalability.

 * 1.  tags_by_id: Stores all tags (direct and propagated). Optimized for finding
 * all tags for a given asset. Uses soft deletes.
 * -   PK: ((bucket, id), is_propagated, source_id, tag_type_name)
 * -   Compaction: SizeTieredCompactionStrategy (STCS)

 * 2.  propagated_tags_by_source: A query-optimized table for finding all assets that
 * have a specific propagated tag. This table is optimized for reads and uses hard deletes.
 * -   PK: ((source_id, tag_type_name), propagated_asset_id)
 * -   Compaction: LeveledCompactionStrategy (LCS)
 */
public class TagDAOCassandraImpl implements TagDAO, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TagDAOCassandraImpl.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Configuration constants
    private static final int MAX_RETRIES = 3;
    private static final Duration INITIAL_BACKOFF = Duration.ofMillis(100);
    private static final int BATCH_SIZE_LIMIT = 200;
    private static final int BATCH_SIZE_LIMIT_FOR_DELETION = 10000;
    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(5);
    //private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(30);
    public static final String DEFAULT_HOST = "localhost";
    public static final String DATACENTER = "datacenter1";

    private static final TagDAOCassandraImpl INSTANCE;

    /**
     * Static initializer block to create the singleton instance.
     * This handles potential exceptions during initialization.
     * Also, this will not block requests initializing or waiting due to lazy initialization
     */
    static {
        try {
            INSTANCE = new TagDAOCassandraImpl();
        } catch (AtlasBaseException e) {
            LOG.error("FATAL: Failed to initialize TagDAOCassandraImpl singleton", e);
            throw new RuntimeException("Could not initialize TagDAO Cassandra implementation: TagDAOCassandraImpl", e);
        }
    }

    /**
     * Provides the global point of access to the TagDAOCassandraImpl instance.
     * @return The single instance of TagDAOCassandraImpl.
     */
    public static TagDAOCassandraImpl getInstance() {
        return INSTANCE;
    }

    private final CqlSession cassSession;

    // Prepared Statements for 'tags_by_id' table
    private final PreparedStatement findAllTagsForAssetStmt;
    private final PreparedStatement findDirectClassificationsForAssetStmt;
    private final PreparedStatement findDirectTagsForAssetStmt;
    private final PreparedStatement findPropagatedTagsForAssetStmt;
    private final PreparedStatement findSpecificDirectTagStmt;
    private final PreparedStatement findSpecificDeletedTagStmt;
    private final PreparedStatement insertEffectiveTagStmt;
    private final PreparedStatement deleteEffectiveTagStmt;
    private final PreparedStatement findAllTagDetailsForAssetStmt;

    // Prepared Statements for new 'propagated_tags_by_source' table
    private final PreparedStatement findPropagationsBySourceStmt;
    private final PreparedStatement insertPropagationBySourceStmt;
    private final PreparedStatement deletePropagationStmt;

    // Health check prepared statement
    private final PreparedStatement healthCheckStmt;


    private TagDAOCassandraImpl() throws AtlasBaseException {
        try {
            String hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, DEFAULT_HOST);
            Map<String, String> replicationConfig = Map.of("class", "SimpleStrategy", "replication_factor", ApplicationProperties.get().getString(CASSANDRA_REPLICATION_FACTOR_PROPERTY, "3"));

            DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                    //.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, REQUEST_TIMEOUT)
                    .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, CONNECTION_TIMEOUT)
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, CONNECTION_TIMEOUT)
                    .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, CONNECTION_TIMEOUT)
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, calculateOptimalLocalPoolSize())
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, calculateOptimalRemotePoolSize())
                    .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL)
                    .build();

            cassSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(hostname, CASSANDRA_PORT))
                    .withConfigLoader(configLoader)
                    .withLocalDatacenter(DATACENTER)
                    .build();

            initializeSchema(replicationConfig);

            // === Statements for 'tags_by_id' table ===
            insertEffectiveTagStmt = prepare(String.format(
                    "INSERT INTO %s.%s (bucket, id, is_propagated, source_id, tag_type_name, tag_meta_json, asset_metadata, is_deleted, updated_at) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, false, ?)", KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));

            deleteEffectiveTagStmt = prepare(String.format(
                    "UPDATE %s.%s SET is_deleted = true, updated_at = ? WHERE bucket = ? AND id = ? AND is_propagated = ? AND source_id = ? AND tag_type_name = ?",
                    KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));

            findAllTagsForAssetStmt = prepare(String.format(
                    "SELECT tag_meta_json, is_deleted FROM %s.%s WHERE bucket = ? AND id = ?", KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));

            findAllTagDetailsForAssetStmt = prepare(String.format(
                    "SELECT tag_meta_json, source_id, is_propagated, tag_type_name, is_deleted FROM %s.%s WHERE bucket = ? AND id = ?", KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));

            findDirectClassificationsForAssetStmt = prepare(String.format(
                    "SELECT tag_meta_json, is_deleted FROM %s.%s WHERE bucket = ? AND id = ? AND is_propagated = false", KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));

            findDirectTagsForAssetStmt = prepare(String.format(
                    "SELECT is_propagated, source_id, tag_type_name, tag_meta_json, is_deleted FROM %s.%s WHERE bucket = ? AND id = ? AND is_propagated = false", KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));

            findPropagatedTagsForAssetStmt = prepare(String.format(
                    "SELECT tag_meta_json, source_id, is_deleted FROM %s.%s WHERE bucket = ? AND id = ? AND is_propagated = true", KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));

            findSpecificDirectTagStmt = prepare(String.format(
                    "SELECT tag_meta_json, asset_metadata, is_deleted FROM %s.%s WHERE bucket = ? AND id = ? AND is_propagated = false AND source_id = ? AND tag_type_name = ?",
                    KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));

            findSpecificDeletedTagStmt = prepare(String.format(
                    "SELECT tag_meta_json, is_deleted FROM %s.%s WHERE bucket = ? AND id = ? AND is_propagated = false AND source_id = ? AND tag_type_name = ?",
                    KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));

            // === Statements for 'propagated_tags_by_source' table (using HARD DELETES) ===
            findPropagationsBySourceStmt = prepare(String.format(
                    "SELECT propagated_asset_id, asset_metadata FROM %s.%s WHERE source_id = ? AND tag_type_name = ?", KEYSPACE, PROPAGATED_TAGS_TABLE_NAME));

            insertPropagationBySourceStmt = prepare(String.format(
                    "INSERT INTO %s.%s (source_id, tag_type_name, propagated_asset_id, asset_metadata, updated_at) VALUES (?, ?, ?, ?, ?)",
                    KEYSPACE, PROPAGATED_TAGS_TABLE_NAME));

            deletePropagationStmt = prepare(String.format(
                    "DELETE FROM %s.%s WHERE source_id = ? AND tag_type_name = ? AND propagated_asset_id = ?", KEYSPACE, PROPAGATED_TAGS_TABLE_NAME));

            // === Health check statement ===
            healthCheckStmt = prepare("SELECT release_version FROM system.local");

        } catch (Exception e) {
            LOG.error("Failed to initialize TagDAO", e);
            throw new AtlasBaseException("Failed to initialize TagDAO", e);
        }
    }

    private PreparedStatement prepare(String cql) {
        return cassSession.prepare(SimpleStatement.builder(cql)
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .build());
    }

    private void initializeSchema(Map<String, String> replicationConfig) throws AtlasBaseException {
        String replicationConfigString = replicationConfig.entrySet().stream()
                .map(entry -> String.format("'%s': '%s'", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(", "));

        String createKeyspaceQuery = String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {%s} AND durable_writes = true;",
                KEYSPACE, replicationConfigString);
        executeWithRetry(SimpleStatement.builder(createKeyspaceQuery).setConsistencyLevel(DefaultConsistencyLevel.ALL).build());
        LOG.info("Ensured keyspace {} exists", KEYSPACE);

        String createEffectiveTagsTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "id text, " +
                        "bucket int, " +
                        "is_propagated boolean, " +
                        "source_id text, " +
                        "tag_type_name text, " +
                        "tag_meta_json text, " +
                        "asset_metadata text, " +
                        "updated_at timestamp, " +
                        "is_deleted boolean, " +
                        "PRIMARY KEY ((bucket, id), is_propagated, source_id, tag_type_name)" +
                        ") WITH compaction = {'class': 'SizeTieredCompactionStrategy'};",
                KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME);
        executeWithRetry(SimpleStatement.builder(createEffectiveTagsTable).setConsistencyLevel(DefaultConsistencyLevel.ALL).build());
        LOG.info("Ensured table {}.{} exists with SizeTieredCompactionStrategy", KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME);

        String createPropagatedTagsTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "source_id text, " +
                        "tag_type_name text, " +
                        "propagated_asset_id text, " +
                        "asset_metadata text, " +
                        "updated_at timestamp, " +
                        "PRIMARY KEY ((source_id, tag_type_name), propagated_asset_id)" +
                        ") WITH compaction = {'class': 'SizeTieredCompactionStrategy'};",
                KEYSPACE, PROPAGATED_TAGS_TABLE_NAME);
        executeWithRetry(SimpleStatement.builder(createPropagatedTagsTable).setConsistencyLevel(DefaultConsistencyLevel.ALL).build());
        LOG.info("Ensured table {}.{} exists with SizeTieredCompactionStrategy and hard deletes", KEYSPACE, PROPAGATED_TAGS_TABLE_NAME);
    }

    @Override
    public void putDirectTag(String assetId, String tagTypeName, AtlasClassification tag, Map<String, Object> assetMetadata) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("putDirectTag");
        try {
            int bucket = calculateBucket(assetId);
            BoundStatement bound = insertEffectiveTagStmt.bind()
                    .setInt("bucket", bucket)
                    .setString("id", assetId)
                    .setBoolean("is_propagated", false)
                    .setString("source_id", assetId) // For direct tags, source_id is the asset itself
                    .setString("tag_type_name", tagTypeName)
                    .setString("tag_meta_json", AtlasType.toJson(tag))
                    .setString("asset_metadata", AtlasType.toJson(assetMetadata))
                    .setInstant("updated_at", Instant.ofEpochMilli(RequestContext.get().getRequestTime()));

            executeWithRetry(bound);
        } catch (Exception e) {
            LOG.error("Error in putDirectTag for assetId: {}, tagTypeName: {}", assetId, tagTypeName, e);
            throw new AtlasBaseException("Error executing direct tag insert", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void putPropagatedTags(String sourceAssetId, String tagTypeName, Set<String> propagatedAssetVertexIds,
                                  Map<String, Map<String, Object>> assetMinAttrsMap, AtlasClassification tag) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("putPropagatedTags");
        try {
            List<String> vertexIds = new ArrayList<>(propagatedAssetVertexIds);
            Instant now = Instant.ofEpochMilli(RequestContext.get().getRequestTime());
            String tagJson = AtlasType.toJson(tag);

            for (int i = 0; i < vertexIds.size(); i += BATCH_SIZE_LIMIT) {
                // Use a LOGGED batch to ensure atomicity across the two tables
                BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED)
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

                int endIndex = Math.min(i + BATCH_SIZE_LIMIT, vertexIds.size());
                for (int j = i; j < endIndex; j++) {
                    String propagatedAssetId = vertexIds.get(j);
                    int bucket = calculateBucket(propagatedAssetId);
                    String assetMetadataJson = AtlasType.toJson(assetMinAttrsMap.get(propagatedAssetId));

                    // 1. Insert into tags_by_id
                    batchBuilder.addStatement(insertEffectiveTagStmt.bind()
                            .setInt("bucket", bucket)
                            .setString("id", propagatedAssetId)
                            .setBoolean("is_propagated", true)
                            .setString("source_id", sourceAssetId)
                            .setString("tag_type_name", tagTypeName)
                            .setString("tag_meta_json", tagJson)
                            .setString("asset_metadata", assetMetadataJson)
                            .setInstant("updated_at", now));

                    // 2. Insert into propagated_tags_by_source
                    batchBuilder.addStatement(insertPropagationBySourceStmt.bind()
                            .setString("source_id", sourceAssetId)
                            .setString("tag_type_name", tagTypeName)
                            .setString("propagated_asset_id", propagatedAssetId)
                            .setString("asset_metadata", assetMetadataJson)
                            .setInstant("updated_at", now));
                }
                executeWithRetry(batchBuilder.build());
            }
        } catch (Exception e) {
            LOG.error("Error in putPropagatedTags for sourceAssetId: {}, tagTypeName: {}", sourceAssetId, tagTypeName, e);
            throw new AtlasBaseException("Error executing batch propagation insert", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public List<AtlasClassification> getAllDirectClassificationsForVertex(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getAllDirectClassificationsForVertex");
        List<AtlasClassification> classifications = new ArrayList<>();
        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findDirectClassificationsForAssetStmt.bind(bucket, vertexId);
            ResultSet rs = executeWithRetry(bound);
            for (Row row : rs) {
                if (!row.getBoolean("is_deleted")) {
                    classifications.add(convertToAtlasClassification(row.getString("tag_meta_json")));
                }
            }
            if (classifications.isEmpty()) {
                LOG.warn("No active direct tags found for vertexId={}, bucket={}", vertexId, bucket);
            }
            return classifications;
        } catch (Exception e) {
            LOG.error("Error fetching direct classifications for vertexId={}", vertexId, e);
            throw new AtlasBaseException("Error fetching direct tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }


    @Override
    public List<AtlasClassification> getAllClassificationsForVertex(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getAllClassificationsForVertex");
        List<AtlasClassification> finalTags = new ArrayList<>();
        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findAllTagsForAssetStmt.bind(bucket, vertexId);
            ResultSet rs = executeWithRetry(bound);
            for (Row row : rs) {
                if (!row.getBoolean("is_deleted")) {
                    finalTags.add(convertToAtlasClassification(row.getString("tag_meta_json")));
                }
            }
            return finalTags;
        } catch (Exception e) {
            LOG.error("Error fetching all classifications for vertexId={}", vertexId, e);
            throw new AtlasBaseException("Error fetching all classifications", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public PaginatedTagResult getPropagationsForAttachmentBatchWithPagination(String sourceVertexId, String tagTypeName,
                                                                              String pagingStateStr, int pageSize) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getPropagationsForAttachmentBatchWithPagination");
        try {
            BoundStatement bound = findPropagationsBySourceStmt.bind(sourceVertexId, tagTypeName).setPageSize(pageSize);

            if (pagingStateStr != null && !pagingStateStr.isEmpty()) {
                bound = bound.setPagingState(ByteBuffer.wrap(Base64.getDecoder().decode(pagingStateStr)));
            }

            ResultSet rs = executeWithRetry(bound);
            List<Tag> tags = new ArrayList<>(pageSize);

            Iterator<Row> iterator = rs.iterator();
            int count = 0;

            while (count < pageSize && iterator.hasNext()) {
                Row row = iterator.next();
                Tag tag = new Tag();
                tag.setVertexId(row.getString("propagated_asset_id"));
                tag.setSourceVertexId(sourceVertexId);
                tag.setTagTypeName(tagTypeName);
                try {
                    tag.setAssetMetadata(objectMapper.readValue(row.getString("asset_metadata"), Map.class));
                } catch (JsonProcessingException e) {
                    LOG.error("Error parsing asset_metadata for propagated tag on sourceVertexId={}, tagTypeName={}", sourceVertexId, tagTypeName, e);
                }
                tags.add(tag);
                count++;
            }

            LOG.debug("Fetched {} propagations in this page for sourceVertexId={}, tagTypeName={}", tags.size(), sourceVertexId, tagTypeName);

            ByteBuffer pagingStateBuffer = rs.getExecutionInfo().getPagingState();
            String nextPagingState = null;

            if (pagingStateBuffer != null) {
                byte[] bytes = new byte[pagingStateBuffer.remaining()];
                pagingStateBuffer.get(bytes);
                if (bytes.length > 0) {
                    nextPagingState = Base64.getEncoder().encodeToString(bytes);
                }
            }

            boolean done = (nextPagingState == null || nextPagingState.isEmpty());
            if (tags.isEmpty() && done) {
                LOG.warn("No propagations found for sourceVertexId={}, tagTypeName={}", sourceVertexId, tagTypeName);
            }
            LOG.debug("Next paging state for sourceVertexId={}, tagTypeName={}. Has more pages: {}",
                    sourceVertexId, tagTypeName, !done);

            return new PaginatedTagResult(tags, nextPagingState, done);
        } catch (Exception e) {
            LOG.error("Error fetching paginated propagations for sourceVertexId={}, tagTypeName={}", sourceVertexId, tagTypeName, e);
            throw new AtlasBaseException("Error fetching paginated propagations", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void deleteDirectTag(String sourceVertexId, AtlasClassification tagToDelete) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("deleteDirectTag");
        String tagTypeName = tagToDelete.getTypeName();
        try {
            int bucket = calculateBucket(sourceVertexId);
            BoundStatement bound = deleteEffectiveTagStmt.bind()
                    .setInstant("updated_at", Instant.ofEpochMilli(RequestContext.get().getRequestTime()))
                    .setInt("bucket", bucket)
                    .setString("id", sourceVertexId)
                    .setBoolean("is_propagated", false)
                    .setString("source_id", sourceVertexId)
                    .setString("tag_type_name", tagTypeName);

            executeWithRetry(bound);
        } catch (Exception e) {
            LOG.error("Error deleting direct tag for sourceVertexId={}, tagTypeName={}", sourceVertexId, tagTypeName, e);
            throw new AtlasBaseException("Error deleting direct tag", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void deleteTags(List<Tag> tagsToDelete) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(tagsToDelete)) {
            return;
        }
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("deleteTags");
        try {
            Instant now = Instant.ofEpochMilli(RequestContext.get().getRequestTime());
            List<BatchableStatement<?>> statements = new ArrayList<>();

            for (Tag tag : tagsToDelete) {
                int bucket = calculateBucket(tag.getVertexId());
                boolean isPropagated = !Objects.equals(tag.getSourceVertexId(), tag.getVertexId());

                if (tag.isPropagated() != isPropagated) {
                    LOG.warn("Discrepancy in is_propagated flag for tag delete operation. " +
                                    "Tag object passed by caller had is_propagated={}, but derived value is {}. " +
                                    "Proceeding with derived value. vertexId={}, sourceVertexId={}",
                            tag.isPropagated(), isPropagated, tag.getVertexId(), tag.getSourceVertexId());
                }

                // 1. Soft delete from tags_by_id
                statements.add(deleteEffectiveTagStmt.bind()
                        .setInstant("updated_at", now)
                        .setInt("bucket", bucket)
                        .setString("id", tag.getVertexId())
                        .setBoolean("is_propagated", isPropagated)
                        .setString("source_id", tag.getSourceVertexId())
                        .setString("tag_type_name", tag.getTagTypeName()));

                // 2. If it's a propagated tag, HARD delete from the lookup table
                if (isPropagated) {
                    statements.add(deletePropagationStmt.bind()
                            .setString("source_id", tag.getSourceVertexId())
                            .setString("tag_type_name", tag.getTagTypeName())
                            .setString("propagated_asset_id", tag.getVertexId()));
                }
            }

            for (int i = 0; i < statements.size(); i += BATCH_SIZE_LIMIT_FOR_DELETION) {
                int end = Math.min(i + BATCH_SIZE_LIMIT_FOR_DELETION, statements.size());
                List<BatchableStatement<?>> batchStatements = statements.subList(i, end);

                BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED)
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
                batchBuilder.addStatements(batchStatements);

                executeWithRetry(batchBuilder.build());
            }
        } catch (Exception e) {
            LOG.error("deleteTags=Failed to delete tags batch", e);
            throw new AtlasBaseException("Error deleting tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public AtlasClassification findDirectTagByVertexIdAndTagTypeName(String assetVertexId, String tagTypeName, boolean includeDeleted) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("findDirectTagByVertexIdAndTagTypeName");
        try {
            Tag tag = findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(assetVertexId, tagTypeName, includeDeleted);
            return tag != null ? toAtlasClassification(tag.getTagMetaJson()) : null;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public Tag findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(String vertexId, String tagTypeName, boolean includeDeleted) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata");
        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findSpecificDirectTagStmt.bind(bucket, vertexId, vertexId, tagTypeName);
            ResultSet rs = executeWithRetry(bound);
            Row row = rs.one();

            if (row == null || (!includeDeleted && row.getBoolean("is_deleted"))) {
                LOG.warn("No active direct tag found for vertexId={}, tagTypeName={}, bucket={}", vertexId, tagTypeName, bucket);
                return null;
            }

            Tag tag = new Tag();
            tag.setVertexId(vertexId);
            tag.setTagTypeName(tagTypeName);
            try {
                tag.setTagMetaJson(objectMapper.readValue(row.getString("tag_meta_json"), Map.class));
                tag.setAssetMetadata(objectMapper.readValue(row.getString("asset_metadata"), Map.class));
            } catch (JsonProcessingException e) {
                throw new AtlasBaseException("Failed to parse tag JSON for vertexId=" + vertexId, e);
            }
            return tag;
        } catch (Exception e) {
            LOG.error("Error finding direct tag for vertexId={}, tagTypeName={}", vertexId, tagTypeName, e);
            throw new AtlasBaseException("Error finding direct tag", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public AtlasClassification findDirectDeletedTagByVertexIdAndTagTypeName(String vertexId, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("findDirectDeletedTagByVertexIdAndTagTypeName");
        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findSpecificDeletedTagStmt.bind(bucket, vertexId, vertexId, tagTypeName);
            ResultSet rs = executeWithRetry(bound);
            Row row = rs.one();

            if (row != null && row.getBoolean("is_deleted")) {
                return convertToAtlasClassification(row.getString("tag_meta_json"));
            }
            LOG.warn("No deleted direct tag found for vertexId={}, tagTypeName={}, bucket={}", vertexId, tagTypeName, bucket);
            return null;
        } catch (Exception e) {
            LOG.error("Error finding deleted direct tag for vertexId={}, tagTypeName={}", vertexId, tagTypeName, e);
            throw new AtlasBaseException("Error finding deleted direct tag", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public PaginatedTagResult getPropagationsForAttachmentBatch(String sourceVertexId, String tagTypeName, String storedPagingState) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getPropagationsForAttachmentBatch");
        try {
            // Default page size of 100
            return getPropagationsForAttachmentBatchWithPagination(sourceVertexId, tagTypeName, storedPagingState, BATCH_SIZE_LIMIT_FOR_DELETION);
        } catch (Exception e) {
            LOG.error("Error getting propagations for attachment batch for sourceVertexId={}, tagTypeName={}", sourceVertexId, tagTypeName, e);
            throw new AtlasBaseException("Error getting propagations for attachment batch", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }


    @Override
    public List<AtlasClassification> findByVertexIdAndPropagated(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("findByVertexIdAndPropagated");
        List<AtlasClassification> tags = new ArrayList<>();
        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findPropagatedTagsForAssetStmt.bind(bucket, vertexId);
            ResultSet rs = executeWithRetry(bound);
            for (Row row : rs) {
                if (!row.getBoolean("is_deleted")) {
                    tags.add(convertToAtlasClassification(row.getString("tag_meta_json")));
                }
            }
            if (tags.isEmpty()) {
                LOG.warn("No propagated tags found for vertexId={}, bucket={}", vertexId, bucket);
            }
            return tags;
        } catch (Exception e) {
            LOG.error("Error fetching propagated tags for vertexId={}", vertexId, e);
            throw new AtlasBaseException("Error fetching propagated tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public List<AtlasClassification> getPropagationsForAttachment(String vertexId, String sourceEntityGuid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getPropagationsForAttachment");
        try {
            List<AtlasClassification> allTags = getAllClassificationsForVertex(vertexId);
            List<AtlasClassification> filteredTags = allTags.stream()
                    .filter(tag -> sourceEntityGuid.equals(tag.getEntityGuid()))
                    .collect(Collectors.toList());
            if (filteredTags.isEmpty()) {
                LOG.warn("No propagated tags found for vertexId={} with sourceEntityGuid={}", vertexId, sourceEntityGuid);
            }
            return filteredTags;
        } catch (Exception e) {
            LOG.error("Error in getPropagationsForAttachment for vertexId={}, sourceEntityGuid={}", vertexId, sourceEntityGuid, e);
            throw new AtlasBaseException("Error getting propagations for attachment", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public List<Tag> getAllTagsByVertexId(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getAllTagsByVertexId");
        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findAllTagDetailsForAssetStmt.bind(bucket, vertexId);
            ResultSet rs = executeWithRetry(bound);

            return resultSetToTags(vertexId, rs);
        } catch (Exception e) {
            LOG.error("Error in getAllTagsByVertexId for vertexId={}", vertexId, e);
            throw new AtlasBaseException("Error getting all tags by vertexId", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private static List<Tag> resultSetToTags(String vertexId, ResultSet rs) {
        List<Tag> tags = new ArrayList<>();
        for (Row row : rs) {
            if (row.getBoolean("is_deleted")) {
                continue;
            }

            Tag tag = new Tag();
            tag.setVertexId(vertexId);
            tag.setTagTypeName(row.getString("tag_type_name"));
            tag.setPropagated(row.getBoolean("is_propagated"));
            tag.setSourceVertexId(row.getString("source_id"));

            try {
                tag.setTagMetaJson(objectMapper.readValue(row.getString("tag_meta_json"), new TypeReference<>() {
                }));
            } catch (JsonProcessingException e) {
                LOG.error("Error parsing tag_meta_json in getAllTagsByVertexId for vertexId: {}", vertexId, e);
                continue;
            }
            tags.add(tag);
        }
        return tags;
    }

    private <T extends Statement<T>> ResultSet executeWithRetry(Statement<T> statement) throws AtlasBaseException {
        int retryCount = 0;
        Exception lastException;

        while (true) {
            try {
                return cassSession.execute(statement);
            } catch (DriverTimeoutException | WriteTimeoutException | NoHostAvailableException e) {
                lastException = e;
                retryCount++;
                LOG.warn("Retry attempt {} for statement execution due to exception: {}", retryCount, e.toString());
                if (retryCount >= MAX_RETRIES) {
                    break;
                }
                try {
                    long backoff = INITIAL_BACKOFF.toMillis() * (long)Math.pow(2, retryCount - 1);
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new AtlasBaseException("Interrupted during retry backoff", ie);
                }
            }
        }
        LOG.error("Failed to execute statement after {} retries", MAX_RETRIES, lastException);
        throw new AtlasBaseException("Failed to execute statement after " + MAX_RETRIES + " retries", lastException);
    }

    private int calculateOptimalLocalPoolSize() {
        return Math.min(Math.max((int) Math.ceil(Runtime.getRuntime().availableProcessors() * 0.75), 4), 8);
    }

    private int calculateOptimalRemotePoolSize() {
        return Math.max(calculateOptimalLocalPoolSize() / 2, 2);
    }

    public static int calculateBucket(String vertexId) {
        int numBuckets = 2 << BUCKET_POWER; // 2 * 2^5 = 64
        return (int) (Long.parseLong(vertexId) % numBuckets);
    }

    public static AtlasClassification convertToAtlasClassification(String tagMetaJson) throws AtlasBaseException {
        try {
            AtlasClassification classification = objectMapper.readValue(tagMetaJson, AtlasClassification.class);
            // Set default value is tagMetadataJson fields are null
            if (classification.getRestrictPropagationThroughLineage() == null) {
                classification.setRestrictPropagationThroughLineage(false);
            }
            if (classification.getRestrictPropagationThroughHierarchy() == null) {
                classification.setRestrictPropagationThroughHierarchy(false);
            }
            if (classification.getRemovePropagationsOnEntityDelete() == null) {
                classification.setRemovePropagationsOnEntityDelete(true);
            }
            if (classification.getPropagate() == null) {
                classification.setPropagate(CLASSIFICATION_PROPAGATION_DEFAULT.getBoolean());
            }
            return classification;
        } catch (JsonProcessingException e) {
            throw new AtlasBaseException("Unable to map to AtlasClassification", e);
        }
    }

    public static AtlasClassification toAtlasClassification(Map<String, Object> tagMetaJsonMap) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("dao.toAtlasClassification");
        try {
            return objectMapper.convertValue(tagMetaJsonMap, AtlasClassification.class);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /**
     * Performs a lightweight health check against Cassandra to verify connectivity and availability.
     * This method executes a simple query against the system.local table which is always available.
     * 
     * @return true if Cassandra is healthy and responsive, false otherwise
     */
    public boolean isHealthy() {
        try {
            Instant start = Instant.now();
            
            // Execute a lightweight query against system.local
            ResultSet rs = cassSession.execute(healthCheckStmt.bind());
            
            // Verify we get at least one row back
            boolean hasResults = rs.iterator().hasNext();
            
            Duration duration = Duration.between(start, Instant.now());
            
            if (hasResults) {
                LOG.debug("Cassandra health check successful in {}ms", duration.toMillis());
                return true;
            } else {
                LOG.warn("Cassandra health check failed - no results returned from system.local");
                return false;
            }
            
        } catch (DriverTimeoutException e) {
            LOG.warn("Cassandra health check failed due to timeout: {}", e.getMessage());
            return false;
        } catch (NoHostAvailableException e) {
            LOG.warn("Cassandra health check failed - no hosts available: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            LOG.warn("Cassandra health check failed due to unexpected error: {}", e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void close() {
        if (cassSession != null && !cassSession.isClosed()) {
            cassSession.close();
        }
    }
}

