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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.store.graph.v2.tags.CassandraTagConfig.*;

/**
 * Data Access Object for tag operations in Cassandra
 */
@Repository
public class TagDAOCassandraImpl implements TagDAO, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TagDAOCassandraImpl.class);

    // Configuration constants
    // Retry Configuration
    private static final int MAX_RETRIES = 3;
    private static final Duration INITIAL_BACKOFF = Duration.ofMillis(100);

    // Batch Configuration
    private static final int BATCH_SIZE_LIMIT = 100;

    // Configuration constants
    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(30);
    public static final String DEFAULT_HOST = "localhost";
    public static final String DATACENTER = "datacenter1";

    private final CqlSession cassSession;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Prepared Statements
    private final PreparedStatement findAllTagsStmt;
    private final PreparedStatement findAllTagsByVertexIdStmt;
    private final PreparedStatement findTagAttachmentsByPK;
    private final PreparedStatement findAllDirectTagsStmt;
    private final PreparedStatement findADirectTagStmt;
    private final PreparedStatement findADirectTagWithAssetMetadataRowStmt;
    private final PreparedStatement findAllPropagatedTagsStmt;
    private final PreparedStatement findAllPropagatedTagsByTypeNameStmt;
    private final PreparedStatement findAllPropagatedTagsOptStmt;
    private final PreparedStatement findADirectDeletedTagStmt;
    private final PreparedStatement insertTagStmt;
    private final PreparedStatement deleteTagStmt;


    public TagDAOCassandraImpl() throws AtlasBaseException {
        try {
            String hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, DEFAULT_HOST);
            Map<String, String> replicationConfig = Map.of("class", "SimpleStrategy", "replication_factor", ApplicationProperties.get().getString(CASSANDRA_REPLICATION_FACTOR_PROPERTY, "3"));

            DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                    // Connection timeouts
                    .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, CONNECTION_TIMEOUT)
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, CONNECTION_TIMEOUT)
                    .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, CONNECTION_TIMEOUT)

                    // Connection pool settings
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, calculateOptimalLocalPoolSize())
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, calculateOptimalRemotePoolSize())
                    .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL)
                    .build();

            cassSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(hostname, CASSANDRA_PORT))
                    .withConfigLoader(configLoader)
                    .withLocalDatacenter(DATACENTER)
                    .build();

            // Initialize keyspace and table
            initializeSchema(replicationConfig);

            // Find all direct tags
            SimpleStatement findAllDirectTagsStatement = SimpleStatement.builder(
                            String.format("SELECT tag_meta_json FROM %s.%s " +
                                            "WHERE bucket = ? AND id = ? AND source_id = ? AND is_deleted = false",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            findAllDirectTagsStmt = cassSession.prepare(findAllDirectTagsStatement);

            // Find all tags
            SimpleStatement findAllTagsStatement = SimpleStatement.builder(
                            String.format("SELECT id, source_id, tag_meta_json FROM %s.%s " +
                                            "WHERE bucket = ? AND id = ? AND is_deleted = false",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            findAllTagsStmt = cassSession.prepare(findAllTagsStatement);

            // Find all tags with additional is_propagated key
            SimpleStatement findTagsWithIsPropagatedByVertexIdStatement = SimpleStatement.builder(
                            String.format("SELECT id, source_id, tag_meta_json, is_propagated, tag_type_name FROM %s.%s " +
                                            "WHERE bucket = ? AND id = ? AND is_deleted = false",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            findAllTagsByVertexIdStmt = cassSession.prepare(findTagsWithIsPropagatedByVertexIdStatement);

            // Find a direct tag
            SimpleStatement findADirectTagStatement = SimpleStatement.builder(
                            String.format("SELECT tag_meta_json FROM %s.%s " +
                                            "WHERE bucket = ? AND id = ? AND source_id = ? AND tag_type_name = ? AND is_deleted = false",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            findADirectTagStmt = cassSession.prepare(findADirectTagStatement);

            // Find a deleted direct tag
            SimpleStatement findADirectDeletedTagStatement = SimpleStatement.builder(
                            String.format("SELECT tag_meta_json FROM %s.%s " +
                                            "WHERE bucket = ? AND id = ? AND source_id = ? AND tag_type_name = ? AND is_deleted = true",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            findADirectDeletedTagStmt = cassSession.prepare(findADirectDeletedTagStatement);

            // Find a direct tag with asset metadata
            SimpleStatement findADirectTagWithAssetMetadata = SimpleStatement.builder(
                            String.format("SELECT tag_meta_json, asset_metadata FROM %s.%s " +
                                            "WHERE bucket = ? AND id = ? AND source_id = ? AND tag_type_name = ? AND is_deleted = false",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            findADirectTagWithAssetMetadataRowStmt = cassSession.prepare(findADirectTagWithAssetMetadata);

            // Find all propagated tags
            SimpleStatement findAllPropagatedTagsStatement = SimpleStatement.builder(
                            String.format("SELECT id, source_id, tag_meta_json FROM %s.%s " +
                                            "WHERE bucket = ? AND id = ? AND is_propagated = true AND is_deleted = false ALLOW FILTERING",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            findAllPropagatedTagsStmt = cassSession.prepare(findAllPropagatedTagsStatement);

            // Find all propagated tags by type name
            SimpleStatement findAllPropagatedTagsByTypeNameStatement = SimpleStatement.builder(
                            String.format("SELECT bucket, id, source_id, tag_type_name, asset_metadata FROM %s.%s " +
                                            "WHERE source_id = ? AND tag_type_name = ? AND is_propagated = true AND is_deleted = false ALLOW FILTERING",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            findAllPropagatedTagsByTypeNameStmt = cassSession.prepare(findAllPropagatedTagsByTypeNameStatement);

            // Find all propagated tags optimized
            SimpleStatement findAllPropagatedTagsOptStatement = SimpleStatement.builder(
                            String.format("SELECT bucket, id, source_id, tag_type_name FROM %s.%s " +
                                            "WHERE source_id = ? AND tag_type_name = ? AND is_propagated = true AND is_deleted = false ALLOW FILTERING",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            findAllPropagatedTagsOptStmt = cassSession.prepare(findAllPropagatedTagsOptStatement);

            // Insert tag
            SimpleStatement insertTagStatement = SimpleStatement.builder(
                            String.format("INSERT INTO %s.%s " +
                                            "(bucket, id, source_id, tag_type_name, is_propagated, is_deleted, updated_at, asset_metadata, tag_meta_json) " +
                                            "VALUES (?, ?, ?, ?, ?, false, ?, ?, ?)",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            insertTagStmt = cassSession.prepare(insertTagStatement);

            // Soft delete tag
            SimpleStatement softDeleteTagStatement = SimpleStatement.builder(
                            String.format("UPDATE %s.%s " +
                                            "SET is_deleted = true, updated_at = ? " +
                                            "WHERE bucket = ? AND id = ? AND source_id = ? AND tag_type_name = ?",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();
            deleteTagStmt = cassSession.prepare(softDeleteTagStatement);

            // To Fetch a tag row with PK
            // Find all tags with additional is_propagated key
            SimpleStatement findTagByPK = SimpleStatement.builder(
                            String.format(
                                    "SELECT tag_meta_json FROM %s.%s " +
                                            "WHERE bucket = ? AND id = ? AND source_id = ? AND tag_type_name = ?",
                                    KEYSPACE, TABLE_NAME))
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                    .build();

            findTagAttachmentsByPK = cassSession.prepare(findTagByPK);

        } catch (Exception e) {
            LOG.error("Failed to initialize TagDAO", e);
            throw new AtlasBaseException("Failed to initialize TagDAO", e);
        }
    }

    // Helper method to calculate optimal local pool size
    private int calculateOptimalLocalPoolSize() {
        int cores = Runtime.getRuntime().availableProcessors();
        double targetUtilization = 0.75;
        int baseConnections = (int) Math.ceil(cores * targetUtilization);

        // Minimum of 4, maximum of 8 connections per host
        return Math.min(Math.max(baseConnections, 4), 8);
    }

    // Helper method to calculate optimal remote pool size
    private int calculateOptimalRemotePoolSize() {
        return Math.max(calculateOptimalLocalPoolSize() / 2, 2);
    }

    private void initializeSchema(Map<String, String> replicationConfig) throws AtlasBaseException {
        String replicationConfigString = replicationConfig.entrySet().stream()
                .map(entry -> String.format("'%s': '%s'", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(", "));

        // Create keyspace
        String createKeyspaceQuery = String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {%s} AND durable_writes = true;",
                KEYSPACE, replicationConfigString);
        SimpleStatement keyspaceStmt = SimpleStatement.builder(createKeyspaceQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                .build();

        executeWithRetry(keyspaceStmt);
        LOG.info("Ensured keyspace {} exists", KEYSPACE);

        // Create table if not exists
        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "id text, " +
                        "bucket int, " +
                        "property_name text, " +
                        "tag_type_name text, " +
                        "is_propagated boolean, " +
                        "source_id text, " +
                        "tag_meta_json text, " +
                        "asset_metadata text, " +
                        "updated_at timestamp, " +
                        "is_deleted boolean, " +
                        "PRIMARY KEY ((bucket), id, source_id, tag_type_name)" +
                        ") WITH compaction = {" +
                        "'class': 'SizeTieredCompactionStrategy', " +
                        "'min_threshold': 4, " +
                        "'max_threshold': 32" +
                        "};",
                KEYSPACE, TABLE_NAME);

        SimpleStatement tableStmt = SimpleStatement.builder(createTableQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                .build();

        executeWithRetry(tableStmt);
        LOG.info("Ensured table {}.{} exists", KEYSPACE, TABLE_NAME);

        // Create necessary indexes
        String createIsDeletedIndex = String.format(
                "CREATE INDEX IF NOT EXISTS idx_is_deleted ON %s.%s (is_deleted);",
                KEYSPACE, TABLE_NAME);
        executeWithRetry(SimpleStatement.builder(createIsDeletedIndex).build());
        LOG.info("Created index on is_deleted column");

        String createSourceIdIndex = String.format(
                "CREATE INDEX IF NOT EXISTS idx_source_id ON %s.%s (source_id);",
                KEYSPACE, TABLE_NAME);
        executeWithRetry(SimpleStatement.builder(createSourceIdIndex).build());

        String createTagTypeNameIndex = String.format(
                "CREATE INDEX IF NOT EXISTS idx_tag_type_name ON %s.%s (tag_type_name);",
                KEYSPACE, TABLE_NAME);
        executeWithRetry(SimpleStatement.builder(createTagTypeNameIndex).build());

        String createIsPropagatedIndex = String.format(
                "CREATE INDEX IF NOT EXISTS idx_is_propagated ON %s.%s (is_propagated);",
                KEYSPACE, TABLE_NAME);
        executeWithRetry(SimpleStatement.builder(createIsPropagatedIndex).build());
    }

    @Override
    public void putDirectTag(String assetId,
                             String tagTypeName,
                             AtlasClassification tag,
                             Map<String, Object> assetMetadata) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("putDirectTag");

        try {
            int bucket = calculateBucket(assetId);
            // Use prepared statement with bound values
            BoundStatement bound = insertTagStmt.bind()
                    .setInt(0, bucket)                                    // bucket
                    .setString(1, assetId)                               // id
                    .setString(2, assetId)                               // source_id
                    .setString(3, tagTypeName)                           // tag_type_name
                    .setBoolean(4, false)                                // is_propagated
                    // is_deleted is hardcoded as false in the query
                    .setInstant(5, Instant.ofEpochMilli(RequestContext.get().getRequestTime()))   // updated_at
                    .setString(6, AtlasType.toJson(assetMetadata))      // asset_metadata
                    .setString(7, AtlasType.toJson(tag));               // tag_meta_json


            executeWithRetry(bound);

        } catch (Exception e) {
            LOG.error("Error in putDirectTag for assetId: {}, tagTypeName: {}", assetId, tagTypeName, e);
            throw new AtlasBaseException("Error executing direct tag insert", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public List<AtlasClassification> getPropagationsForAttachment(String vertexId,
                                                                  String sourceEntityGuid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder =
                RequestContext.get().startMetricRecord("getPropagationsForAttachment");
        try {
            // Fetch all (direct + propagated) tags on this vertex
            List<AtlasClassification> allTags = getAllClassificationsForVertex(vertexId);

            List<AtlasClassification> matchingTags = allTags.stream()
                    .filter(tag -> sourceEntityGuid.equals(tag.getEntityGuid()))
                    .collect(Collectors.toList());

            if (matchingTags.isEmpty()) {
                LOG.info("No propagated tags found for vertexId: {}, sourceEntityGuid: {}", vertexId, sourceEntityGuid);
            }
            return matchingTags;
        } catch (AtlasBaseException abe) {
            throw abe;
        } catch (Exception e) {
            throw new AtlasBaseException(
                    String.format("Error fetching propagations for attachment: vertexId=%s, sourceEntityGuid=%s",
                            vertexId, sourceEntityGuid),
                    e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public List<AtlasClassification> getAllDirectTagsForVertex(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getAllDirectTagsForVertex");
        List<AtlasClassification> tags = new ArrayList<>();

        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findAllDirectTagsStmt.bind(bucket, vertexId, vertexId);

            ResultSet rs = executeWithRetry(bound);

            for (Row row : rs) {
                AtlasClassification classification = convertToAtlasClassification(row.getString("tag_meta_json"));
                tags.add(classification);
            }

            if (tags.isEmpty()) {
                LOG.warn("No direct tags found for vertexId: {}, bucket: {}", vertexId, bucket);
            }

        } catch (Exception e) {
            throw new AtlasBaseException("Error fetching direct tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return tags;
    }

    /**
     * Extracts or resolves an AtlasClassification for the given row, loading direct tags if needed.
     */
    private AtlasClassification extractClassification(Row row) throws AtlasBaseException {
        String sourceId = row.getString("source_id");
        String id = row.getString("id");

        if (sourceId == null || id == null) {
            throw new AtlasBaseException("id or sourceId not present in Row");
        }
        AtlasClassification classification = convertToAtlasClassification(row.getString("tag_meta_json"));
        if (!sourceId.equals(id)) {
            AtlasClassification atlasClassification = null;
            String typeName = classification.getTypeName();
            atlasClassification = getClassificationFromPK(sourceId, sourceId, typeName);
            if (atlasClassification != null) {
                classification = atlasClassification;
            }
        }
        return classification;
    }

    @Override
    public List<AtlasClassification> getAllClassificationsForVertex(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagsForAsset");
        List<AtlasClassification> tags = new ArrayList<>();
        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findAllTagsStmt.bind(bucket, vertexId);

            ResultSet rs = executeWithRetry(bound);
            for (Row row : rs) {
                AtlasClassification classification = extractClassification(row);
                tags.add(classification);
            }

            if (tags.isEmpty())
                LOG.warn("getAllClassificationsForVertex: No classifications found for vertexId={}", vertexId);

        } catch(Exception e){
            throw new AtlasBaseException("Error fetching tags", e);
        } finally{
            RequestContext.get().endMetricRecord(recorder);
        }
        return tags;
    }

    @Override
    public Tag getTagFromPK(String vertexId, String sourceId, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagsFromPK");
        List<Tag> tags = new ArrayList<>();

        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findTagAttachmentsByPK.bind(bucket, vertexId, sourceId, tagTypeName);

            ResultSet rs = executeWithRetry(bound);

            for (Row row : rs) {
                Tag tag = new Tag();
                tag.setVertexId(vertexId);
                tag.setTagTypeName(tagTypeName);
                tag.setTagMetaJson(objectMapper.readValue(row.getString("tag_meta_json"), Map.class));
                tags.add(tag);
            }

            if (tags.isEmpty()) {
                LOG.warn("getTagFromPK: No tag found for vertexId={}, sourceId={}, tagTypeName={}", vertexId, sourceId, tagTypeName);
                return null;
            }

        } catch (Exception e) {
            LOG.error("getTagFromPK: Error fetching tag for vertexId={}, sourceId={}, tagTypeName={}", vertexId, sourceId, tagTypeName, e);
            throw new AtlasBaseException("Error fetching tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return tags.get(0);
    }

    public AtlasClassification getClassificationFromPK(String vertexId, String sourceId, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagFromPK");
        List<AtlasClassification> classifications = new ArrayList<>();

        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findTagAttachmentsByPK.bind(bucket, vertexId, sourceId, tagTypeName);

            ResultSet rs = executeWithRetry(bound);
            for (Row row : rs) {
                AtlasClassification classification = convertToAtlasClassification(row.getString("tag_meta_json"));
                classifications.add(classification);
            }
        } catch (Exception e) {
            LOG.error("Error fetching classification for vertexId={}, sourceId={}, tagTypeName={}", vertexId, sourceId, tagTypeName, e);
            throw new AtlasBaseException("Error fetching tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        if (classifications.isEmpty()) {
            LOG.warn("No classification found for vertexId={}, sourceId={}, tagTypeName={}", vertexId, sourceId, tagTypeName);
        }

        return classifications.get(0);
    }

    @Override
    public List<AtlasClassification> findByVertexIdAndPropagated(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getPropagatedTagsForVertex");
        List<AtlasClassification> tags = new ArrayList<>();

        int bucket = calculateBucket(vertexId);
        try {
            BoundStatement bound = findAllPropagatedTagsStmt.bind(bucket, vertexId);
            ResultSet rs = executeWithRetry(bound);

            for (Row row : rs) {
                AtlasClassification classification = extractClassification(row);
                tags.add(classification);
            }
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tags for asset: %s, bucket: %s", vertexId, bucket), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        if (tags.isEmpty())
            LOG.warn("No propagated tags found for vertexId={}", vertexId);
        return tags;
    }

    @Override
    public AtlasClassification findDirectTagByVertexIdAndTagTypeName(String vertexId, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("findTagByVertexIdAndTagTypeName");
        int bucket = calculateBucket(vertexId);
        try {
            BoundStatement bound = findADirectTagStmt.bind(bucket, vertexId, vertexId, tagTypeName);
            ResultSet rs = executeWithRetry(bound);

            for (Row row : rs) {
                return convertToAtlasClassification(row.getString("tag_meta_json"));
            }
            LOG.warn("No direct tag found for vertexId={}, tagTypeName={}, bucket={}", vertexId, tagTypeName, bucket);
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tag for asset: %s and tag type: %s, bucket: %s", vertexId, tagTypeName, bucket), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return null;
    }

    @Override
    public AtlasClassification findDirectDeletedTagByVertexIdAndTagTypeName(String vertexId, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("findDirectDeletedTagByVertexIdAndTagTypeName");
        int bucket = calculateBucket(vertexId);
        try {
            BoundStatement bound = findADirectDeletedTagStmt.bind(bucket, vertexId, vertexId, tagTypeName);
            ResultSet rs = executeWithRetry(bound);

            for (Row row : rs) {
                return convertToAtlasClassification(row.getString("tag_meta_json"));
            }
            LOG.warn("No deleted tag found for vertexId={}, tagTypeName={}, bucket={}", vertexId, tagTypeName, bucket);
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tag for asset: %s and tag type: %s, bucket: %s", vertexId, tagTypeName, bucket), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return null;
    }

    @Override
    public Tag findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(String vertexId, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata");
        int bucket = calculateBucket(vertexId);
        try {
            BoundStatement bound = findADirectTagWithAssetMetadataRowStmt.bind(bucket, vertexId, vertexId, tagTypeName);
            ResultSet rs = executeWithRetry(bound);

            for (Row row : rs) {
                Tag tag = new Tag();
                tag.setVertexId(vertexId);
                tag.setTagTypeName(tagTypeName);
                tag.setTagMetaJson(objectMapper.readValue(row.getString("tag_meta_json"), Map.class));
                tag.setAssetMetadata(objectMapper.readValue(row.getString("asset_metadata"), Map.class));
                return tag;
            }
            LOG.warn("No direct tag with asset metadata found for vertexId={}, tagTypeName={}, bucket={}", vertexId, tagTypeName, bucket);
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tag for asset: %s and tag type: %s, bucket: %s", vertexId, tagTypeName, bucket), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return null;
    }

    @Override
    public List<Tag> getAllTagsByVertexId(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagsWithIsPropagatedByVertexId");
        int bucket = calculateBucket(vertexId);
        try {
            BoundStatement bound = findAllTagsByVertexIdStmt.bind(bucket, vertexId);
            ResultSet rs = executeWithRetry(bound);

            List<Tag> results = new ArrayList<>();
            int rowCount = 0;

            for (Row row : rs) {
                rowCount++;
                Tag tag = new Tag();
                tag.setVertexId(vertexId);
                tag.setPropagated(row.getBoolean("is_propagated"));
                String sourceId = row.getString("source_id");
                String typeName = row.getString("tag_type_name");
                Tag sourceTag = getTagFromPK(sourceId, sourceId, typeName);
                tag.setTagMetaJson(sourceTag.getTagMetaJson());
                results.add(tag);
            }

            if (rowCount == 0) {
                LOG.warn("No tag rows found for vertexId={}, bucket={}", vertexId, bucket);
            }

            return results;
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tag for id: %s and bucket: %s", vertexId, bucket), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public PaginatedTagResult getPropagationsForAttachmentBatch(String sourceVertexId, String tagTypeName) throws AtlasBaseException {
        // Track all pages using a static variable per tag type and source
        String cacheKey = sourceVertexId + "|" + tagTypeName;
        String storedPagingState = PagingStateCache.getState(cacheKey);

        PaginatedTagResult result = getPropagationsForAttachmentBatchWithPagination(sourceVertexId, tagTypeName, storedPagingState, 100, cacheKey);

        // Store paging state for next call
        PagingStateCache.setState(cacheKey, result.getPagingState());

        // Return empty list when we've reached the end
        if (result.getTags().isEmpty() && result.getPagingState() == null) {
            LOG.info("No more tags found for source_id: {}, tagTypeName: {}", sourceVertexId, tagTypeName);
        }

        return result;
    }

    @Override
    public PaginatedTagResult getPropagationsForAttachmentBatchWithPagination(String sourceVertexId, String tagTypeName,
                                                                              String pagingStateStr, int pageSize, String cacheKey) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getVertexIdsForAttachment");
        List<Tag> tags = new ArrayList<>();
        String nextPagingState = null;
        Boolean done = false;
        try {
            BoundStatement bound = findAllPropagatedTagsByTypeNameStmt.bind(sourceVertexId, tagTypeName).setPageSize(pageSize);

            // Apply the paging state if provided
            if (pagingStateStr != null && !pagingStateStr.isEmpty()) {
                try {
                    // For Driver 4.13.0, convert the Base64 string back to ByteBuffer
                    byte[] decoded = Base64.getDecoder().decode(pagingStateStr);
                    ByteBuffer pagingStateBuffer = ByteBuffer.wrap(decoded);
                    bound = bound.setPagingState(pagingStateBuffer);
                } catch (Exception e) {
                    LOG.warn("Failed to decode paging state string", e);
                    // Continue without paging state on error
                }
            }

            ResultSet rs = cassSession.execute(bound);

            // Process only up to pageSize rows
            int count = 0;
            for (Row row : rs) {
                Tag tag = new Tag();
                tag.setBucket(row.getInt("bucket"));
                tag.setVertexId(row.getString("id"));
                tag.setTagTypeName(row.getString("tag_type_name"));
                tag.setSourceVertexId(row.getString("source_id"));
                tag.setAssetMetadata(objectMapper.readValue(row.getString("asset_metadata"), Map.class));
                tags.add(tag);

                count++;
                if (count >= pageSize) {
                    break;
                }
            }

            // Only set paging state if we got a full page of results
            if (count >= pageSize && !rs.isFullyFetched()) {
                try {
                    // Get the paging state from the result set
                    ByteBuffer pagingStateBuffer = rs.getExecutionInfo().getPagingState();
                    if (pagingStateBuffer != null) {
                        // Make a copy of the buffer to avoid position issues
                        ByteBuffer copy = ByteBuffer.allocate(pagingStateBuffer.remaining());
                        copy.put(pagingStateBuffer.duplicate());
                        copy.flip();

                        // Encode as Base64 string for safe transport
                        byte[] bytes = new byte[copy.remaining()];
                        copy.get(bytes);
                        nextPagingState = Base64.getEncoder().encodeToString(bytes);
                    }
                } catch (Exception e) {
                    LOG.warn("Could not process paging state, pagination may not work correctly", e);
                    nextPagingState = null;
                    PagingStateCache.setState(cacheKey, nextPagingState);
                }
            } else {
                // We've reached the end - explicitly set nextPagingState to null
                nextPagingState = null;
                PagingStateCache.setState(cacheKey, nextPagingState);
                done = true;
                LOG.info("Reached end of results for source_id: {}, tagTypeName: {}, fetched {} rows",
                        sourceVertexId, tagTypeName, count);
            }

            if (tags.isEmpty()) {
                LOG.info("No propagated tags found for source_id: {}, tagTypeName: {}", sourceVertexId, tagTypeName);
            }
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tags for source_id: %s and tag type: %s",
                    sourceVertexId, tagTypeName), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return new PaginatedTagResult(tags, nextPagingState, done);
    }

    @Override
    public List<Tag> getTagPropagationsForAttachment(String sourceVertexId, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getVertexIdsForAttachment");
        List<Tag> tags = new ArrayList<>();

        try {
            BoundStatement bound = findAllPropagatedTagsOptStmt.bind(sourceVertexId, tagTypeName);

            ResultSet rs = executeWithRetry(bound);

            for (Row row : rs) {
                Tag tag = new Tag();
                tag.setBucket(row.getInt("bucket"));
                tag.setVertexId(row.getString("id"));
                tag.setTagTypeName(row.getString("tag_type_name"));
                tag.setSourceVertexId(row.getString("source_id"));
                tags.add(tag);
            }

            if (tags.isEmpty()) {
                LOG.info("No propagated assets found for source_id: {}, tagTypeName: {}", sourceVertexId, tagTypeName);
            }
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tags for source_id: %s and tag type: %s",
                                       sourceVertexId, tagTypeName), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return tags;
    }

    @Override
    public void deleteDirectTag(String sourceVertexId, AtlasClassification tagToDelete) throws AtlasBaseException {
        // Do not delete row, mark is_active as false
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("deleteTags");

        try {
            BoundStatement bound = deleteTagStmt.bind()
                    .setInstant(0, Instant.ofEpochMilli(RequestContext.get().getRequestTime()))   // updated_at
                    .setInt(1, calculateBucket(sourceVertexId))          // bucket
                    .setString(2, sourceVertexId)                        // id
                    .setString(3, sourceVertexId)                        // source_id
                    .setString(4, tagToDelete.getTypeName());           // tag_type_name

            executeWithRetry(bound);
        } catch (Exception e) {
            throw new AtlasBaseException("Error deleting tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void deleteTags(List<Tag> tagsToDelete) throws AtlasBaseException {
        // Do not delete rows, mark is_active as false

        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("deleteTags");
        try {
            // Create initial batch
            BatchStatement batch = BatchStatement.builder(DefaultBatchType.LOGGED)
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                    .build();

            for (Tag tagToDelete : tagsToDelete) {
                // Create bound statement for each delete
                BoundStatement bound = deleteTagStmt.bind()
                        .setInstant(0, Instant.ofEpochMilli(RequestContext.get().getRequestTime()))   // updated_at
                        .setInt(1, tagToDelete.getBucket())            // bucket
                        .setString(2, tagToDelete.getVertexId())       // id
                        .setString(3, tagToDelete.getSourceVertexId()) // source_id
                        .setString(4, tagToDelete.getTagTypeName());   // tag_type_name

                // Add to batch
                batch = batch.add(bound);

                // If batch size limit reached, execute it
                if (batch.size() >= BATCH_SIZE_LIMIT) {
                    cassSession.execute(batch);
                    // Create new batch for next statements
                    batch = BatchStatement.builder(DefaultBatchType.LOGGED)
                            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                            .build();
                }
            }

            // Execute any remaining statements in the final batch
            if (batch.size() > 0) {
                cassSession.execute(batch);
            }

        } catch (Exception e) {
            throw new AtlasBaseException("Error deleting tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void putPropagatedTags(String sourceAssetId,
                                  String tagTypeName,
                                  Set<String> propagatedAssetVertexIds,
                                  Map<String, Map<String, Object>> assetMinAttrsMap,
                                  AtlasClassification tag) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("putPropagatedTags");

        try {
            List<String> vertexIds = new ArrayList<>(propagatedAssetVertexIds);
            for (int i = 0; i < vertexIds.size(); i += BATCH_SIZE_LIMIT) {
                // Create new batch for each group
                BatchStatement batch = BatchStatement.builder(DefaultBatchType.LOGGED)
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                        .build();

                // Process up to BATCH_SIZE_LIMIT vertices
                int endIndex = Math.min(i + BATCH_SIZE_LIMIT, vertexIds.size());
                for (int j = i; j < endIndex; j++) {
                    String propagatedAssetVertexId = vertexIds.get(j);
                    int bucket = calculateBucket(propagatedAssetVertexId);

                    // Create bound statement for insert
                    BoundStatement bound = insertTagStmt.bind()
                            .setInt(0, bucket)
                            .setString(1, propagatedAssetVertexId)
                            .setString(2, sourceAssetId)
                            .setString(3, tagTypeName)
                            .setBoolean(4, true)  // is_propagated
                            .setInstant(5, Instant.ofEpochMilli(RequestContext.get().getRequestTime()))
                            .setString(6, AtlasType.toJson(assetMinAttrsMap.get(propagatedAssetVertexId)))
                            .setString(7, AtlasType.toJson(tag));

                    batch = batch.add(bound);
                }

                // Execute batch with retry logic
                executeWithRetry(batch);
            }

        } catch (Exception e) {
            LOG.error("Error in putPropagatedTags for sourceAssetId: {}, tagTypeName: {}",
                    sourceAssetId, tagTypeName, e);
            throw new AtlasBaseException("Error executing batch operation", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }


    public static AtlasClassification convertToAtlasClassification(String tagMetaJson) throws AtlasBaseException {
        if (tagMetaJson == null) {
            throw new AtlasBaseException("Tag metadata JSON cannot be null");
        }
        try {
            Map tagMetaJsonMap = objectMapper.readValue(tagMetaJson, Map.class);
            return getAtlasClassification(tagMetaJsonMap);
        } catch (JsonProcessingException e) {
            LOG.error("Error converting to AtlasClassification. JSON: {}",
                    tagMetaJson, e);
            throw new AtlasBaseException("Unable to map to AtlasClassification", e);
        }
    }

    public static AtlasClassification toAtlasClassification(Map<String, Object> tagMetaJsonMap) throws AtlasBaseException {
        return getAtlasClassification(tagMetaJsonMap);
    }

    private static AtlasClassification getAtlasClassification(Map<String, Object> tagMetaJsonMap) throws AtlasBaseException {
        AtlasClassification classification = new AtlasClassification();
        classification.setTypeName((String) tagMetaJsonMap.get("typeName"));
        classification.setEntityGuid((String) tagMetaJsonMap.get("entityGuid"));
        classification.setPropagate((Boolean) tagMetaJsonMap.get("propagate"));
        classification.setRemovePropagationsOnEntityDelete((Boolean) tagMetaJsonMap.get("removePropagationsOnEntityDelete"));
        classification.setRestrictPropagationThroughLineage((Boolean) tagMetaJsonMap.get("restrictPropagationThroughLineage"));
        classification.setRestrictPropagationThroughHierarchy((Boolean) tagMetaJsonMap.get("restrictPropagationThroughHierarchy"));

        Map<String, Object> originalAttributes = (Map<String, Object>) tagMetaJsonMap.get("attributes");
        if (originalAttributes != null) {
            classification.setAttributes(deepCopyMap(originalAttributes));
        }
        return classification;
    }

    private static Map<String, Object> deepCopyMap(Map<String, Object> original) throws AtlasBaseException {
        try {
            String json = objectMapper.writeValueAsString(original);
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            throw new AtlasBaseException("Error during deep copy of map", e);
        }
    }

    private <T extends Statement<T>> ResultSet executeWithRetry(Statement<T> statement) throws AtlasBaseException {
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount < MAX_RETRIES) {
            try {
                return cassSession.execute(statement);
            } catch (DriverTimeoutException | WriteTimeoutException | NoHostAvailableException e) {
                lastException = e;
                retryCount++;
                if (retryCount == MAX_RETRIES) {
                    break;
                }
                try {
                    Thread.sleep(INITIAL_BACKOFF.multipliedBy(retryCount).toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new AtlasBaseException("Interrupted during retry backoff", ie);
                }
            }
        }
        LOG.error("Failed to execute statement after {} retries", MAX_RETRIES, lastException);
        throw new AtlasBaseException("Failed to execute statement after retries", lastException);
    }

    public static int calculateBucket(String vertexId) {
        int numBuckets = 2 << BUCKET_POWER; // 2^5=32
        return (int) (Long.parseLong(vertexId) % numBuckets);
    }

    @Override
    public void close() throws Exception {
        if (cassSession != null) {
            try {
                if (!cassSession.isClosed()) {
                    cassSession.close();
                }
            } catch (Exception e) {
                LOG.error("Error closing Cassandra session", e);
            }
        }
    }

    private static class PagingStateCache {
        private static final Map<String, String> pagingStates = new java.util.HashMap<>();

        public static String getState(String key) {
            return pagingStates.get(key);
        }

        public static void setState(String key, String state) {
            if (state == null) {
                pagingStates.remove(key);
            } else {
                pagingStates.put(key, state);
            }
        }
    }
}