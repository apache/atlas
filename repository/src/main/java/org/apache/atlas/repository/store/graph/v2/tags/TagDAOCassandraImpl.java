package org.apache.atlas.repository.store.graph.v2.tags;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.store.graph.v2.CassandraConnector.CASSANDRA_HOSTNAME_PROPERTY;

/**
 * Data Access Object for tag operations in Cassandra
 */
@Repository
public class TagDAOCassandraImpl implements TagDAO {
    private static final Logger LOG = LoggerFactory.getLogger(TagDAOCassandraImpl.class);
    private static int BUCKET_POWER = 5;
    private static String KEYSPACE = null;
    private static final String CASSANDRA_BUCKET_POWER = "atlas.graph.new.bucket.power";
    public static final String CASSANDRA_NEW_KEYSPACE_PROPERTY = "atlas.graph.new.keyspace";
    private final CqlSession cassSession;
    private final PreparedStatement findDirectTagsStmt;
    private final PreparedStatement findTagsStmt;
    private final PreparedStatement findDirectTagsByVertexIdAndTypeNameStmt;
    private final PreparedStatement findPropagatedTagsBySourceAndTypeNameStmt;
    private final PreparedStatement deletePropagatedTag;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static String INSERT_PROPAGATED_TAG = "INSERT into tags.effective_tags (bucket, id, tag_type_name, source_id, is_propagated, updated_at, asset_metadata) values (%s, '%s', '%s', '%s', %s, %s, '%s')";
    private static String INSERT_DIRECT_TAG = "INSERT into tags.effective_tags (bucket, id, tag_type_name, source_id, is_propagated, updated_at, asset_metadata, tag_meta_json) values (%s, '%s', '%s', '%s', %s, %s,'%s', '%s')";

    private static String DELETE_DIRECT_TAG = "DELETE FROM tags.effective_tags where bucket = %s AND id = '%s' AND source_id = '%s' AND tag_type_name = '%s' AND is_propagated = false";

    private static String DELETE_PROPAGATED_TAG = "DELETE FROM tags.effective_tags where bucket = %s AND id = '%s' AND source_id = '%s' AND tag_type_name = '%s'";


    public TagDAOCassandraImpl() throws AtlasBaseException {
        try {
            KEYSPACE = ApplicationProperties.get().getString(CASSANDRA_NEW_KEYSPACE_PROPERTY, "tags");
            BUCKET_POWER = ApplicationProperties.get().getInt(CASSANDRA_BUCKET_POWER, 6);

            // Initialize Cassandra connection
            String hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");
            cassSession = initializeCassandraSession(hostname);

            // Prepare statements for reuse
            findDirectTagsStmt = cassSession.prepare(
                    "SELECT * FROM tags.effective_tags WHERE id = ? AND bucket = ? AND source_id = ? AND is_propagated = false"
            );

            findTagsStmt = cassSession.prepare(
                    "SELECT * FROM tags.effective_tags WHERE id = ? AND bucket = ?"
            );

            findDirectTagsByVertexIdAndTypeNameStmt = cassSession.prepare(
                    "SELECT * FROM tags.effective_tags where bucket = ? AND id = ? AND tag_type_name = ? AND source_id = ? AND is_propagated = false"
            );

            findPropagatedTagsBySourceAndTypeNameStmt = cassSession.prepare(
                    "SELECT * FROM tags.effective_tags where source_id = ? AND tag_type_name = ? AND is_propagated = true ALLOW FILTERING"
            );

            deletePropagatedTag = cassSession.prepare(
                    "DELETE FROM tags.effective_tags where bucket = ? AND id = ? AND source_id = ? AND tag_type_name = ?"
            );

        } catch (Exception e) {
            LOG.error("Failed to initialize TagDAO", e);
            throw new AtlasBaseException("Failed to initialize TagDAO", e);
        }
    }

    @Override
    public List<AtlasClassification> getAllDirectTagsForVertex(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getAllDirectTagsForVertex");
        List<AtlasClassification> tags = new ArrayList<>();

        int bucket = calculateBucket(vertexId);
        try {
            BoundStatement bound = findDirectTagsStmt.bind(vertexId, bucket, vertexId);
            ResultSet rs = cassSession.execute(bound);

            for (Row row : rs) {
                AtlasClassification classification = convertToAtlasClassification(row.getString("tag_meta_json"));
                tags.add(classification);
            }
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tags for asset: %s, bucket: %s", vertexId, bucket), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return tags;
    }

    @Override
    public List<AtlasClassification> getTagsForVertex(String vertexId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagsForAsset");
        List<AtlasClassification> tags = new ArrayList<>();

        int bucket = calculateBucket(vertexId);
        try {
            BoundStatement bound = findTagsStmt.bind(vertexId, bucket);
            ResultSet rs = cassSession.execute(bound);

            for (Row row : rs) {
                AtlasClassification classification;
                boolean isPropagated = row.getBoolean("is_propagated");
                if (isPropagated) {
                    String assetMetadata = row.getString("asset_metadata");
                    Map jsonMap = objectMapper.readValue(assetMetadata, Map.class);

                    classification = new AtlasClassification();
                    classification.setTypeName(row.getString("tag_type_name"));
                    classification.setEntityGuid((String) jsonMap.get("__guid"));
                } else {
                    classification = convertToAtlasClassification(row.getString("tag_meta_json"));
                }

                tags.add(classification);
            }
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tags for asset: %s, bucket: %s", vertexId, bucket), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return tags;
    }

    @Override
    public AtlasClassification findDirectTagByVertexIdAndTagTypeName(String vertexId, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("findTagByVertexIdAndTagTypeName");
        int bucket = calculateBucket(vertexId);
        try {
            BoundStatement bound = findDirectTagsByVertexIdAndTypeNameStmt.bind(bucket, vertexId, tagTypeName, vertexId);
            ResultSet rs = cassSession.execute(bound);

            for (Row row : rs) {
                AtlasClassification classification = convertToAtlasClassification(row.getString("tag_meta_json"));
                return classification;
            }
            LOG.info("No tags found for id: {}, tag type: {}, bucket {}, returning null", vertexId, tagTypeName, bucket);
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tag for asset: %s and tag type: %s, bucket: %s", vertexId, tagTypeName, bucket), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return null;
    }

    @Override
    public List<Tag> getPropagationsForAttachmentBatch(String sourceVertexId, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getVertexIdsForAttachment");
        List<Tag> tags = new ArrayList<>();

        try {
            BoundStatement bound = findPropagatedTagsBySourceAndTypeNameStmt.bind(sourceVertexId, tagTypeName).setPageSize(100);
            ResultSet rs = cassSession.execute(bound);

            for (Row row : rs) {
                Tag tag = new Tag();

                tag.setBucket(row.getInt("bucket"));
                tag.setVertexId(row.getString("id"));
                tag.setTagTypeName(row.getString("tag_type_name"));
                tag.setSourceVertexId(row.getString("source_id"));
                tag.setAssetMetadata(objectMapper.readValue(row.getString("asset_metadata"), Map.class));

                tags.add(tag);
            }
            LOG.info("No propagated tags found for source_id: {}, tagTypeName: {}, returning null", sourceVertexId, tagTypeName);
        } catch (Exception e) {
            throw new AtlasBaseException(String.format("Error fetching tags found for source_id: %s and tag type: %s", sourceVertexId, tagTypeName), e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return tags;
    }

    @Override
    public void deleteDirectTag(String sourceVertexId, AtlasClassification tagToDelete) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("deleteTags");

        try {
            String delete = String.format(DELETE_DIRECT_TAG,
                    calculateBucket(sourceVertexId),
                    sourceVertexId,
                    sourceVertexId,
                    tagToDelete.getTypeName());

            cassSession.execute(delete);

        } catch (Exception e) {
            throw new AtlasBaseException("Error deleting tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void deleteTags(List<Tag> tagsToDelete) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("deleteTags");
        //BatchStatement batchStatement = BatchStatement.newInstance(LOGGED);
        StringBuilder batchQuery = new StringBuilder();
        batchQuery.append("BEGIN BATCH ");

        try {
            for (Tag tagToDelete: tagsToDelete) {
                String delete = String.format(DELETE_PROPAGATED_TAG,
                        tagToDelete.getBucket(),
                        tagToDelete.getVertexId(),
                        tagToDelete.getSourceVertexId(),
                        tagToDelete.getTagTypeName());
                batchQuery.append(delete).append(";");
            }

            batchQuery.append("APPLY BATCH;");
            cassSession.execute(batchQuery.toString());

        } catch (Exception e) {
            throw new AtlasBaseException("Error deleting tags", e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void putPropagatedTags(String sourceAssetId, String tagTypeName,
                                  Set<String> propagatedAssetVertexIds,
                                  Map<String, Map<String, Object>> assetMinAttrsMap) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("putPropagatedTags");
        StringBuilder batchQuery = new StringBuilder();
        batchQuery.append("BEGIN BATCH ");

        for (String propagatedAssetVertexId : propagatedAssetVertexIds) {
            int bucket = calculateBucket(propagatedAssetVertexId);
            String insert = String.format(INSERT_PROPAGATED_TAG, bucket, propagatedAssetVertexId, tagTypeName, sourceAssetId, true,
                    RequestContext.get().getRequestTime(),
                    AtlasType.toJson(assetMinAttrsMap.get(propagatedAssetVertexId)));
            batchQuery.append(insert).append(";");
        }

        batchQuery.append("APPLY BATCH;");
        cassSession.execute(batchQuery.toString());

        RequestContext.get().endMetricRecord(recorder);
    }

    @Override
    public void putDirectTag(String assetId,
                             String tagTypeName,
                             AtlasClassification tag,
                             Map<String, Object> assetMetadata) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("putDirectTag");

        int bucket = calculateBucket(assetId);

        String insert = String.format(INSERT_DIRECT_TAG, bucket, assetId, tagTypeName, assetId, false,
                RequestContext.get().getRequestTime(),
                AtlasType.toJson(assetMetadata),
                AtlasType.toJson(tag));

        cassSession.execute(insert);

        RequestContext.get().endMetricRecord(recorder);
    }

    private AtlasClassification convertToAtlasClassification(String tagMetaJson) throws AtlasBaseException {
        try {
            Map jsonMap = objectMapper.readValue(tagMetaJson, Map.class);

            AtlasClassification classification = new AtlasClassification();
            classification.setTypeName((String) jsonMap.get("typeName"));
            classification.setEntityGuid((String) jsonMap.get("entityGuid"));
            classification.setPropagate((Boolean) jsonMap.get("propagate"));
            classification.setRemovePropagationsOnEntityDelete((Boolean) jsonMap.get("removePropagations"));
            classification.setRestrictPropagationThroughLineage((Boolean) jsonMap.get("restrictPropagationThroughLineage"));
            classification.setRestrictPropagationThroughHierarchy((Boolean) jsonMap.get("restrictPropagationThroughHierarchy"));
            return classification;
        } catch (JsonProcessingException e) {
            LOG.error("Error converting to AtlasClassification. JSON: {}",
                    tagMetaJson, e);
            throw new AtlasBaseException("Unable to map to AtlasClassification", e);
        }
    }

    private CqlSession initializeCassandraSession(String hostname) {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, 9042))
                .withConfigLoader(
                        DriverConfigLoader.programmaticBuilder()
                                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
                                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(15))
                                // Control timeout for requests
                                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(20))
                                // More specific timeouts for different query types
                                .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofMillis(500))
                                .withDuration(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS, Duration.ofSeconds(20))
                                .build())
                .withLocalDatacenter("datacenter1")
                .withKeyspace(KEYSPACE)
                .build();
    }

    private int calculateBucket(String vertexId) {
        int numBuckets = 2 << BUCKET_POWER; // 2^5=32
        return (int) (Long.parseLong(vertexId) % numBuckets);
    }
}