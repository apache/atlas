package org.apache.atlas.repository.store.graph.v2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.AtlasConstants.DEFAULT_CLUSTER_NAME;

public class CassandraConnector {
    private static final Logger LOG      = LoggerFactory.getLogger(EntityGraphMapper.class);

    public static final String CASSANDRA_NEW_KEYSPACE_PROPERTY = "atlas.graph.new.keyspace";
    public static final String CASSANDRA_NEW_KEYSPACE_VERTEX_TABLE_NAME_PROPERTY = "atlas.graph.new.keyspace.vertex.table.name";
    public static final String CASSANDRA_HOSTNAME_PROPERTY = "atlas.graph.storage.hostname";
    public static final String CASSANDRA_CLUSTERNAME_PROPERTY = "atlas.graph.storage.clustername";

    private static CqlSession cassSession;
    private static String keyspace;
    private static String vertexTableName;

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static String SELECT_BY_ID = "SELECT * FROM %s where id = '%s' AND bucket = %s";
    private static String UPDATE_BY_ID = "UPDATE %s SET json_data = '%s' WHERE id = '%s' AND bucket = %s";

    //private static String SELECT_DIRECT_TAG = "SELECT * FROM tags_direct where bucket = %s AND id = '%s' AND tag_type_name = '%s' AND source_id = '%s'";
    private static String SELECT_DIRECT_TAG = "SELECT * FROM tag_direct_attachments where bucket = %s AND id = '%s' AND tag_type_name = '%s'";
    //private static String SELECT_ALL_TAGS_FOR_ASSET = "SELECT * FROM tags where bucket = %s AND id = '%s'";
    //private static String SELECT_PROPAGATED_TAGS = "SELECT * FROM propagated_tags where bucket = %s AND id = '%s' AND tag_type_name = '%s'";

    private static String INSERT_PROPAGATED_TAG = "INSERT into tags.effective_tags (bucket, id, tag_type_name, source_id) values (%s, '%s', '%s', '%s')";

    static {
        try {
            keyspace = ApplicationProperties.get().getString(CASSANDRA_NEW_KEYSPACE_PROPERTY, "tags");
            vertexTableName = ApplicationProperties.get().getString(CASSANDRA_NEW_KEYSPACE_VERTEX_TABLE_NAME_PROPERTY, "vertices");
            String hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");
            String clusterName = ApplicationProperties.get().getString(CASSANDRA_CLUSTERNAME_PROPERTY, DEFAULT_CLUSTER_NAME);
            int port = 9042;

            LOG.info("Using keyspace: {}", keyspace);
            LOG.info("Using vertexTableName: {}", vertexTableName);

            cassSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(hostname, 9042))
                    .withConfigLoader(
                            DriverConfigLoader.programmaticBuilder()
                                    .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
                                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(15))
                                    // Control timeout for requests
                                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                                    // Control timeout for schema agreement
                                    .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(20))
                                    // More specific timeouts for different query types
                                    .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofMillis(500))
                                    .withDuration(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS, Duration.ofSeconds(20))
                                    .build())
                    .withLocalDatacenter("datacenter1")
                    .withKeyspace(keyspace)
                    .build();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Integer> getIdsToPropagate() {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getIdsToPropagate");
        Map<String, Integer> ret;

        try {
            ret = new HashMap<>();
            String query = "SELECT * FROM vertices_id";

            ResultSet resultSet = cassSession.execute(query);
            for (Row row : resultSet) {
                String id = row.getString("id");
                int bucket = row.getInt("bucket");
                ret.put(id, bucket);
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return ret;
    }

    public static int calculateBucket(String value) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("calculateBucket");
        try {
            return Hashing.murmur3_32()
                    .hashString(value, StandardCharsets.UTF_8)
                    .asInt() & 0x7fffffff % 156;

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static Map<String, Object> getVertexProperties(String vertexId, int bucket) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getVertexPropertiesWithBucket");
        try {
            String query = String.format(SELECT_BY_ID, vertexTableName, vertexId, bucket);
            ResultSet resultSet = cassSession.execute(query);

            for (Row row : resultSet) {
                //System.out.println("Vertex: " + AtlasType.toJson(row));
                return convertRowToMap(vertexId, bucket, row);
            }
            LOG.info("Returning null for vertex {}", vertexId);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return null;
    }

    public static Map<String, Object> getTag(String assetVertexId, String tagTypeName) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTag");
        try {
            int bucket = calculateBucket(assetVertexId);
            String query = String.format(SELECT_DIRECT_TAG, bucket, assetVertexId, tagTypeName, assetVertexId);
            ResultSet resultSet = cassSession.execute(query);

            for (Row row : resultSet) {
                return convertRowToMap(assetVertexId, bucket, row);
            }
            LOG.info("Returning null for tag {}", assetVertexId);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return null;
    }

    public static Map<String, Object> getVertexProperties(String vertexId) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getVertexProperties");
        try {

            int bucket = calculateBucket(vertexId);
            String query = String.format(SELECT_BY_ID, vertexTableName, vertexId, bucket);
            ResultSet resultSet = cassSession.execute(query);

            Map<String, Object> ret = new HashMap<>();
            for (Row row : resultSet) {
                System.out.println("Vertex: " + AtlasType.toJson(row));
                return convertRowToMap(vertexId, bucket, row);
            }
            LOG.info("Returning null for vertex {}", vertexId);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return null;
    }

    public static Map<String, Object> convertRowToMap(String vertexId, int bucket, Row row) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("convertRowToMap");
        Map<String, Object> map = new HashMap<>();
        row.getColumnDefinitions().forEach(column -> {
            String columnName = column.getName().toString();
            Object columnValue = row.getObject(columnName);

            if (columnName.equals("json_data") || columnName.equals("tag_meta_json")) {
                Map<String, Object> interimValue;
                try {
                    interimValue = objectMapper.readValue(columnValue.toString(), new TypeReference<Map<String, Object>>() {});
                } catch (JsonProcessingException e) {
                    LOG.error("Failed to parse {} for record where id='{}' AND bucket={}", columnName, vertexId, bucket);
                    throw new RuntimeException(e);
                }

                for (String attribute : interimValue.keySet()) {
                    map.put(attribute, interimValue.get(attribute));
                }
            } else {
                map.put(columnName, columnValue);
            }
        });

        RequestContext.get().endMetricRecord(recorder);
        return map;
    }

    public static Map<String, Object> tempPutTagMap(String tagVertexId, String typeName, String entityGuid) throws JsonProcessingException {

        String json_data = "{\"id\":"+ tagVertexId +",\"__typeName\":\""+ typeName +"\",\"__modifiedBy\":\"service-account-atlan-argo\",\"__state\":\"ACTIVE\",\"__propagate\":true,\"__restrictPropagationThroughLineage\":false,\"__removePropagations\":true,\"__restrictPropagationThroughHierarchy\":false,\"__entityGuid\":\" " +entityGuid+ "\",\"__createdBy\":\"service-account-atlan-argo\",\"__modificationTimestamp\":1743060425553,\"__entityStatus\":\"ACTIVE\",\"__timestamp\":1743060425553}";

        String insert = "INSERT INTO "+vertexTableName+" (id, name, created_at, json_data) VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStmt = cassSession.prepare(insert);
        BoundStatement boundStmt  = preparedStmt.bind( tagVertexId, "placeholder", System.currentTimeMillis(), json_data);
        cassSession.execute(boundStmt);

        return objectMapper.readValue(json_data, new TypeReference<Map<String, Object>>() {});
    }

    /*public static void updateEntity(Map<String, Object> entityMap) {

        String query = "UPDATE vertices SET json_data = " + AtlasType.toJson(entityMap) + " WHERE id = " +  entityMap.get("id");
        cassSession.execute(query);

    }*/

    public static void putEntities(Collection<Map<String, Object>> entitiesMap) {
        StringBuilder batchQuery = new StringBuilder();
        batchQuery.append("BEGIN BATCH ");

        for (Map entry : entitiesMap) {
            String update = "UPDATE "+vertexTableName+" SET json_data = '" + AtlasType.toJson(entry) + "' WHERE id = '" +  entry.get("id") + "'";
            batchQuery.append(update).append(";");
        }

        batchQuery.append("APPLY BATCH;");
        cassSession.execute(batchQuery.toString());
    }

    public static void putEntitiesWithBucket(Collection<Map<String, Object>> entitiesMap) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("putEntitiesWithBucket");
        StringBuilder batchQuery = new StringBuilder();
        batchQuery.append("BEGIN BATCH ");

        for (Map entry : entitiesMap) {
            String update = String.format(UPDATE_BY_ID, vertexTableName, AtlasType.toJson(entry), entry.get("id"), entry.get("bucket"));
            batchQuery.append(update).append(";");
        }

        batchQuery.append("APPLY BATCH;");
        cassSession.execute(batchQuery.toString());

        RequestContext.get().endMetricRecord(recorder);
    }
}