package org.apache.atlas.repository.store.graph.v2.tags;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
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

import static org.apache.atlas.repository.store.graph.v2.CassandraConnector.CASSANDRA_HOSTNAME_PROPERTY;

/**
 * Data Access Object for tag operations in Cassandra
 */
@Repository
public class TagDAOCassandraImpl implements TagDAO {
    private static final Logger LOG = LoggerFactory.getLogger(TagDAOCassandraImpl.class);
    private static int BUCKET_POWER = 6;
    private static String KEYSPACE = null;
    private static final String CASSANDRA_BUCKET_POWER = "atlas.graph.new.bucket.power";
    public static final String CASSANDRA_NEW_KEYSPACE_PROPERTY = "atlas.graph.new.keyspace";
    private final CqlSession cassSession;
    private final PreparedStatement findTagsStmt;

    public TagDAOCassandraImpl() {
        try {
            KEYSPACE = ApplicationProperties.get().getString(CASSANDRA_NEW_KEYSPACE_PROPERTY, "tags");
            BUCKET_POWER = ApplicationProperties.get().getInt(CASSANDRA_BUCKET_POWER, 6);

            // Initialize Cassandra connection
            String hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");
            cassSession = initializeCassandraSession(hostname);

            // Prepare statements for reuse
            findTagsStmt = cassSession.prepare(
                    "SELECT * FROM tags.tag_direct_attachments WHERE id = ? AND bucket = ?"
            );
        } catch (Exception e) {
            LOG.error("Failed to initialize TagDAO", e);
            throw new RuntimeException("Failed to initialize TagDAO", e);
        }
    }

    @Override
    public List<AtlasTag> getTagsForVertex(String vertexId) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagsForAsset");
        List<AtlasTag> tags = new ArrayList<>();

        try {
            int bucket = calculateBucket(vertexId);
            BoundStatement bound = findTagsStmt.bind(vertexId, bucket);
            ResultSet rs = cassSession.execute(bound);

            for (Row row : rs) {
                AtlasTag tag = convertRowToTag(row);
                if (tag != null) {
                    tags.add(tag);
                }
            }
        } catch (Exception e) {
            LOG.error("Error fetching tags for asset {}", vertexId, e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return tags;
    }

    /**
     * Convert Cassandra row to AtlasTag object
     */
    private AtlasTag convertRowToTag(Row row) {
        try {
            // Get tag metadata JSON directly
            String tagMetaJson = row.getString("tag_meta_json");

            AtlasTag tag = AtlasType.fromJson(tagMetaJson, AtlasTag.class);
            tag.setTagTypeName(row.getString("tag_type_name"));

            return tag;
        } catch (Exception e) {
            LOG.error("Error converting row to AtlasTag for tag_type_name: {}",
                    row.getString("tag_type_name"), e);
            return null;
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
        int numBuckets = 1 << BUCKET_POWER; // 2^6=64
        return Integer.parseInt(vertexId) % numBuckets;
    }
}