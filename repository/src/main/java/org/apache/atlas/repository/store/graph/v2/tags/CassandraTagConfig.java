package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;


public class CassandraTagConfig {

    public static final String CASSANDRA_TAG_TABLE_NAME = "atlas.graph.tag.table.name";
    public static final String CASSANDRA_PROPAGATED_TAG_TABLE_NAME = "atlas.graph.propagated.tag.table.name";
    public static final int CASSANDRA_PORT = 9042;
    public static final String CASSANDRA_HOSTNAME_PROPERTY = "atlas.graph.storage.hostname";
    public static int BUCKET_POWER = 5;
    public static final String CASSANDRA_NEW_KEYSPACE_PROPERTY = "atlas.graph.new.keyspace";

    // Configuration constants
    public static final String KEYSPACE;
    public static final String EFFECTIVE_TAGS_TABLE_NAME;
    public static final String HOST_NAME;
    // New table name for optimized propagation lookups
    public static final String PROPAGATED_TAGS_TABLE_NAME;

    static {
        try {
            KEYSPACE = ApplicationProperties.get().getString(CASSANDRA_NEW_KEYSPACE_PROPERTY, "tags");
            EFFECTIVE_TAGS_TABLE_NAME = ApplicationProperties.get().getString(CASSANDRA_TAG_TABLE_NAME, "tags_by_id");
            PROPAGATED_TAGS_TABLE_NAME = ApplicationProperties.get().getString(CASSANDRA_PROPAGATED_TAG_TABLE_NAME, "propagated_tags_by_source");
            HOST_NAME = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

}
