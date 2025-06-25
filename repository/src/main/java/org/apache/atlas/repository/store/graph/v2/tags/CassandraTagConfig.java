package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;


public class CassandraTagConfig {

    public static final String CASSANDRA_TAG_TABLE_NAME = "atlas.graph.tag.table.name";
    public static final int CASSANDRA_PORT = 9042;
    public static final String CASSANDRA_HOSTNAME_PROPERTY = "atlas.graph.storage.hostname";
    public static final String CASSANDRA_REPLICATION_FACTOR_PROPERTY = "atlas.graph.storage.replication-factor";
    public static int BUCKET_POWER = 5;
    public static final String CASSANDRA_NEW_KEYSPACE_PROPERTY = "atlas.graph.new.keyspace";

    // Configuration constants
    public static final String KEYSPACE;
    public static final String TABLE_NAME;
    public static final String HOST_NAME;

    static {
        try {
            KEYSPACE = ApplicationProperties.get().getString(CASSANDRA_NEW_KEYSPACE_PROPERTY, "tags_v2");
            TABLE_NAME = ApplicationProperties.get().getString(CASSANDRA_TAG_TABLE_NAME, "effective_tags");
            HOST_NAME = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

}
