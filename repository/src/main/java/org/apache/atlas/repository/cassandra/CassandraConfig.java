package org.apache.atlas.repository.cassandra;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.net.InetSocketAddress;
import java.time.Duration;


@Configuration
public class CassandraConfig {


    @Bean(destroyMethod = "close")
    public CqlSession cqlSession() throws AtlasException {
        // Read configuration from Atlas properties
        String CASSANDRA_HOSTNAME_PROPERTY = "atlas.graph.storage.hostname";
        String CASSANDRA_PORT_PROPERTY = "atlas.graph.storage.port";
        String CASSANDRA_DATACENTER_PROPERTY = "atlas.graph.storage.datacenter";

        String keyspace = AtlasConfiguration.ATLAS_CASSANDRA_VANILLA_KEYSPACE.getString();

        String hostname = ApplicationProperties.get().getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");

        int port = ApplicationProperties.get().getInt(CASSANDRA_PORT_PROPERTY, 9042);

        String datacenter = ApplicationProperties.get().getString(CASSANDRA_DATACENTER_PROPERTY, "datacenter1");

        // Initialize Cassandra connection
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, port))
                .withConfigLoader(
                        DriverConfigLoader.programmaticBuilder()
                                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
                                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(15))
                                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(20))
                                .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofMillis(500))
                                .withDuration(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS, Duration.ofSeconds(20))
                                .build())
                .withLocalDatacenter(datacenter)
                .withKeyspace(keyspace)
                .build();
    }

}