/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.janusgraph.diskstorage.rdbms.dao;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceProviderResolver;
import javax.persistence.spi.PersistenceProviderResolverHolder;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * DAO manager that initializes JPA layer
 *
 * Sample properties to be set in atlas-application.properties for RDBMS storage backend:
 *   atlas.graph.storage.backend=rdbms
 *   atlas.graph.storage.rdbms.jpa.hikari.driverClassName=org.postgresql.Driver
 *   atlas.graph.storage.rdbms.jpa.hikari.jdbcUrl=jdbc:postgresql://atlas-db/atlas
 *   atlas.graph.storage.rdbms.jpa.hikari.username=atlas
 *   atlas.graph.storage.rdbms.jpa.hikari.password=atlasR0cks!
 *   atlas.graph.storage.rdbms.jpa.hikari.maximumPoolSize=40
 *   atlas.graph.storage.rdbms.jpa.hikari.minimumIdle=5
 *   atlas.graph.storage.rdbms.jpa.hikari.idleTimeout=300000
 *   atlas.graph.storage.rdbms.jpa.hikari.connectionTestQuery=select 1
 *   atlas.graph.storage.rdbms.jpa.hikari.maxLifetime=1800000
 *   atlas.graph.storage.rdbms.jpa.hikari.connectionTimeout=30000
 *   atlas.graph.storage.rdbms.jpa.javax.persistence.jdbc.dialect=org.eclipse.persistence.platform.database.PostgreSQLPlatform
 *   atlas.graph.storage.rdbms.jpa.javax.persistence.schema-generation.database.action=create
 *   atlas.graph.storage.rdbms.jpa.javax.persistence.schema-generation.create-database-schemas=true
 *   atlas.graph.storage.rdbms.jpa.javax.persistence.schema-generation.create-source=script
 *   atlas.graph.storage.rdbms.jpa.javax.persistence.schema-generation.create-script-source=META-INF/postgres/create_schema.sql
 *   atlas.EntityAuditRepository.impl=org.apache.atlas.repository.audit.rdbms.RdbmsBasedAuditRepository
 *
 */
public class DaoManager {
    private static final Logger LOG = LoggerFactory.getLogger(DaoManager.class);

    private final EntityManagerFactory emFactory;
    private static final String HIKARI_PASSWORD_KEY = "atlas.graph.storage.rdbms.jpa.hikari.password";

    /**
     *
     * @param jpaConfig
     */
    public DaoManager(Map<String, Object> jpaConfig) {
        Map<String, Object> config       = new HashMap<>();
        Properties          hikariConfig = new Properties();

        if (jpaConfig != null) {
            for (Map.Entry<String, Object> entry : jpaConfig.entrySet()) {
                String key   = entry.getKey();
                Object value = entry.getValue();

                if (value != null) {
                    if (key.startsWith("hikari.")) {
                        if ("hikari.password".equals(key)) {
                            try {
                                String decrypted = ApplicationProperties.getDecryptedPassword(ApplicationProperties.get(), HIKARI_PASSWORD_KEY);
                                if (decrypted != null) {
                                    value = decrypted;
                                }
                            } catch (AtlasException e) {
                                LOG.error("Error in getting secure password ", e);
                            }
                        }
                        hikariConfig.put(key.substring("hikari".length() + 1), value.toString());
                    } else {
                        config.put(key, value.toString());
                    }
                }
            }
        }

        DataSource dataSource = new HikariDataSource(new HikariConfig(hikariConfig));

        config.put(PersistenceUnitProperties.ECLIPSELINK_PERSISTENCE_XML, "META-INF/janus-persistence.xml");
        config.put(PersistenceUnitProperties.NON_JTA_DATASOURCE, dataSource);
        suppressSchemaGenerationIfSchemaExists(dataSource, config);

        PersistenceProviderResolver resolver = PersistenceProviderResolverHolder.getPersistenceProviderResolver();
        EntityManagerFactory        emf      = null;

        for (PersistenceProvider provider : resolver.getPersistenceProviders()) {
            LOG.debug("PersistenceProvider: {}", provider);

            emf = provider.createEntityManagerFactory("janusPU", config);

            if (emf != null) {
                break;
            }
        }

        emFactory = emf;
    }

    public EntityManager createEntityManager() {
        return emFactory.createEntityManager();
    }

    public void close() {
        LOG.info("DaoManager.close()");

        if (this.emFactory.isOpen()) {
            this.emFactory.close();
        }
    }

    /**
     * EclipseLink {@code schema-generation.database.action=create} runs {@code CREATE UNIQUE INDEX}
     * on every graph open, even when the tables already exist. {@code IF NOT EXISTS} still takes a
     * {@code ShareLock} on {@code janus_column}. A peer in the middle of index-init commit holds
     * uncommitted {@code INSERT}s on that table, so the create-index waits, and the peer's later
     * {@code DELETE} (JanusGraph lock cleanup) waits on the create-index: a hang Postgres will not
     * abort, because the INSERT session is idle in its transaction rather than waiting.
     */
    static void suppressSchemaGenerationIfSchemaExists(DataSource dataSource, Map<String, Object> config) {
        if (!janusSchemaExists(dataSource)) {
            return;
        }

        Object previous = config.put(PersistenceUnitProperties.SCHEMA_GENERATION_DATABASE_ACTION, "none");

        config.remove(PersistenceUnitProperties.SCHEMA_GENERATION_CREATE_DATABASE_SCHEMAS);
        config.remove(PersistenceUnitProperties.SCHEMA_GENERATION_CREATE_SOURCE);
        config.remove(PersistenceUnitProperties.SCHEMA_GENERATION_CREATE_SCRIPT_SOURCE);

        LOG.info("Janus RDBMS schema already exists; skipping EclipseLink schema generation (was {})", previous);
    }

    static boolean janusSchemaExists(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            return tableExists(connection, "janus_column") || tableExists(connection, "JANUS_COLUMN");
        } catch (SQLException exception) {
            LOG.warn("Could not check whether the Janus RDBMS schema exists; leaving schema generation as configured", exception);

            return false;
        }
    }

    private static boolean tableExists(Connection connection, String tableName) throws SQLException {
        try (ResultSet tables = connection.getMetaData().getTables(null, null, tableName, new String[] {"TABLE"})) {
            return tables.next();
        }
    }
}
