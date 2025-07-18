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
import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceProviderResolver;
import javax.persistence.spi.PersistenceProviderResolverHolder;
import javax.sql.DataSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * DAO manager that initializes JPA layer
 *
 * Sample properties to initialize JPA
 *   storage.backend=rdbms
 *   storage.rdbms.jpa.dataSource.driverClassName=org.postgresql.Driver
 *   storage.rdbms.jpa.dataSource.jdbcUrl=jdbc:postgresql://dbhost/dbname
 *   storage.rdbms.jpa.dataSource.username=janus
 *   storage.rdbms.jpa.dataSource.password=janusR0cks!
 *   storage.rdbms.jpa.dataSource.maximumPoolSize=40
 *   storage.rdbms.jpa.dataSource.minimumPoolSize=5
 *   storage.rdbms.jpa.dataSource.minimumIdle=janusR0cks!
 *   storage.rdbms.jpa.dataSource.idleTimeout=300000
 *   storage.rdbms.jpa.dataSource.connectionTestQuery=select 1
 *   storage.rdbms.jpa.dataSource.maxLifetime=1800000
 *   storage.rdbms.jpa.dataSource.connectionTimeout=30000
 *   storage.rdbms.jpa.javax.persistence.jdbc.dialect=org.eclipse.persistence.platform.database.PostgreSQLPlatform
 *   storage.rdbms.jpa.javax.persistence.schema-generation.database.action=create
 *   storage.rdbms.jpa.javax.persistence.schema-generation.create-database-schemas=true
 *   storage.rdbms.jpa.javax.persistence.schema-generation.create-source=metadata
 *
 */
public class DaoManager {
    private static final Logger LOG = LoggerFactory.getLogger(DaoManager.class);

    private final EntityManagerFactory emFactory;

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
}
