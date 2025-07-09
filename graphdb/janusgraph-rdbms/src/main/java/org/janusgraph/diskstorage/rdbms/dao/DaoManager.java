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

import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceProviderResolver;
import javax.persistence.spi.PersistenceProviderResolverHolder;

import java.util.HashMap;
import java.util.Map;

/**
 * DAO manager that initializes JPA layer
 *
 * Sample properties to initialize JPA
 *   storage.backend=rdbms
 *   storage.rdbms.jpa.javax.persistence.jdbc.dialect=org.eclipse.persistence.platform.database.PostgreSQLPlatform
 *   storage.rdbms.jpa.javax.persistence.jdbc.driver=org.postgresql.Driver
 *   storage.rdbms.jpa.javax.persistence.jdbc.url=jdbc:postgresql://dbhost/dbname
 *   storage.rdbms.jpa.javax.persistence.jdbc.user=janus
 *   storage.rdbms.jpa.javax.persistence.jdbc.password=janusR0cks!
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
        Map<String, String> config = new HashMap<>();

        if (jpaConfig != null) {
            for (Map.Entry<String, Object> entry : jpaConfig.entrySet()) {
                String key   = entry.getKey();
                Object value = entry.getValue();

                if (value != null) {
                    config.put(key, value.toString());
                }
            }
        }

        config.put(PersistenceUnitProperties.ECLIPSELINK_PERSISTENCE_XML, "META-INF/janus-persistence.xml");

        LOG.debug("DaoManager: config={}", config);

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
