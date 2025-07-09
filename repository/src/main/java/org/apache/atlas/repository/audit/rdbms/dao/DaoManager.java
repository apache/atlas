/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.audit.rdbms.dao;

import org.apache.commons.configuration.Configuration;
import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceProviderResolver;
import javax.persistence.spi.PersistenceProviderResolverHolder;

import java.util.HashMap;
import java.util.Map;

public class DaoManager {
    private static final Logger LOG = LoggerFactory.getLogger(DaoManager.class);

    private final EntityManagerFactory emFactory;

    public DaoManager(Configuration jpaConfig) {
        Map<String, String> config = new HashMap<>();

        if (jpaConfig != null) {
            jpaConfig.getKeys().forEachRemaining(key -> {
                Object value = jpaConfig.getProperty(key);

                if (value != null) {
                    config.put(key, value.toString());
                }
            });
        }

        config.put(PersistenceUnitProperties.ECLIPSELINK_PERSISTENCE_XML, "META-INF/atlas-persistence.xml");

        LOG.debug("DaoManager: config={}", config);

        PersistenceProviderResolver resolver = PersistenceProviderResolverHolder.getPersistenceProviderResolver();
        EntityManagerFactory        emf      = null;

        for (PersistenceProvider provider : resolver.getPersistenceProviders()) {
            LOG.debug("PersistenceProvider: {}", provider);

            emf = provider.createEntityManagerFactory("atlasPU", config);

            if (emf != null) {
                break;
            }
        }

        emFactory = emf;
    }

    public EntityManagerFactory getEntityManagerFactory() {
        return emFactory;
    }

    public DbEntityAuditDao getEntityAuditDao(EntityManager em) {
        return new DbEntityAuditDao(em);
    }

    public void close() {
        LOG.info("DaoManager.close()");

        if (this.emFactory.isOpen()) {
            this.emFactory.close();
        }
    }
}
