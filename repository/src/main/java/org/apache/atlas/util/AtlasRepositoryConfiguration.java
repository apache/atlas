/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.audit.HBaseBasedAuditRepository;
import org.apache.atlas.repository.graph.DeleteHandler;
import org.apache.atlas.repository.graph.SoftDeleteHandler;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v1.SoftDeleteHandlerV1;
import org.apache.atlas.typesystem.types.cache.DefaultTypeCache;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Atlas configuration for repository project
 *
 */
public class AtlasRepositoryConfiguration {

    private static Logger LOG = LoggerFactory.getLogger(AtlasRepositoryConfiguration.class);

    public static final String TYPE_CACHE_IMPLEMENTATION_PROPERTY = "atlas.TypeCache.impl";
    public static final String AUDIT_EXCLUDED_OPERATIONS = "atlas.audit.excludes";
    private static List<String> skippedOperations = null;
    public static final String SEPARATOR = ":";

    @SuppressWarnings("unchecked")
    public static Class<? extends TypeCache> getTypeCache() {
        // Get the type cache implementation class from Atlas configuration.
        try {
            Configuration config = ApplicationProperties.get();
            return ApplicationProperties.getClass(config, TYPE_CACHE_IMPLEMENTATION_PROPERTY,
                    DefaultTypeCache.class.getName(), TypeCache.class);
        } catch (AtlasException e) {
            LOG.error("Error loading typecache ", e);
            return DefaultTypeCache.class;
        }
    }
    private static final String AUDIT_REPOSITORY_IMPLEMENTATION_PROPERTY = "atlas.EntityAuditRepository.impl";

    @SuppressWarnings("unchecked")
    public static Class<? extends EntityAuditRepository> getAuditRepositoryImpl() {
        try {
            Configuration config = ApplicationProperties.get();
            return ApplicationProperties.getClass(config, 
                    AUDIT_REPOSITORY_IMPLEMENTATION_PROPERTY, HBaseBasedAuditRepository.class.getName(), EntityAuditRepository.class);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String DELETE_HANDLER_IMPLEMENTATION_PROPERTY = "atlas.DeleteHandler.impl";

    @SuppressWarnings("unchecked")
    public static Class<? extends DeleteHandler> getDeleteHandlerImpl() {
        try {
            Configuration config = ApplicationProperties.get();
            return ApplicationProperties.getClass(config, 
                    DELETE_HANDLER_IMPLEMENTATION_PROPERTY, SoftDeleteHandler.class.getName(), DeleteHandler.class);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    public static Class<? extends DeleteHandlerV1> getDeleteHandlerV1Impl() {
        try {
            Configuration config = ApplicationProperties.get();
            return ApplicationProperties.getClass(config,
                DELETE_HANDLER_IMPLEMENTATION_PROPERTY, SoftDeleteHandlerV1.class.getName(), DeleteHandlerV1.class);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static final String GRAPH_DATABASE_IMPLEMENTATION_PROPERTY = "atlas.graphdb.backend";
    private static final String DEFAULT_GRAPH_DATABASE_IMPLEMENTATION_CLASS = "org.apache.atlas.repository.graphdb.titan0.Titan0GraphDatabase";
    
    @SuppressWarnings("unchecked")
    public static Class<? extends GraphDatabase> getGraphDatabaseImpl() {
        try {
            Configuration config = ApplicationProperties.get();
            return ApplicationProperties.getClass(config, 
                    GRAPH_DATABASE_IMPLEMENTATION_PROPERTY, DEFAULT_GRAPH_DATABASE_IMPLEMENTATION_CLASS, GraphDatabase.class);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the list of operations which are configured to be skipped from auditing
     * Valid format is HttpMethod:URL eg: GET:Version
     * @return list of string
     * @throws AtlasException
     */
    public static List<String> getAuditExcludedOperations(Configuration config) throws AtlasException {
        if (config == null) {
            try {
                config = ApplicationProperties.get();
            } catch (AtlasException e) {
                LOG.error(" Error reading operations for auditing ", e);
                throw e;
            }
        }
        if (skippedOperations == null) {
            skippedOperations = new ArrayList<String>();
                String[] skipAuditForOperations = config
                        .getStringArray(AUDIT_EXCLUDED_OPERATIONS);
                if (skipAuditForOperations != null
                        && skipAuditForOperations.length > 0) {
                    for (String skippedOperation : skipAuditForOperations) {
                        String[] excludedOperations = skippedOperation.trim().toLowerCase().split(SEPARATOR);
                        if (excludedOperations!= null && excludedOperations.length == 2) {
                            skippedOperations.add(skippedOperation.toLowerCase());
                        } else {
                            LOG.error("Invalid format for skipped operation {}. Valid format is HttpMethod:URL eg: GET:Version", skippedOperation);
                        }
                    }
                }
        }
        return skippedOperations;
    }

    public static boolean isExcludedFromAudit(Configuration config, String httpMethod, String httpUrl) throws AtlasException {
        if (getAuditExcludedOperations(config).size() > 0) {
            return getAuditExcludedOperations(config).contains(httpMethod.toLowerCase() + SEPARATOR + httpUrl.toLowerCase());
        } else {
            return false;
        }
    }
   public static void resetExcludedOperations() { //for test purpose only
        skippedOperations = null;
    }

}
