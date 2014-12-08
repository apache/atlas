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

package org.apache.hadoop.metadata.services;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.service.Services;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An implementation backed by Titan Graph DB.
 */
public class GraphBackedMetadataRepositoryService implements MetadataRepositoryService {

    private static final Logger LOG =
            LoggerFactory.getLogger(GraphBackedMetadataRepositoryService.class);
    public static final String NAME = GraphBackedMetadataRepositoryService.class.getSimpleName();

    private GraphService graphService;

    /**
     * Name of the service.
     *
     * @return name of the service
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Starts the service. This method blocks until the service has completely started.
     *
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        graphService = Services.get().getService(TitanGraphService.NAME);
        if (graphService == null) {
            throw new RuntimeException("graph service is not initialized");
        }
    }

    /**
     * Stops the service. This method blocks until the service has completely shut down.
     */
    @Override
    public void stop() {
        // do nothing
    }

    /**
     * A version of stop() that is designed to be usable in Java7 closure
     * clauses.
     * Implementation classes MUST relay this directly to {@link #stop()}
     *
     * @throws java.io.IOException never
     * @throws RuntimeException    on any failure during the stop operation
     */
    @Override
    public void close() throws IOException {
        stop();
    }

    private TitanGraph getGraph() {
        return ((TitanGraphService) graphService).getTitanGraph();
    }

    @Override
    public String submitEntity(String entity, String entityType) {
        Map<String, String> properties = (Map<String, String>) JSONValue.parse(entity);

        final String entityName = properties.get("entityName");
        Preconditions.checkNotNull(entityName, "entity name cannot be null");

        // todo check if this is a duplicate

        final String guid = UUID.randomUUID().toString();
        try {
            getGraph().newTransaction();

            Vertex entityVertex = getGraph().addVertex(null);
            entityVertex.setProperty("guid", guid);
            entityVertex.setProperty("entityName", entityName);
            entityVertex.setProperty("entityType", entityType);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                entityVertex.setProperty(entry.getKey(), entry.getValue());
            }
        } finally {
            getGraph().commit();
        }

        return guid;
    }

    @Override
    public String getEntityDefinition(String entityName, String entityType) {
        Vertex entityVertex = findVertex(entityName, entityType);
        if (entityVertex == null) {
            return null;
        }

        Map<String, String> properties = extractProperties(entityVertex);
        return JSONValue.toJSONString(properties);
    }

    protected Vertex findVertex(String entityName, String entityType) {
        LOG.debug("Finding vertex for: name={}, type={}", entityName, entityType);

        GraphQuery query = getGraph().query()
                .has("entityName", entityName)
                .has("entityType", entityType);
        Iterator<Vertex> results = query.vertices().iterator();
        return results.hasNext() ? results.next() : null;  // returning one since name/type is unique
    }

    private Map<String, String> extractProperties(Vertex entityVertex) {
        Map<String, String> properties = new HashMap<>();
        for (String key : entityVertex.getPropertyKeys()) {
            properties.put(key, String.valueOf(entityVertex.getProperty(key)));
        }

        return properties;
    }

    @Override
    public List<String> getEntityList(String entityType) {
        return null;
    }
}
