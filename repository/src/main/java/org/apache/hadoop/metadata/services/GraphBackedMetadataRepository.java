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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.hadoop.metadata.util.GraphUtils;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * An implementation backed by a Graph database provided
 * as a Graph Service.
 */
public class GraphBackedMetadataRepository implements MetadataRepository {

    private static final Logger LOG =
            LoggerFactory.getLogger(GraphBackedMetadataRepository.class);

    private GraphService graphService;
    
    @Inject
    GraphBackedMetadataRepository(GraphService service) {
    	this.graphService = service;
    }

    @Override
    public String submitEntity(String entity, String entityType) {
        LOG.info("adding entity={} type={}", entity, entityType);
        @SuppressWarnings("unchecked")
        Map<String, String> properties = (Map<String, String>) JSONValue.parse(entity);

        final String entityName = properties.get("entityName");
        Preconditions.checkNotNull(entityName, "entity name cannot be null");

        // todo check if this is a duplicate

        final String guid = UUID.randomUUID().toString();
        final TransactionalGraph transactionalGraph = graphService.getTransactionalGraph();
        try {
            transactionalGraph.rollback();

            Vertex entityVertex = transactionalGraph.addVertex(null);
            entityVertex.setProperty("guid", guid);
            entityVertex.setProperty("entityName", entityName);
            entityVertex.setProperty("entityType", entityType);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                entityVertex.setProperty(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            transactionalGraph.rollback();
        } finally {
            transactionalGraph.commit();
        }

        return guid;
    }

    @Override
    public String getEntityDefinition(String entityName, String entityType) {
        LOG.info("Retrieving entity name={} type={}", entityName, entityType);
        Vertex entityVertex = GraphUtils.findVertex(graphService.getBlueprintsGraph(), entityName, entityType);
        if (entityVertex == null) {
            return null;
        }

        Map<String, String> properties = GraphUtils.extractProperties(entityVertex);
        return JSONValue.toJSONString(properties);
    }

    @Override
    public List<String> getEntityList(String entityType) {
        LOG.info("Retrieving entity list for type={}", entityType);
        return Collections.emptyList();
    }
}
