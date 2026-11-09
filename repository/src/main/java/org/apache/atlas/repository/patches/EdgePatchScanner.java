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
package org.apache.atlas.repository.patches;

import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;

/**
 * Scans relationship edges in bounded batches to avoid loading oversized Janus/HBase vertex rows.
 */
public final class EdgePatchScanner {
    private static final Logger LOG = LoggerFactory.getLogger(EdgePatchScanner.class);

    private EdgePatchScanner() {
    }

    /**
     * Submits edge ids that have {@link org.apache.atlas.repository.Constants#ENTITY_TYPE_PROPERTY_KEY} set,
     * by querying per registered relationship type with offset/limit pagination.
     */
    public static int submitRelationshipEdgesWithEntityType(AtlasGraph graph, AtlasTypeRegistry typeRegistry, WorkItemManager manager) {
        Collection<AtlasRelationshipType> relationshipTypes = typeRegistry.getAllRelationshipTypes();
        int                               total             = 0;
        int                               batchSize         = EdgePatchProcessor.BATCH_SIZE;

        for (AtlasRelationshipType relationshipType : relationshipTypes) {
            total += submitEdgesForRelationshipType(graph, manager, relationshipType.getTypeName(), batchSize);
        }

        LOG.info("found {} edges with typeName != null", total);

        return total;
    }

    private static int submitEdgesForRelationshipType(AtlasGraph graph, WorkItemManager manager, String relationshipTypeName, int batchSize) {
        int offset = 0;
        int typeTotal = 0;

        while (true) {
            Iterable<? extends AtlasEdge<?, ?>> edges;
            try {
                AtlasGraphQuery<?, ?> query = graph.query().has(ENTITY_TYPE_PROPERTY_KEY, relationshipTypeName);

                edges = query.edges(offset, batchSize);
            } catch (Exception ex) {
                LOG.warn("submitEdgesForRelationshipType(typeName={}, offset={}): scan failed, skipping remainder for this type",
                        relationshipTypeName, offset, ex);

                break;
            }

            int pageCount = 0;

            for (AtlasEdge edge : edges) {
                pageCount++;

                try {
                    if (edge.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class) != null) {
                        manager.checkProduce(edge.getId().toString());

                        typeTotal++;
                    }
                } catch (Exception edgeEx) {
                    LOG.warn("submitEdgesForRelationshipType(typeName={}): skipping edgeId={}",
                            relationshipTypeName, edge.getId(), edgeEx);
                }
            }

            if (pageCount < batchSize) {
                break;
            }

            offset += batchSize;
        }

        if (typeTotal > 0) {
            LOG.info("submitEdgesForRelationshipType(typeName={}): queued {} edges", relationshipTypeName, typeTotal);
        }

        return typeTotal;
    }
}
