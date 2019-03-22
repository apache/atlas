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
package org.apache.atlas.repository.patches;

import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer.UniqueKind;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.FAILED;
import static org.apache.atlas.repository.graph.GraphHelper.getGuid;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.findActiveEntityVerticesByType;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.setEncodedProperty;

public class UniqueAttributePatchHandler extends AtlasJavaPatchHandler {
    private static final String PATCH_ID          = "JAVA_PATCH_0000_001";
    private static final String PATCH_DESCRIPTION = "Add new vertex property for each unique attribute of active entities";
    private static final Logger LOG               = LoggerFactory.getLogger(UniqueAttributePatchHandler.class);

    public UniqueAttributePatchHandler(PatchContext context) {
        super(context, PATCH_ID, PATCH_DESCRIPTION);
    }

    @Override
    public void applyPatch() {
        Collection<AtlasEntityType> allEntityTypes = typeRegistry.getAllEntityTypes();
        boolean                     patchFailed    = false;

        for (AtlasEntityType entityType : allEntityTypes) {
            String                      typeName          = entityType.getTypeName();
            Map<String, AtlasAttribute> uniqAttributes    = entityType.getUniqAttributes();
            int                         entitiesProcessed = 0;

            LOG.info("Applying java patch: {} for type: {}", getPatchId(), typeName);

            if (MapUtils.isNotEmpty(uniqAttributes)) {
                Collection<AtlasAttribute> attributes = uniqAttributes.values();

                try {
                    // register unique attribute property keys in graph
                    registerUniqueAttrPropertyKeys(attributes);

                    Iterator<AtlasVertex> iterator = findActiveEntityVerticesByType(typeName);

                    while (iterator.hasNext()) {
                        AtlasVertex entityVertex = iterator.next();
                        boolean     patchApplied = false;

                        for (AtlasAttribute attribute : attributes) {
                            String                       uniquePropertyKey = attribute.getVertexUniquePropertyName();
                            Collection<? extends String> propertyKeys      = entityVertex.getPropertyKeys();

                            if (!propertyKeys.contains(uniquePropertyKey)) {
                                String            propertyKey   = attribute.getVertexPropertyName();
                                AtlasAttributeDef attributeDef  = attribute.getAttributeDef();
                                Object            uniqAttrValue = entityRetriever.mapVertexToPrimitive(entityVertex, propertyKey, attributeDef);

                                // add the unique attribute property to vertex
                                setEncodedProperty(entityVertex, uniquePropertyKey, uniqAttrValue);

                                try {
                                    graph.commit();

                                    patchApplied = true;

                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("{}: Added unique attribute property: {} to entity: {} ({})",
                                                   PATCH_ID, uniquePropertyKey, getGuid(entityVertex), typeName);
                                    }
                                } catch (Throwable t) {
                                    LOG.warn("Java patch ({}): failed to update entity guid: {}; typeName: {}; attrName: {}; attrValue: {}",
                                              getPatchId(), getGuid(entityVertex), typeName, attribute.getName(), uniqAttrValue);

                                    continue;
                                }
                            }
                        }

                        if (patchApplied) {
                            entitiesProcessed++;
                        }

                        if (entitiesProcessed % 1000 == 0) {
                            LOG.info("Java patch: {} : processed {} {} entities.", getPatchId(), entitiesProcessed, typeName);
                        }
                    }
                } catch (IndexException e) {
                    LOG.error("Java patch: {} failed! error: {}", getPatchId(), e);

                    patchFailed = true;

                    break;
                }
            }

            LOG.info("Applied java patch ({}) for type: {}; Total processed: {}", getPatchId(), typeName, entitiesProcessed);
        }

        if (patchFailed) {
            setPatchStatus(FAILED);
        } else {
            setPatchStatus(APPLIED);
        }

        LOG.info("Applied java patch: {}; status: {}", getPatchId(), getPatchStatus());

        updatePatchVertex(getPatchStatus());
    }

    private void registerUniqueAttrPropertyKeys(Collection<AtlasAttribute> attributes) throws IndexException {
        AtlasGraphManagement management = graph.getManagementSystem();
        boolean              idxCreated = false;

        for (AtlasAttribute attribute : attributes) {
            String  uniquePropertyName       = attribute.getVertexUniquePropertyName();
            boolean uniquePropertyNameExists = management.getPropertyKey(uniquePropertyName) != null;

            if (!uniquePropertyNameExists) {
                AtlasAttributeDef attributeDef   = attribute.getAttributeDef();
                boolean           isIndexable    = attributeDef.getIsIndexable();
                String            attribTypeName = attributeDef.getTypeName();
                Class             propertyClass  = indexer.getPrimitiveClass(attribTypeName);
                AtlasCardinality  cardinality    = indexer.toAtlasCardinality(attributeDef.getCardinality());

                indexer.createVertexIndex(management, uniquePropertyName, UniqueKind.NONE, propertyClass, cardinality, isIndexable, true);

                idxCreated = true;
            }
        }

        //Commit indexes
        if (idxCreated) {
            indexer.commit(management);
        }
    }
}