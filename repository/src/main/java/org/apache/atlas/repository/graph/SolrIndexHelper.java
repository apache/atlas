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
package org.apache.atlas.repository.graph;

import org.apache.atlas.AtlasException;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.DEFAULT_SEARCHWEIGHT;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;

/**
 This is a component that will go through all entity type definitions and create free text index
 request handler with SOLR. This is a no op class in non-solr index based deployments.
 This component needs to be initialized after type definitions are completely fixed with the needed patches (Ordder 3 initialization).
 */
public class SolrIndexHelper implements IndexChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(SolrIndexHelper.class);

    public static final int DEFAULT_SEARCHWEIGHT_FOR_STRINGS = 3;
    public static final int SEARCHWEIGHT_FOR_CLASSIFICATIONS = 10;
    public static final int SEARCHWEIGHT_FOR_TYPENAME        = 1;

    private final AtlasTypeRegistry typeRegistry;


    public SolrIndexHelper(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void onChange(ChangedTypeDefs changedTypeDefs) {
        if (!AtlasRepositoryConfiguration.isFreeTextSearchEnabled() ||
            changedTypeDefs == null || !changedTypeDefs.hasEntityDef()) { // nothing to do if there are no changes to entity-defs
            return;
        }

        try {
            AtlasGraph            atlasGraph                    = AtlasGraphProvider.getGraphInstance();
            AtlasGraphIndexClient atlasGraphIndexClient         = atlasGraph.getGraphIndexClient();
            Map<String, Integer>  attributeName2SearchWeightMap = getAttributesWithSearchWeights();

            atlasGraphIndexClient.applySearchWeight(Constants.VERTEX_INDEX, attributeName2SearchWeightMap);
        } catch (AtlasException e) {
            LOG.error("Error encountered in handling type system change notification.", e);
            throw new RuntimeException("Error encountered in handling type system change notification.", e);
        }
    }

    private Map<String, Integer> getAttributesWithSearchWeights() {
        Map<String, Integer>       attributesWithSearchWeights = new HashMap<>();
        Collection<AtlasEntityDef> allEntityDefs               = typeRegistry.getAllEntityDefs();

        attributesWithSearchWeights.put(CLASSIFICATION_TEXT_KEY, SEARCHWEIGHT_FOR_CLASSIFICATIONS);
        attributesWithSearchWeights.put(TYPE_NAME_PROPERTY_KEY, SEARCHWEIGHT_FOR_TYPENAME);

        if (CollectionUtils.isNotEmpty(allEntityDefs)) {
            for (AtlasEntityDef entityDef : allEntityDefs) {
                processEntity(attributesWithSearchWeights, entityDef);
            }
        }

        return attributesWithSearchWeights;
    }

    private void processEntity(Map<String, Integer> attributesWithSearchWeights, AtlasEntityDef entityDef) {
        for (AtlasAttributeDef attributeDef : entityDef.getAttributeDefs()) {
            processAttributeDefinition(attributesWithSearchWeights, entityDef, attributeDef);
        }
    }

    private void processAttributeDefinition(Map<String, Integer> attributesWithSearchWeights, AtlasEntityDef entityDef, AtlasAttributeDef attributeDef) {
        if (GraphBackedSearchIndexer.isStringAttribute(attributeDef)) {
            final String attributeName = GraphBackedSearchIndexer.getEncodedPropertyName(entityDef.getName(), attributeDef);
            int          searchWeight  = attributeDef.getSearchWeight();

            if (searchWeight == DEFAULT_SEARCHWEIGHT) {
                //We will use default search weight of 3 for string attributes.
                //this will make the string data searchable like in FullTextIndex Searcher using Free Text searcher.
                searchWeight = DEFAULT_SEARCHWEIGHT_FOR_STRINGS;
            } else if (!GraphBackedSearchIndexer.isValidSearchWeight(searchWeight)) { //validate the value provided in the model.
                LOG.warn("Invalid search weight {} for attribute {}.{}. Will use default {}", searchWeight, entityDef.getName(), attributeName, DEFAULT_SEARCHWEIGHT_FOR_STRINGS);

                searchWeight = DEFAULT_SEARCHWEIGHT_FOR_STRINGS;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Applying search weight {} for attribute {}.{}", searchWeight, entityDef.getName(), attributeName);
            }

            attributesWithSearchWeights.put(attributeName, searchWeight);
        }
    }
}