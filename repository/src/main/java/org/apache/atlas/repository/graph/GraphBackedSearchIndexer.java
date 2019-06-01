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

package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.discovery.SearchIndexer;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.listener.TypeDefChangeListener;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasEnumType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graphdb.AtlasCardinality.LIST;
import static org.apache.atlas.repository.graphdb.AtlasCardinality.SET;
import static org.apache.atlas.repository.graphdb.AtlasCardinality.SINGLE;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.isReference;
import static org.apache.atlas.type.AtlasStructType.UNIQUE_ATTRIBUTE_SHADE_PROPERTY_PREFIX;
import static org.apache.atlas.type.AtlasTypeUtil.isArrayType;
import static org.apache.atlas.type.AtlasTypeUtil.isMapType;


/**
 * Adds index for properties of a given type when its added before any instances are added.
 */
@Component
public class GraphBackedSearchIndexer implements SearchIndexer, ActiveStateChangeHandler, TypeDefChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedSearchIndexer.class);

    private static final String VERTEX_ID_IN_IMPORT_KEY = "__vIdInImport";
    private static final String EDGE_ID_IN_IMPORT_KEY   = "__eIdInImport";
    private static final List<Class> INDEX_EXCLUSION_CLASSES = new ArrayList() {
        {
            add(Boolean.class);
            add(BigDecimal.class);
            add(BigInteger.class);
        }
    };

    // Added for type lookup when indexing the new typedefs
    private final AtlasTypeRegistry typeRegistry;

    //allows injection of a dummy graph for testing
    private IAtlasGraphProvider provider;

    private boolean     recomputeIndexedKeys = true;
    private Set<String> vertexIndexKeys      = new HashSet<>();

    private enum UniqueKind { NONE, GLOBAL_UNIQUE, PER_TYPE_UNIQUE }

    @Inject
    public GraphBackedSearchIndexer(AtlasTypeRegistry typeRegistry) throws AtlasException {
        this(new AtlasGraphProvider(), ApplicationProperties.get(), typeRegistry);
    }

    @VisibleForTesting
    GraphBackedSearchIndexer(IAtlasGraphProvider provider, Configuration configuration, AtlasTypeRegistry typeRegistry)
            throws IndexException, RepositoryException {
        this.provider = provider;
        this.typeRegistry = typeRegistry;
        if (!HAConfiguration.isHAEnabled(configuration)) {
            initialize(provider.get());
        }
    }

    /**
     * Initialize global indices for JanusGraph on server activation.
     *
     * Since the indices are shared state, we need to do this only from an active instance.
     */
    @Override
    public void instanceIsActive() throws AtlasException {
        LOG.info("Reacting to active: initializing index");
        try {
            initialize();
        } catch (RepositoryException | IndexException e) {
            throw new AtlasException("Error in reacting to active on initialization", e);
        }
    }

    @Override
    public void instanceIsPassive() {
        LOG.info("Reacting to passive state: No action right now.");
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.GRAPH_BACKED_SEARCH_INDEXER.getOrder();
    }

    @Override
    public void onChange(ChangedTypeDefs changedTypeDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing changed typedefs {}", changedTypeDefs);
        }
        AtlasGraphManagement management = null;
        try {
            management = provider.get().getManagementSystem();

            // Update index for newly created types
            if (CollectionUtils.isNotEmpty(changedTypeDefs.getCreateTypeDefs())) {
                for (AtlasBaseTypeDef typeDef : changedTypeDefs.getCreateTypeDefs()) {
                    updateIndexForTypeDef(management, typeDef);
                }
            }

            // Update index for updated types
            if (CollectionUtils.isNotEmpty(changedTypeDefs.getUpdatedTypeDefs())) {
                for (AtlasBaseTypeDef typeDef : changedTypeDefs.getUpdatedTypeDefs()) {
                    updateIndexForTypeDef(management, typeDef);
                }
            }

            // Invalidate the property key for deleted types
            if (CollectionUtils.isNotEmpty(changedTypeDefs.getDeletedTypeDefs())) {
                for (AtlasBaseTypeDef typeDef : changedTypeDefs.getDeletedTypeDefs()) {
                    deleteIndexForType(management, typeDef);
                }
            }

            //Commit indexes
            commit(management);
        } catch (RepositoryException | IndexException e) {
            LOG.error("Failed to update indexes for changed typedefs", e);
            attemptRollback(changedTypeDefs, management);
        }

    }

    public Set<String> getVertexIndexKeys() {
        if (recomputeIndexedKeys) {
            AtlasGraphManagement management = null;

            try {
                management = provider.get().getManagementSystem();

                if (management != null) {
                    AtlasGraphIndex vertexIndex = management.getGraphIndex(VERTEX_INDEX);

                    if (vertexIndex != null) {
                        recomputeIndexedKeys = false;

                        Set<String> indexKeys = new HashSet<>();

                        for (AtlasPropertyKey fieldKey : vertexIndex.getFieldKeys()) {
                            indexKeys.add(fieldKey.getName());
                        }

                        vertexIndexKeys = indexKeys;
                    }

                    management.commit();
                }
            } catch (Exception excp) {
                LOG.error("getVertexIndexKeys(): failed to get indexedKeys from graph", excp);

                if (management != null) {
                    try {
                        management.rollback();
                    } catch (Exception e) {
                        LOG.error("getVertexIndexKeys(): rollback failed", e);
                    }
                }
            }
        }

        return vertexIndexKeys;
    }

    /**
     * Initializes the indices for the graph - create indices for Global AtlasVertex Keys
     */
    private void initialize() throws RepositoryException, IndexException {
        initialize(provider.get());
    }
    
    /**
     * Initializes the indices for the graph - create indices for Global AtlasVertex and AtlasEdge Keys
     */
    private void initialize(AtlasGraph graph) throws RepositoryException, IndexException {
        AtlasGraphManagement management = graph.getManagementSystem();

        try {
            LOG.info("Creating indexes for graph.");

            if (management.getGraphIndex(VERTEX_INDEX) == null) {
                management.createVertexMixedIndex(VERTEX_INDEX, BACKING_INDEX, Collections.emptyList());

                LOG.info("Created index : {}", VERTEX_INDEX);
            }

            if (management.getGraphIndex(EDGE_INDEX) == null) {
                management.createEdgeMixedIndex(EDGE_INDEX, BACKING_INDEX, Collections.emptyList());

                LOG.info("Created index : {}", EDGE_INDEX);
            }

            if (management.getGraphIndex(FULLTEXT_INDEX) == null) {
                management.createFullTextMixedIndex(FULLTEXT_INDEX, BACKING_INDEX, Collections.emptyList());

                LOG.info("Created index : {}", FULLTEXT_INDEX);
            }

            // create vertex indexes
            createVertexIndex(management, GUID_PROPERTY_KEY, UniqueKind.GLOBAL_UNIQUE, String.class, SINGLE, true, false);
            createVertexIndex(management, TYPENAME_PROPERTY_KEY, UniqueKind.GLOBAL_UNIQUE, String.class, SINGLE, true, false);
            createVertexIndex(management, TYPESERVICETYPE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false);
            createVertexIndex(management, VERTEX_TYPE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false);
            createVertexIndex(management, VERTEX_ID_IN_IMPORT_KEY, UniqueKind.NONE, Long.class, SINGLE, true, false);

            createVertexIndex(management, ENTITY_TYPE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false);
            createVertexIndex(management, SUPER_TYPES_PROPERTY_KEY, UniqueKind.NONE, String.class, SET, true, false);
            createVertexIndex(management, TIMESTAMP_PROPERTY_KEY, UniqueKind.NONE, Long.class, SINGLE, false, false);
            createVertexIndex(management, MODIFICATION_TIMESTAMP_PROPERTY_KEY, UniqueKind.NONE, Long.class, SINGLE, false, false);
            createVertexIndex(management, STATE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, false, false);
            createVertexIndex(management, CREATED_BY_KEY, UniqueKind.NONE, String.class, SINGLE, false, false);
            createVertexIndex(management, MODIFIED_BY_KEY, UniqueKind.NONE, String.class, SINGLE, false, false);
            createVertexIndex(management, TRAIT_NAMES_PROPERTY_KEY, UniqueKind.NONE, String.class, SET, true, true);
            createVertexIndex(management, PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, UniqueKind.NONE, String.class, LIST, true, true);

            // create vertex-centric index
            createVertexCentricIndex(management, CLASSIFICATION_LABEL, AtlasEdgeDirection.BOTH, CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, String.class, SINGLE);
            createVertexCentricIndex(management, CLASSIFICATION_LABEL, AtlasEdgeDirection.BOTH, CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, Boolean.class, SINGLE);
            createVertexCentricIndex(management, CLASSIFICATION_LABEL, AtlasEdgeDirection.BOTH, Arrays.asList(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY));

            // create edge indexes
            createEdgeIndex(management, RELATIONSHIP_GUID_PROPERTY_KEY, String.class, SINGLE, true);
            createEdgeIndex(management, EDGE_ID_IN_IMPORT_KEY, String.class, SINGLE, true);

            // create fulltext indexes
            createFullTextIndex(management, ENTITY_TEXT_PROPERTY_KEY, String.class, SINGLE);

            commit(management);

            LOG.info("Index creation for global keys complete.");
        } catch (Throwable t) {
            rollback(management);
            throw new RepositoryException(t);
        }
    }

    private void addIndexForType(AtlasGraphManagement management, AtlasBaseTypeDef typeDef) {
        if (typeDef instanceof AtlasEnumDef) {
            // Only handle complex types like Struct, Classification and Entity
            return;
        }
        if (typeDef instanceof AtlasStructDef) {
            AtlasStructDef structDef = (AtlasStructDef) typeDef;
            List<AtlasAttributeDef> attributeDefs = structDef.getAttributeDefs();
            if (CollectionUtils.isNotEmpty(attributeDefs)) {
                for (AtlasAttributeDef attributeDef : attributeDefs) {
                    createIndexForAttribute(management, typeDef.getName(), attributeDef);
                }
            }
        } else if (!AtlasTypeUtil.isBuiltInType(typeDef.getName())){
            throw new IllegalArgumentException("bad data type" + typeDef.getName());
        }
    }

    private void deleteIndexForType(AtlasGraphManagement management, AtlasBaseTypeDef typeDef) {
        Preconditions.checkNotNull(typeDef, "Cannot process null typedef");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting indexes for type {}", typeDef.getName());
        }

        if (typeDef instanceof AtlasStructDef) {
            AtlasStructDef          structDef     = (AtlasStructDef) typeDef;
            List<AtlasAttributeDef> attributeDefs = structDef.getAttributeDefs();

            if (CollectionUtils.isNotEmpty(attributeDefs)) {
                for (AtlasAttributeDef attributeDef : attributeDefs) {
                    deleteIndexForAttribute(management, typeDef.getName(), attributeDef);
                }
            }
        }

        LOG.info("Completed deleting indexes for type {}", typeDef.getName());
    }

    private void createIndexForAttribute(AtlasGraphManagement management, String typeName, AtlasAttributeDef attributeDef) {
        final String     propertyName   = AtlasGraphUtilsV2.encodePropertyKey(typeName + "." + attributeDef.getName());
        AtlasCardinality cardinality    = toAtlasCardinality(attributeDef.getCardinality());
        boolean          isUnique       = attributeDef.getIsUnique();
        boolean          isIndexable    = attributeDef.getIsIndexable();
        String           attribTypeName = attributeDef.getTypeName();
        boolean          isBuiltInType  = AtlasTypeUtil.isBuiltInType(attribTypeName);
        boolean          isArrayType    = isArrayType(attribTypeName);
        boolean          isMapType      = isMapType(attribTypeName);
        final String     uniqPropName   = isUnique ? AtlasGraphUtilsV2.encodePropertyKey(typeName + "." + UNIQUE_ATTRIBUTE_SHADE_PROPERTY_PREFIX + attributeDef.getName()) : null;


        try {
            AtlasType atlasType     = typeRegistry.getType(typeName);
            AtlasType attributeType = typeRegistry.getType(attribTypeName);

            if (isClassificationType(attributeType)) {
                LOG.warn("Ignoring non-indexable attribute {}", attribTypeName);
            }

            if (isArrayType) {
                createLabelIfNeeded(management, propertyName, attribTypeName);

                AtlasArrayType arrayType   = (AtlasArrayType) attributeType;
                boolean        isReference = isReference(arrayType.getElementType());

                if (!isReference) {
                    createPropertyKey(management, propertyName, ArrayList.class, SINGLE);
                }
            }

            if (isMapType) {
                createLabelIfNeeded(management, propertyName, attribTypeName);

                AtlasMapType mapType     = (AtlasMapType) attributeType;
                boolean      isReference = isReference(mapType.getValueType());

                if (!isReference) {
                    createPropertyKey(management, propertyName, HashMap.class, SINGLE);
                }
            }

            if (isEntityType(attributeType)) {
                createEdgeLabel(management, propertyName);

            } else if (isBuiltInType) {
                if (isRelationshipType(atlasType)) {
                    createEdgeIndex(management, propertyName, getPrimitiveClass(attribTypeName), cardinality, false);
                } else {
                    createVertexIndex(management, propertyName, UniqueKind.NONE, getPrimitiveClass(attribTypeName), cardinality, isIndexable, false);

                    if (uniqPropName != null) {
                        createVertexIndex(management, uniqPropName, UniqueKind.PER_TYPE_UNIQUE, getPrimitiveClass(attribTypeName), cardinality, isIndexable, true);
                    }
                }
            } else if (isEnumType(attributeType)) {
                if (isRelationshipType(atlasType)) {
                    createEdgeIndex(management, propertyName, String.class, cardinality, false);
                } else {
                    createVertexIndex(management, propertyName, UniqueKind.NONE, String.class, cardinality, isIndexable, false);

                    if (uniqPropName != null) {
                        createVertexIndex(management, uniqPropName, UniqueKind.PER_TYPE_UNIQUE, String.class, cardinality, isIndexable, true);
                    }
                }
            } else if (isStructType(attributeType)) {
                AtlasStructDef structDef = typeRegistry.getStructDefByName(attribTypeName);
                updateIndexForTypeDef(management, structDef);
            }
        } catch (AtlasBaseException e) {
            LOG.error("No type exists for {}", attribTypeName, e);
        }
    }

    private void deleteIndexForAttribute(AtlasGraphManagement management, String typeName, AtlasAttributeDef attributeDef) {
        final String propertyName = AtlasGraphUtilsV2.encodePropertyKey(typeName + "." + attributeDef.getName());

        try {
            if (management.containsPropertyKey(propertyName)) {
                LOG.info("Deleting propertyKey {}, for attribute {}.{}", propertyName, typeName, attributeDef.getName());

                management.deletePropertyKey(propertyName);
            }
        } catch (Exception excp) {
            LOG.warn("Failed to delete propertyKey {}, for attribute {}.{}", propertyName, typeName, attributeDef.getName());
        }
    }

    private void createLabelIfNeeded(final AtlasGraphManagement management, final String propertyName, final String attribTypeName) {
        // If any of the referenced typename is of type Entity or Struct then the edge label needs to be created
        for (String typeName : AtlasTypeUtil.getReferencedTypeNames(attribTypeName)) {
            if (typeRegistry.getEntityDefByName(typeName) != null || typeRegistry.getStructDefByName(typeName) != null) {
                // Create the edge label upfront to avoid running into concurrent call issue (ATLAS-2092)
                createEdgeLabel(management, propertyName);
            }
        }
    }

    private boolean isEntityType(AtlasType type) {
        return type instanceof AtlasEntityType;
    }

    private boolean isClassificationType(AtlasType type) {
        return type instanceof AtlasClassificationType;
    }

    private boolean isEnumType(AtlasType type) {
        return type instanceof AtlasEnumType;
    }

    private boolean isStructType(AtlasType type) {
        return type instanceof AtlasStructType;
    }

    private boolean isRelationshipType(AtlasType type) {
        return type instanceof AtlasRelationshipType;
    }

    private Class getPrimitiveClass(String attribTypeName) {
        String attributeTypeName = attribTypeName.toLowerCase();

        switch (attributeTypeName) {
            case ATLAS_TYPE_BOOLEAN:
                return Boolean.class;
            case ATLAS_TYPE_BYTE:
                return Byte.class;
            case ATLAS_TYPE_SHORT:
                return Short.class;
            case ATLAS_TYPE_INT:
                return Integer.class;
            case ATLAS_TYPE_LONG:
            case ATLAS_TYPE_DATE:
                return Long.class;
            case ATLAS_TYPE_FLOAT:
                return Float.class;
            case ATLAS_TYPE_DOUBLE:
                return Double.class;
            case ATLAS_TYPE_BIGINTEGER:
                return BigInteger.class;
            case ATLAS_TYPE_BIGDECIMAL:
                return BigDecimal.class;
            case ATLAS_TYPE_STRING:
                return String.class;
        }

        throw new IllegalArgumentException(String.format("Unknown primitive typename %s", attribTypeName));
    }

    private AtlasCardinality toAtlasCardinality(AtlasAttributeDef.Cardinality cardinality) {
        switch (cardinality) {
            case SINGLE:
                return SINGLE;
            case LIST:
                return LIST;
            case SET:
                return SET;
        }
        // Should never reach this point
        throw new IllegalArgumentException(String.format("Bad cardinality %s", cardinality));
    }

    private void createEdgeLabel(final AtlasGraphManagement management, final String propertyName) {
        // Create the edge label upfront to avoid running into concurrent call issue (ATLAS-2092)
        // ATLAS-2092 addresses this problem by creating the edge label upfront while type creation
        // which resolves the race condition during the entity creation

        String label = Constants.INTERNAL_PROPERTY_KEY_PREFIX + propertyName;

        org.apache.atlas.repository.graphdb.AtlasEdgeLabel edgeLabel = management.getEdgeLabel(label);

        if (edgeLabel == null) {
            management.makeEdgeLabel(label);

            LOG.info("Created edge label {} ", label);
        }
    }

    private AtlasPropertyKey createPropertyKey(AtlasGraphManagement management, String propertyName, Class propertyClass, AtlasCardinality cardinality) {
        AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

        if (propertyKey == null) {
            propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);
        }

        return propertyKey;
    }

    private void createVertexIndex(AtlasGraphManagement management, String propertyName, UniqueKind uniqueKind, Class propertyClass,
                                   AtlasCardinality cardinality, boolean createCompositeIndex, boolean createCompositeIndexWithTypeAndSuperTypes) {
        if (propertyName != null) {
            AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

            if (propertyKey == null) {
                propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);

                if (isIndexApplicable(propertyClass, cardinality)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Creating backing index for vertex property {} of type {} ", propertyName, propertyClass.getName());
                    }

                    management.addMixedIndex(VERTEX_INDEX, propertyKey);

                    LOG.info("Created backing index for vertex property {} of type {} ", propertyName, propertyClass.getName());
                }
            }

            if (propertyKey != null) {
                if (createCompositeIndex || uniqueKind == UniqueKind.GLOBAL_UNIQUE || uniqueKind == UniqueKind.PER_TYPE_UNIQUE) {
                    createVertexCompositeIndex(management, propertyClass, propertyKey, uniqueKind == UniqueKind.GLOBAL_UNIQUE);
                }

                if (createCompositeIndexWithTypeAndSuperTypes) {
                    createVertexCompositeIndexWithTypeName(management, propertyClass, propertyKey, uniqueKind == UniqueKind.PER_TYPE_UNIQUE);
                    createVertexCompositeIndexWithSuperTypeName(management, propertyClass, propertyKey);
                }
            } else {
                LOG.warn("Index not created for {}: propertyKey is null", propertyName);
            }
        }
    }

    private void createVertexCentricIndex(AtlasGraphManagement management, String edgeLabel, AtlasEdgeDirection edgeDirection,
                                          String propertyName, Class propertyClass, AtlasCardinality cardinality) {
        AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

        if (propertyKey == null) {
            propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating vertex-centric index for edge label: {} direction: {} for property: {} of type: {} ",
                        edgeLabel, edgeDirection.name(), propertyName, propertyClass.getName());
        }

        final String indexName = edgeLabel + propertyKey.getName();

        if (!management.edgeIndexExist(edgeLabel, indexName)) {
            management.createEdgeIndex(edgeLabel, indexName, edgeDirection, Collections.singletonList(propertyKey));

            LOG.info("Created vertex-centric index for edge label: {} direction: {} for property: {} of type: {}",
                    edgeLabel, edgeDirection.name(), propertyName, propertyClass.getName());
        }
    }

    private void createVertexCentricIndex(AtlasGraphManagement management, String edgeLabel, AtlasEdgeDirection edgeDirection, List<String> propertyNames) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating vertex-centric index for edge label: {} direction: {} for properties: {}",
                    edgeLabel, edgeDirection.name(), propertyNames);
        }

        String                 indexName    = edgeLabel;
        List<AtlasPropertyKey> propertyKeys = new ArrayList<>();

        for (String propertyName : propertyNames) {
            AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

            if (propertyKey != null) {
                propertyKeys.add(propertyKey);
                indexName = indexName + propertyKey.getName();
            }
        }

        if (!management.edgeIndexExist(edgeLabel, indexName) && CollectionUtils.isNotEmpty(propertyKeys)) {
            management.createEdgeIndex(edgeLabel, indexName, edgeDirection, propertyKeys);

            LOG.info("Created vertex-centric index for edge label: {} direction: {} for properties: {}", edgeLabel, edgeDirection.name(), propertyNames);
        }
    }


    private void createEdgeIndex(AtlasGraphManagement management, String propertyName, Class propertyClass,
                                 AtlasCardinality cardinality, boolean createCompositeIndex) {
        if (propertyName != null) {
            AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

            if (propertyKey == null) {
                propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);

                if (isIndexApplicable(propertyClass, cardinality)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Creating backing index for edge property {} of type {} ", propertyName, propertyClass.getName());
                    }

                    management.addMixedIndex(EDGE_INDEX, propertyKey);

                    LOG.info("Created backing index for edge property {} of type {} ", propertyName, propertyClass.getName());
                }
            }

            if (propertyKey != null) {
                if (createCompositeIndex) {
                    createEdgeCompositeIndex(management, propertyClass, propertyKey);
                }
            } else {
                LOG.warn("Index not created for {}: propertyKey is null", propertyName);
            }
        }
    }

    private AtlasPropertyKey createFullTextIndex(AtlasGraphManagement management, String propertyName, Class propertyClass,
                                                 AtlasCardinality cardinality) {
        AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

        if (propertyKey == null) {
            propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);

            if (isIndexApplicable(propertyClass, cardinality)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating backing index for vertex property {} of type {} ", propertyName, propertyClass.getName());
                }

                management.addMixedIndex(FULLTEXT_INDEX, propertyKey);

                LOG.info("Created backing index for vertex property {} of type {} ", propertyName, propertyClass.getName());
            }

            LOG.info("Created index {}", FULLTEXT_INDEX);
        }

        return propertyKey;
    }
    
    private void createVertexCompositeIndex(AtlasGraphManagement management, Class propertyClass, AtlasPropertyKey propertyKey,
                                            boolean enforceUniqueness) {
        String propertyName = propertyKey.getName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating composite index for property {} of type {}; isUnique={} ", propertyName, propertyClass.getName(), enforceUniqueness);
        }

        AtlasGraphIndex existingIndex = management.getGraphIndex(propertyName);

        if (existingIndex == null) {
            management.createVertexCompositeIndex(propertyName, enforceUniqueness, Collections.singletonList(propertyKey));

            LOG.info("Created composite index for property {} of type {}; isUnique={} ", propertyName, propertyClass.getName(), enforceUniqueness);
        }
    }

    private void createEdgeCompositeIndex(AtlasGraphManagement management, Class propertyClass, AtlasPropertyKey propertyKey) {
        String propertyName = propertyKey.getName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating composite index for property {} of type {}", propertyName, propertyClass.getName());
        }

        AtlasGraphIndex existingIndex = management.getGraphIndex(propertyName);

        if (existingIndex == null) {
            management.createEdgeCompositeIndex(propertyName, false, Collections.singletonList(propertyKey));

            LOG.info("Created composite index for property {} of type {}", propertyName, propertyClass.getName());
        }
    }

    private void createVertexCompositeIndexWithTypeName(AtlasGraphManagement management, Class propertyClass, AtlasPropertyKey propertyKey, boolean isUnique) {
        createVertexCompositeIndexWithSystemProperty(management, propertyClass, propertyKey, ENTITY_TYPE_PROPERTY_KEY, SINGLE, isUnique);
    }

    private void createVertexCompositeIndexWithSuperTypeName(AtlasGraphManagement management, Class propertyClass, AtlasPropertyKey propertyKey) {
        createVertexCompositeIndexWithSystemProperty(management, propertyClass, propertyKey, SUPER_TYPES_PROPERTY_KEY, SET, false);
    }

    private void createVertexCompositeIndexWithSystemProperty(AtlasGraphManagement management, Class propertyClass, AtlasPropertyKey propertyKey,
                                                              final String systemPropertyKey, AtlasCardinality cardinality, boolean isUnique) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating composite index for property {} of type {} and {}", propertyKey.getName(), propertyClass.getName(),  systemPropertyKey);
        }

        AtlasPropertyKey typePropertyKey = management.getPropertyKey(systemPropertyKey);
        if (typePropertyKey == null) {
            typePropertyKey = management.makePropertyKey(systemPropertyKey, String.class, cardinality);
        }

        final String indexName = propertyKey.getName() + systemPropertyKey;
        AtlasGraphIndex existingIndex = management.getGraphIndex(indexName);

        if (existingIndex == null) {
            List<AtlasPropertyKey> keys = new ArrayList<>(2);
            keys.add(typePropertyKey);
            keys.add(propertyKey);
            management.createVertexCompositeIndex(indexName, isUnique, keys);

            LOG.info("Created composite index for property {} of type {} and {}", propertyKey.getName(), propertyClass.getName(), systemPropertyKey);
        }
    }

    private boolean isIndexApplicable(Class propertyClass, AtlasCardinality cardinality) {
        return !(INDEX_EXCLUSION_CLASSES.contains(propertyClass) || cardinality.isMany());
    }
    
    private void commit(AtlasGraphManagement management) throws IndexException {
        try {
            management.commit();

            recomputeIndexedKeys = true;
        } catch (Exception e) {
            LOG.error("Index commit failed", e);
            throw new IndexException("Index commit failed ", e);
        }
    }

    private void rollback(AtlasGraphManagement management) throws IndexException {
        try {
            management.rollback();

            recomputeIndexedKeys = true;
        } catch (Exception e) {
            LOG.error("Index rollback failed ", e);
            throw new IndexException("Index rollback failed ", e);
        }
    }

    private void attemptRollback(ChangedTypeDefs changedTypeDefs, AtlasGraphManagement management)
            throws AtlasBaseException {
        if (null != management) {
            try {
                rollback(management);
            } catch (IndexException e) {
                LOG.error("Index rollback has failed", e);
                throw new AtlasBaseException(AtlasErrorCode.INDEX_ROLLBACK_FAILED, e,
                        changedTypeDefs.toString());
            }
        }
    }

    private void updateIndexForTypeDef(AtlasGraphManagement management, AtlasBaseTypeDef typeDef) {
        Preconditions.checkNotNull(typeDef, "Cannot index on null typedefs");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating indexes for type name={}, definition={}", typeDef.getName(), typeDef.getClass());
        }
        addIndexForType(management, typeDef);
        LOG.info("Index creation for type {} complete", typeDef.getName());
    }
}
