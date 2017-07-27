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
import org.apache.atlas.repository.IndexCreationException;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasEnumType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;


/**
 * Adds index for properties of a given type when its added before any instances are added.
 */
@Component
public class GraphBackedSearchIndexer implements SearchIndexer, ActiveStateChangeHandler,
        TypeDefChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedSearchIndexer.class);
    
    private static final List<Class> VERTEX_INDEX_EXCLUSIONS = new ArrayList() {
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

    @Inject
    public GraphBackedSearchIndexer(AtlasTypeRegistry typeRegistry) throws AtlasException {
        this(new AtlasGraphProvider(), ApplicationProperties.get(), typeRegistry);
    }

    @VisibleForTesting
    GraphBackedSearchIndexer( IAtlasGraphProvider provider, Configuration configuration, AtlasTypeRegistry typeRegistry)
            throws IndexException, RepositoryException {
        this.provider = provider;
        this.typeRegistry = typeRegistry;
        if (!HAConfiguration.isHAEnabled(configuration)) {
            initialize(provider.get());
        }
    }

    /**
     * Initializes the indices for the graph - create indices for Global AtlasVertex Keys
     */
    private void initialize() throws RepositoryException, IndexException {
        
        initialize(provider.get());    
    }
    
    /**
     * Initializes the indices for the graph - create indices for Global AtlasVertex Keys
     */
    private void initialize(AtlasGraph graph) throws RepositoryException, IndexException {
        AtlasGraphManagement management = graph.getManagementSystem();

        try {
            if (management.containsPropertyKey(Constants.VERTEX_TYPE_PROPERTY_KEY)) {
                LOG.info("Global indexes already exist for graph");
                management.commit();

                return;
            }

            /* This is called only once, which is the first time Atlas types are made indexable .*/
            LOG.info("Indexes do not exist, Creating indexes for graph.");

            
            management.createVertexIndex(Constants.VERTEX_INDEX, Constants.BACKING_INDEX, Collections.<AtlasPropertyKey>emptyList());              
            management.createEdgeIndex(Constants.EDGE_INDEX, Constants.BACKING_INDEX);

            // create a composite index for guid as its unique
            createIndexes(management, Constants.GUID_PROPERTY_KEY, String.class, true,
                    AtlasCardinality.SINGLE, true, true);

            // Add creation_timestamp property to Vertex Index (mixed index)
            createIndexes(management, Constants.TIMESTAMP_PROPERTY_KEY, Long.class, false, AtlasCardinality.SINGLE, false, false);

            // Add modification_timestamp property to Vertex Index (mixed index)
            createIndexes(management, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class, false,
                    AtlasCardinality.SINGLE, false, false);


            // create a mixed index for entity state. Set systemProperty flag deliberately to false
            // so that it doesnt create a composite index which has issues with
            // titan 0.5.4 - Refer https://groups.google.com/forum/#!searchin/aureliusgraphs/hemanth/aureliusgraphs/bx7T843mzXU/fjAsclx7GAAJ
            createIndexes(management, Constants.STATE_PROPERTY_KEY, String.class, false, AtlasCardinality.SINGLE, false, false);

            // Create a composite and mixed index for created by property
            createIndexes(management, Constants.CREATED_BY_KEY, String.class, false,
                    AtlasCardinality.SINGLE, true, true);

            // Create a composite and mixed index for modified by property
            createIndexes(management, Constants.MODIFIED_BY_KEY, String.class, false,
                    AtlasCardinality.SINGLE, true, true);

            // create a composite and mixed index for type since it can be combined with other keys
            createIndexes(management, Constants.ENTITY_TYPE_PROPERTY_KEY, String.class, false, AtlasCardinality.SINGLE,
                    true, true);

            // create a composite and mixed index for type since it can be combined with other keys
            createIndexes(management, Constants.SUPER_TYPES_PROPERTY_KEY, String.class, false, AtlasCardinality.SET,
                    true, true);

            // create a composite and mixed index for traitNames since it can be combined with other
            // keys. Traits must be a set and not a list.
            createIndexes(management, Constants.TRAIT_NAMES_PROPERTY_KEY, String.class, false, AtlasCardinality.SET,
                    true, true);

            // Index for full text search
            createFullTextIndex(management);

            //Indexes for graph backed type system store
            createTypeStoreIndexes(management);
      
            
            commit(management);
            LOG.info("Index creation for global keys complete.");
        } catch (Throwable t) {
            rollback(management);
            throw new RepositoryException(t);
        }
    }

    private void createFullTextIndex(AtlasGraphManagement management) {
        AtlasPropertyKey fullText =
                management.makePropertyKey(Constants.ENTITY_TEXT_PROPERTY_KEY, String.class, AtlasCardinality.SINGLE);

        management.createFullTextIndex(Constants.FULLTEXT_INDEX, fullText, Constants.BACKING_INDEX);

    }

    private void createTypeStoreIndexes(AtlasGraphManagement management) {
        //Create unique index on typeName
        createIndexes(management, Constants.TYPENAME_PROPERTY_KEY, String.class, true, AtlasCardinality.SINGLE,
                true, true);

        //create index on vertex type
        createIndexes(management, Constants.VERTEX_TYPE_PROPERTY_KEY, String.class, false, AtlasCardinality.SINGLE,
                true, true);
    }

    /**
     * This is upon adding a new type to Store.
     *
     * @param dataTypes data type
     * @throws AtlasException
     */
    @Override
    public void onAdd(Collection<? extends IDataType> dataTypes) throws AtlasException {
        AtlasGraphManagement management = provider.get().getManagementSystem();
               
        for (IDataType dataType : dataTypes) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Creating indexes for type name={}, definition={}", dataType.getName(), dataType.getClass());
            }

            try {
                addIndexForType(management, dataType);
                LOG.info("Index creation for type {} complete", dataType.getName());
            } catch (Throwable throwable) {
                LOG.error("Error creating index for type {}", dataType, throwable);
                //Rollback indexes if any failure
                rollback(management);
                throw new IndexCreationException("Error while creating index for type " + dataType, throwable);
            }
        }

        //Commit indexes
        commit(management);
    }

    @Override
    public void onChange(Collection<? extends IDataType> dataTypes) throws AtlasException {
        onAdd(dataTypes);
    }

    public Set<String> getVertexIndexKeys() {
        if (recomputeIndexedKeys) {
            AtlasGraphManagement management = null;

            try {
                management = provider.get().getManagementSystem();
            } catch (RepositoryException excp) {
                LOG.error("failed to get indexedKeys from graph", excp);
            }

            if (management != null) {
                AtlasGraphIndex vertexIndex = management.getGraphIndex(Constants.VERTEX_INDEX);

                if (vertexIndex != null) {
                    recomputeIndexedKeys = false;

                    Set<String> indexKeys = new HashSet<>();

                    for (AtlasPropertyKey fieldKey : vertexIndex.getFieldKeys()) {
                        indexKeys.add(fieldKey.getName());
                    }

                    vertexIndexKeys = indexKeys;
                }
            }
        }

        return vertexIndexKeys;
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

    private void createIndexForAttribute(AtlasGraphManagement management, String typeName,
                                         AtlasAttributeDef attributeDef) {
        final String propertyName = GraphHelper.encodePropertyKey(typeName + "." + attributeDef.getName());
        AtlasCardinality cardinality = toAtlasCardinality(attributeDef.getCardinality());
        boolean isUnique = attributeDef.getIsUnique();
        boolean isIndexable = attributeDef.getIsIndexable();
        String attribTypeName = attributeDef.getTypeName();
        boolean isBuiltInType = AtlasTypeUtil.isBuiltInType(attribTypeName);
        boolean isArrayType = AtlasTypeUtil.isArrayType(attribTypeName);
        boolean isMapType = AtlasTypeUtil.isMapType(attribTypeName);


        try {
            AtlasType atlasType = typeRegistry.getType(attribTypeName);

            if (isMapType || isArrayType || isClassificationType(atlasType) || isEntityType(atlasType)) {
                LOG.warn("Ignoring non-indexable attribute {}", attribTypeName);
            } else if (isBuiltInType) {
                createIndexes(management, propertyName, getPrimitiveClass(attribTypeName), isUnique, cardinality, false, isIndexable);
            } else if (isEnumType(atlasType)) {
                createIndexes(management, propertyName, String.class, isUnique, cardinality, false, isIndexable);
            } else if (isStructType(atlasType)) {
                AtlasStructDef structDef = typeRegistry.getStructDefByName(attribTypeName);
                updateIndexForTypeDef(management, structDef);
            }
        } catch (AtlasBaseException e) {
            LOG.error("No type exists for {}", attribTypeName, e);
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

    private Class getPrimitiveClass(String attribTypeName) {
        switch (attribTypeName.toLowerCase()) {
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
                return AtlasCardinality.SINGLE;
            case LIST:
                return AtlasCardinality.LIST;
            case SET:
                return AtlasCardinality.SET;
        }
        // Should never reach this point
        throw new IllegalArgumentException(String.format("Bad cardinality %s", cardinality));
    }

    private void addIndexForType(AtlasGraphManagement management, IDataType dataType) {
        switch (dataType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
        case ARRAY:
        case MAP:
            // do nothing since these are only attributes
            // and not types like structs, traits or classes
            break;

        case STRUCT:
            StructType structType = (StructType) dataType;
            createIndexForFields(management, structType, structType.fieldMapping().fields);
            break;

        case TRAIT:
            TraitType traitType = (TraitType) dataType;
            createIndexForFields(management, traitType, traitType.fieldMapping().fields);
            break;

        case CLASS:
            ClassType classType = (ClassType) dataType;
            createIndexForFields(management, classType, classType.fieldMapping().fields);
            break;

        default:
            throw new IllegalArgumentException("bad data type" + dataType);
        }
    }

    private void createIndexForFields(AtlasGraphManagement management, IDataType dataType, Map<String, AttributeInfo> fields) {
        for (AttributeInfo field : fields.values()) {
            createIndexForAttribute(management, dataType.getName(), field);
        }
    }

    private void createIndexForAttribute(AtlasGraphManagement management, String typeName, AttributeInfo field) {
        final String propertyName = GraphHelper.encodePropertyKey(typeName + "." + field.name);
        switch (field.dataType().getTypeCategory()) {
        case PRIMITIVE:
            AtlasCardinality cardinality = getCardinality(field.multiplicity);
            createIndexes(management, propertyName, getPrimitiveClass(field.dataType()), field.isUnique,
                    cardinality, false, field.isIndexable);
            break;

        case ENUM:
            cardinality = getCardinality(field.multiplicity);
            createIndexes(management, propertyName, String.class, field.isUnique, cardinality, false, field.isIndexable);
            break;

        case ARRAY:
        case MAP:
            // todo - how do we overcome this limitation?
            // IGNORE: Can only index single-valued property keys on vertices in Mixed Index
            break;

        case STRUCT:
            StructType structType = (StructType) field.dataType();
            createIndexForFields(management, structType, structType.fieldMapping().fields);
            break;

        case TRAIT:
            // do nothing since this is NOT contained in other types
            break;

        case CLASS:
            // this is only A reference, index the attribute for edge
            // Commenting this out since we do not need an index for edge here
            //createEdgeMixedIndex(propertyName);
            break;

        default:
            throw new IllegalArgumentException("bad data type" + field.dataType().getName());
        }
    }

    private Class getPrimitiveClass(IDataType dataType) {
        if (dataType == DataTypes.STRING_TYPE) {
            return String.class;
        } else if (dataType == DataTypes.SHORT_TYPE) {
            return Short.class;
        } else if (dataType == DataTypes.INT_TYPE) {
            return Integer.class;
        } else if (dataType == DataTypes.BIGINTEGER_TYPE) {
            return BigInteger.class;
        } else if (dataType == DataTypes.BOOLEAN_TYPE) {
            return Boolean.class;
        } else if (dataType == DataTypes.BYTE_TYPE) {
            return Byte.class;
        } else if (dataType == DataTypes.LONG_TYPE) {
            return Long.class;
        } else if (dataType == DataTypes.FLOAT_TYPE) {
            return Float.class;
        } else if (dataType == DataTypes.DOUBLE_TYPE) {
            return Double.class;
        } else if (dataType == DataTypes.BIGDECIMAL_TYPE) {
            return BigDecimal.class;
        } else if (dataType == DataTypes.DATE_TYPE) {
            //Indexing with date converted to long as of now since Titan is yet to add support for Date type with mixed indexes
            return Long.class;
        }


        throw new IllegalArgumentException("unknown data type " + dataType);
    }
  

    private AtlasCardinality getCardinality(Multiplicity multiplicity) {
        if (multiplicity == Multiplicity.OPTIONAL || multiplicity == Multiplicity.REQUIRED) {
            return AtlasCardinality.SINGLE;
        } else if (multiplicity == Multiplicity.COLLECTION) {
            return AtlasCardinality.LIST;
        } else if (multiplicity == Multiplicity.SET) {
            return AtlasCardinality.SET;
        }

        // todo - default to LIST as this is the most forgiving
        return AtlasCardinality.LIST;
    }
    
    private AtlasPropertyKey createIndexes(AtlasGraphManagement management, String propertyName, Class propertyClass,
            boolean isUnique, AtlasCardinality cardinality, boolean createCompositeForAttribute,
            boolean createCompositeWithTypeandSuperTypes) {

        AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);
        if (propertyKey == null) {
            propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);

            updateVertexIndex(management, propertyName, propertyClass, cardinality, propertyKey);

        }

        if (createCompositeForAttribute) {
            createExactMatchIndex(management, propertyClass, propertyKey, isUnique);
        } else if (createCompositeWithTypeandSuperTypes) {
            // Index with typename since typename+property key queries need to
            // speed up
            createExactMatchIndexWithTypeName(management, propertyClass, propertyKey);
            createExactMatchIndexWithSuperTypeName(management, propertyClass, propertyKey);
        }
        return propertyKey;
    }
    
    private void createExactMatchIndex(AtlasGraphManagement management, Class propertyClass,
            AtlasPropertyKey propertyKey, boolean enforceUniqueness) {
        
        String propertyName = propertyKey.getName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating composite index for property {} of type {}; isUnique={} ", propertyName, propertyClass.getName(), enforceUniqueness);
        }

        AtlasGraphIndex existingIndex = management.getGraphIndex(propertyName);
        if (existingIndex == null) {
            management.createExactMatchIndex(propertyName, enforceUniqueness, Collections.singletonList(propertyKey));
        }

        LOG.info("Created composite index for property {} of type {}; isUnique={} ", propertyName, propertyClass.getName(), enforceUniqueness);
    }
    

    private void createExactMatchIndexWithTypeName(AtlasGraphManagement management,
            Class propertyClass, AtlasPropertyKey propertyKey) {
        createExactMatchIndexWithSystemProperty(management, propertyClass, propertyKey,
                Constants.ENTITY_TYPE_PROPERTY_KEY, AtlasCardinality.SINGLE);
    }

    private void createExactMatchIndexWithSuperTypeName(AtlasGraphManagement management,
            Class propertyClass, AtlasPropertyKey propertyKey) {
        createExactMatchIndexWithSystemProperty(management, propertyClass, propertyKey,
                Constants.SUPER_TYPES_PROPERTY_KEY, AtlasCardinality.SET);
    }

    private void createExactMatchIndexWithSystemProperty(AtlasGraphManagement management,
            Class propertyClass, AtlasPropertyKey propertyKey, final String systemPropertyKey,
            AtlasCardinality cardinality) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating composite index for property {} of type {} and {}", propertyKey.getName(), propertyClass.getName(),
                    systemPropertyKey);
        }

        AtlasPropertyKey typePropertyKey = management.getPropertyKey(systemPropertyKey);
        if (typePropertyKey == null) {
            typePropertyKey = management.makePropertyKey(systemPropertyKey, String.class, cardinality);
        }

        final String indexName = propertyKey.getName() + systemPropertyKey;
        AtlasGraphIndex existingIndex = management.getGraphIndex(indexName);

        if (existingIndex == null) {
            
            List<AtlasPropertyKey> keys = new ArrayList<>(2);
            keys.add(propertyKey);
            keys.add(typePropertyKey);
            management.createExactMatchIndex(indexName, false, keys);

            LOG.info("Created composite index for property {} of type {} and {}", propertyKey.getName(), propertyClass.getName(),
                    systemPropertyKey);
        }
    }

    private void updateVertexIndex(AtlasGraphManagement management, String propertyName, Class propertyClass,
            AtlasCardinality cardinality, AtlasPropertyKey propertyKey) {
        if (checkIfVertexIndexApplicable(propertyClass, cardinality)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Creating backing index for property {} of type {} ", propertyName, propertyClass.getName());
            }

            // Use backing index
            management.addVertexIndexKey(Constants.VERTEX_INDEX, propertyKey);

            LOG.info("Created backing index for property {} of type {} ", propertyName, propertyClass.getName());
        }
    }

    private boolean checkIfVertexIndexApplicable(Class propertyClass, AtlasCardinality cardinality) {
        return !(VERTEX_INDEX_EXCLUSIONS.contains(propertyClass) || cardinality.isMany());
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

    /**
     * Initialize global indices for Titan graph on server activation.
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
                    cleanupIndices(management, typeDef);
                }
            }

            //Commit indexes
            commit(management);
        } catch (RepositoryException | IndexException e) {
            LOG.error("Failed to update indexes for changed typedefs", e);
            attemptRollback(changedTypeDefs, management);
        }

    }

    private void cleanupIndices(AtlasGraphManagement management, AtlasBaseTypeDef typeDef) {
        Preconditions.checkNotNull(typeDef, "Cannot process null typedef");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cleaning up index for {}", typeDef);
        }

        if (typeDef instanceof AtlasEnumDef) {
            // Only handle complex types like Struct, Classification and Entity
            return;
        }

        if (typeDef instanceof AtlasStructDef) {
            AtlasStructDef structDef = (AtlasStructDef) typeDef;
            List<AtlasAttributeDef> attributeDefs = structDef.getAttributeDefs();
            if (CollectionUtils.isNotEmpty(attributeDefs)) {
                for (AtlasAttributeDef attributeDef : attributeDefs) {
                    cleanupIndexForAttribute(management, typeDef.getName(), attributeDef);
                }
            }
        } else if (!AtlasTypeUtil.isBuiltInType(typeDef.getName())){
            throw new IllegalArgumentException("bad data type" + typeDef.getName());
        }
    }

    private void cleanupIndexForAttribute(AtlasGraphManagement management, String typeName, AtlasAttributeDef attributeDef) {
        final String propertyName = GraphHelper.encodePropertyKey(typeName + "." + attributeDef.getName());
        String attribTypeName = attributeDef.getTypeName();
        boolean isBuiltInType = AtlasTypeUtil.isBuiltInType(attribTypeName);
        boolean isArrayType = AtlasTypeUtil.isArrayType(attribTypeName);
        boolean isMapType = AtlasTypeUtil.isMapType(attribTypeName);

        try {
            AtlasType atlasType = typeRegistry.getType(attribTypeName);

            if (isMapType || isArrayType || isClassificationType(atlasType) || isEntityType(atlasType)) {
                LOG.warn("Ignoring non-indexable attribute {}", attribTypeName);
            } else if (isBuiltInType || isEnumType(atlasType)) {
                cleanupIndex(management, propertyName);
            } else if (isStructType(atlasType)) {
                AtlasStructDef structDef = typeRegistry.getStructDefByName(attribTypeName);
                cleanupIndices(management, structDef);
            }
        } catch (AtlasBaseException e) {
            LOG.error("No type exists for {}", attribTypeName, e);
        }
    }

    private void cleanupIndex(AtlasGraphManagement management, String propertyKey) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Invalidating property key = {}", propertyKey);
        }
        management.deletePropertyKey(propertyKey);
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

    /* Commenting this out since we do not need an index for edge label here
    private void createEdgeMixedIndex(String propertyName) {
        EdgeLabel edgeLabel = management.getEdgeLabel(propertyName);
        if (edgeLabel == null) {
            edgeLabel = management.makeEdgeLabel(propertyName).make();
            management.buildEdgeIndex(edgeLabel, propertyName, Direction.BOTH, Order.DEFAULT);
            LOG.info("Created index for edge label {}", propertyName);
        }
    }*/
}
