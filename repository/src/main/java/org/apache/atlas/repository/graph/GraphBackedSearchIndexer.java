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

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.discovery.SearchIndexer;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.IndexCreationException;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * Adds index for properties of a given type when its added before any instances are added.
 */
public class GraphBackedSearchIndexer implements SearchIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedSearchIndexer.class);

    private final TitanGraph titanGraph;

    private TitanManagement management;

    @Inject
    public GraphBackedSearchIndexer(GraphProvider<TitanGraph> graphProvider) throws RepositoryException {

        this.titanGraph = graphProvider.get();

        /* Create the transaction for indexing.
         */
        management = titanGraph.getManagementSystem();
        initialize();
    }

    /**
     * Initializes the indices for the graph - create indices for Global Vertex Keys
     */
    private void initialize() {
        if (management.containsPropertyKey(Constants.GUID_PROPERTY_KEY)) {
            LOG.info("Global indexes already exist for graph");
            return;
        }

        /* This is called only once, which is the first time Atlas types are made indexable .*/
        LOG.info("Indexes do not exist, Creating indexes for titanGraph.");
        management.buildIndex(Constants.VERTEX_INDEX, Vertex.class).buildMixedIndex(Constants.BACKING_INDEX);
        management.buildIndex(Constants.EDGE_INDEX, Edge.class).buildMixedIndex(Constants.BACKING_INDEX);

        // create a composite index for guid as its unique
        createCompositeIndex(Constants.GUID_INDEX, Constants.GUID_PROPERTY_KEY, String.class, true, Cardinality.SINGLE);

        // create a composite and mixed index for type since it can be combined with other keys
        createCompositeAndMixedIndex(Constants.ENTITY_TYPE_INDEX, Constants.ENTITY_TYPE_PROPERTY_KEY, String.class,
                false, Cardinality.SINGLE);

        // create a composite and mixed index for type since it can be combined with other keys
        createCompositeAndMixedIndex(Constants.SUPER_TYPES_INDEX, Constants.SUPER_TYPES_PROPERTY_KEY, String.class,
                false, Cardinality.SET);

        // create a composite and mixed index for traitNames since it can be combined with other
        // keys. Traits must be a set and not a list.
        createCompositeAndMixedIndex(Constants.TRAIT_NAMES_INDEX, Constants.TRAIT_NAMES_PROPERTY_KEY, String.class,
                false, Cardinality.SET);

        // Index for full text search
        createFullTextIndex();

        //Indexes for graph backed type system store
        createTypeStoreIndexes();

        management.commit();
        //Make sure we acquire another transaction after commit for subsequent indexing
        management = titanGraph.getManagementSystem();

        LOG.info("Index creation for global keys complete.");
    }

    private void createFullTextIndex() {
        PropertyKey fullText =
                management.makePropertyKey(Constants.ENTITY_TEXT_PROPERTY_KEY).dataType(String.class).make();

        management.buildIndex(Constants.FULLTEXT_INDEX, Vertex.class)
                .addKey(fullText, com.thinkaurelius.titan.core.schema.Parameter.of("mapping", Mapping.TEXT))
                .buildMixedIndex(Constants.BACKING_INDEX);
        LOG.info("Created mixed index for {}", Constants.ENTITY_TEXT_PROPERTY_KEY);
    }

    private void createTypeStoreIndexes() {
        //Create unique index on typeName
        createCompositeIndex(Constants.TYPENAME_PROPERTY_KEY, Constants.TYPENAME_PROPERTY_KEY, String.class, true,
                Cardinality.SINGLE);

        //create index on vertex type
        createCompositeIndex(Constants.VERTEX_TYPE_PROPERTY_KEY, Constants.VERTEX_TYPE_PROPERTY_KEY, String.class,
                false, Cardinality.SINGLE);

    }

    /**
     * This is upon adding a new type to Store.
     *
     * @param dataTypes data type
     * @throws org.apache.atlas.AtlasException
     */
    @Override
    public void onAdd(Collection<? extends IDataType> dataTypes) throws AtlasException {

        for (IDataType dataType : dataTypes) {
            LOG.info("Creating indexes for type name={}, definition={}", dataType.getName(), dataType.getClass());
            try {
                addIndexForType(dataType);
                LOG.info("Index creation for type {} complete", dataType.getName());
            } catch (Throwable throwable) {
                LOG.error("Error creating index for type {}", dataType, throwable);
                //Rollback indexes if any failure
                rollback();
                throw new IndexCreationException("Error while creating index for type " + dataType, throwable);
            }
        }
        //Commit indexes
        commit();
    }

    private void addIndexForType(IDataType dataType) {
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
            createIndexForFields(structType, structType.fieldMapping().fields);
            break;

        case TRAIT:
            TraitType traitType = (TraitType) dataType;
            createIndexForFields(traitType, traitType.fieldMapping().fields);
            break;

        case CLASS:
            ClassType classType = (ClassType) dataType;
            createIndexForFields(classType, classType.fieldMapping().fields);
            break;

        default:
            throw new IllegalArgumentException("bad data type" + dataType);
        }
    }

    private void createIndexForFields(IDataType dataType, Map<String, AttributeInfo> fields) {
        for (AttributeInfo field : fields.values()) {
            if (field.isIndexable) {
                createIndexForAttribute(dataType.getName(), field);
            }
        }
    }

    private void createIndexForAttribute(String typeName, AttributeInfo field) {
        final String propertyName = typeName + "." + field.name;
        switch (field.dataType().getTypeCategory()) {
        case PRIMITIVE:
            createVertexMixedIndex(propertyName, getPrimitiveClass(field.dataType()));
            break;

        case ENUM:
            createVertexMixedIndex(propertyName, String.class);
            break;

        case ARRAY:
        case MAP:
            // todo - how do we overcome this limitation?
            // IGNORE: Can only index single-valued property keys on vertices in Mixed Index
            break;

        case STRUCT:
            StructType structType = (StructType) field.dataType();
            createIndexForFields(structType, structType.fieldMapping().fields);
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

/*
    private Cardinality getCardinality(Multiplicity multiplicity) {
        if (multiplicity == Multiplicity.OPTIONAL || multiplicity == Multiplicity.REQUIRED) {
            return Cardinality.SINGLE;
        } else if (multiplicity == Multiplicity.COLLECTION) {
            return Cardinality.LIST;
        } else if (multiplicity == Multiplicity.SET) {
            return Cardinality.SET;
        }

        // todo - default to LIST as this is the most forgiving
        return Cardinality.LIST;
    }
*/

    private void createCompositeAndMixedIndex(String indexName, String propertyName, Class propertyClass,
            boolean isUnique, Cardinality cardinality) {
        createCompositeIndex(indexName, propertyName, propertyClass, isUnique, cardinality);
        createVertexMixedIndex(propertyName, propertyClass);
    }

    private PropertyKey createCompositeIndex(String indexName, String propertyName, Class propertyClass,
            boolean isUnique, Cardinality cardinality) {
        PropertyKey propertyKey = management.getPropertyKey(propertyName);
        if (propertyKey == null) {
            propertyKey =
                    management.makePropertyKey(propertyName).dataType(propertyClass).cardinality(cardinality).make();

            TitanManagement.IndexBuilder indexBuilder =
                    management.buildIndex(indexName, Vertex.class).addKey(propertyKey);

            if (isUnique) {
                indexBuilder = indexBuilder.unique();
            }

            indexBuilder.buildCompositeIndex();
            LOG.info("Created index for property {} in composite index {}", propertyName, indexName);
        }

        return propertyKey;
    }

    private PropertyKey createVertexMixedIndex(String propertyName, Class propertyClass) {

        PropertyKey propertyKey = management.getPropertyKey(propertyName);
        if (propertyKey == null) {
            // ignored cardinality as Can only index single-valued property keys on vertices
            propertyKey = management.makePropertyKey(propertyName).dataType(propertyClass).make();

            if (!checkIfMixedIndexApplicable(propertyClass)) {
                LOG.debug("Creating composite index for property {} of type {} ", propertyName,
                        propertyClass.getName());
                //Use standard index as backing index only supports string, int and geo types
                management.buildIndex(propertyName, Vertex.class).addKey(propertyKey).buildCompositeIndex();
                LOG.debug("Created composite index for property {} of type {} ", propertyName, propertyClass.getName());
            } else {
                //Use backing index
                LOG.debug("Creating backing index for property {} of type {} ", propertyName, propertyClass.getName());
                TitanGraphIndex vertexIndex = management.getGraphIndex(Constants.VERTEX_INDEX);
                management.addIndexKey(vertexIndex, propertyKey);
                LOG.debug("Created backing index for property {} of type {} ", propertyName, propertyClass.getName());
            }
            LOG.info("Created mixed vertex index for property {}", propertyName);
        }

        return propertyKey;
    }

    private boolean checkIfMixedIndexApplicable(Class propertyClass) {
        if (propertyClass == Boolean.class || propertyClass == BigDecimal.class || propertyClass == BigInteger.class) {
            return false;
        }
        return true;
    }

    public void commit() throws IndexException {
        try {
            management.commit();
        } catch (Exception e) {
            LOG.error("Index commit failed", e);
            throw new IndexException("Index commit failed ", e);
        }
    }

    public void rollback() throws IndexException {
        try {
            management.rollback();
        } catch (Exception e) {
            LOG.error("Index rollback failed ", e);
            throw new IndexException("Index rollback failed ", e);
        }
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
