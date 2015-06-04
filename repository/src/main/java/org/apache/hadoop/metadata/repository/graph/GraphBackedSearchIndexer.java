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

package org.apache.hadoop.metadata.repository.graph;

import com.google.inject.Singleton;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.discovery.SearchIndexer;
import org.apache.hadoop.metadata.repository.Constants;
import org.apache.hadoop.metadata.repository.RepositoryException;
import org.apache.hadoop.metadata.typesystem.types.AttributeInfo;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.IDataType;
import org.apache.hadoop.metadata.typesystem.types.StructType;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

/**
 * Adds index for properties of a given type when its added before any instances are added.
 */
@Singleton
public class GraphBackedSearchIndexer implements SearchIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedSearchIndexer.class);

    private final TitanGraph titanGraph;

    @Inject
    public GraphBackedSearchIndexer(GraphProvider<TitanGraph> graphProvider)
        throws RepositoryException {

        this.titanGraph = graphProvider.get();

        initialize();
    }

    /**
     * Initializes the indices for the graph - create indices for Global Vertex Keys
     */
    private void initialize() {
        TitanManagement management = titanGraph.getManagementSystem();
        if (management.containsPropertyKey(Constants.GUID_PROPERTY_KEY)) {
            LOG.info("Global indexes already exist for graph");
            return;
        }

        LOG.info("Indexes do not exist, Creating indexes for titanGraph.");
        management.buildIndex(Constants.VERTEX_INDEX, Vertex.class)
                .buildMixedIndex(Constants.BACKING_INDEX);
        management.buildIndex(Constants.EDGE_INDEX, Edge.class)
                .buildMixedIndex(Constants.BACKING_INDEX);

        // create a composite index for guid as its unique
        createCompositeIndex(Constants.GUID_INDEX,
                Constants.GUID_PROPERTY_KEY, String.class, true, Cardinality.SINGLE);

        // create a composite and mixed index for type since it can be combined with other keys
        createCompositeAndMixedIndex(Constants.ENTITY_TYPE_INDEX,
                Constants.ENTITY_TYPE_PROPERTY_KEY, String.class, false, Cardinality.SINGLE);

        // create a composite and mixed index for type since it can be combined with other keys
        createCompositeAndMixedIndex(Constants.SUPER_TYPES_INDEX,
                Constants.SUPER_TYPES_PROPERTY_KEY, String.class, false, Cardinality.SET);

        // create a composite and mixed index for traitNames since it can be combined with other
        // keys. Traits must be a set and not a list.
        createCompositeAndMixedIndex(Constants.TRAIT_NAMES_INDEX,
                Constants.TRAIT_NAMES_PROPERTY_KEY, String.class, false, Cardinality.SET);

        // Index for full text search
        createFullTextIndex();

        //Indexes for graph backed type system store
        createTypeStoreIndexes();

        LOG.info("Index creation for global keys complete.");
    }

    private void createFullTextIndex() {
        TitanManagement management = titanGraph.getManagementSystem();
        PropertyKey fullText =
                management.makePropertyKey(Constants.ENTITY_TEXT_PROPERTY_KEY).dataType(String.class).make();

        management.buildIndex(Constants.FULLTEXT_INDEX, Vertex.class)
                .addKey(fullText, com.thinkaurelius.titan.core.schema.Parameter.of("mapping", Mapping.TEXT))
                .buildMixedIndex(Constants.BACKING_INDEX);
        LOG.info("Created mixed index for {}", Constants.ENTITY_TEXT_PROPERTY_KEY);
    }

    private void createTypeStoreIndexes() {
        //Create unique index on typeName
        createCompositeIndex(Constants.TYPENAME_PROPERTY_KEY, Constants.TYPENAME_PROPERTY_KEY, String.class,
                true, Cardinality.SINGLE);

        //create index on vertex type
        createCompositeIndex(Constants.VERTEX_TYPE_PROPERTY_KEY, Constants.VERTEX_TYPE_PROPERTY_KEY, String.class,
                false, Cardinality.SINGLE);

    }

    /**
     * This is upon adding a new type to Store.
     *
     * @param typeName type name
     * @param dataType data type
     * @throws org.apache.hadoop.metadata.MetadataException
     */
    @Override
    public void onAdd(String typeName, IDataType dataType) throws MetadataException {
        LOG.info("Creating indexes for type name={}, definition={}", typeName, dataType);

        try {
            addIndexForType(dataType);
            LOG.info("Index creation for type {} complete", typeName);

        } catch (Throwable throwable) {
            LOG.error("Error creating index for type {}", dataType, throwable);
            throw new MetadataException("Error while creating index for type " + dataType, throwable);
        }
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
            return Date.class;
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

    private void createCompositeAndMixedIndex(String indexName,
                                              String propertyName, Class propertyClass,
                                              boolean isUnique, Cardinality cardinality) {
        createCompositeIndex(indexName, propertyName, propertyClass, isUnique, cardinality);
        createVertexMixedIndex(propertyName, propertyClass);
    }

    private PropertyKey createCompositeIndex(String indexName,
                                             String propertyName, Class propertyClass,
                                             boolean isUnique, Cardinality cardinality) {
        TitanManagement management = titanGraph.getManagementSystem();
        PropertyKey propertyKey = management.getPropertyKey(propertyName);
        if (propertyKey == null) {
            propertyKey = management
                    .makePropertyKey(propertyName)
                    .dataType(propertyClass)
                    .cardinality(cardinality)
                    .make();

            TitanManagement.IndexBuilder indexBuilder = management
                    .buildIndex(indexName, Vertex.class)
                    .addKey(propertyKey);

            if (isUnique) {
                indexBuilder = indexBuilder.unique();
            }

            indexBuilder.buildCompositeIndex();
            LOG.info("Created index for property {} in composite index {}", propertyName, indexName);
        }

        return propertyKey;
    }

    private PropertyKey createVertexMixedIndex(String propertyName, Class propertyClass) {
        TitanManagement management = titanGraph.getManagementSystem();
        PropertyKey propertyKey = management.getPropertyKey(propertyName);
        if (propertyKey == null) {
            // ignored cardinality as Can only index single-valued property keys on vertices
            propertyKey = management
                    .makePropertyKey(propertyName)
                    .dataType(propertyClass)
                    .make();

            if (propertyClass == Boolean.class) {
                //Use standard index as backing index only supports string, int and geo types
                management.buildIndex(propertyName, Vertex.class).addKey(propertyKey).buildCompositeIndex();
            } else {
                //Use backing index
                TitanGraphIndex vertexIndex = management.getGraphIndex(Constants.VERTEX_INDEX);
                management.addIndexKey(vertexIndex, propertyKey);
            }
            LOG.info("Created mixed vertex index for property {}", propertyName);
        }

        return propertyKey;
    }

    /* Commenting this out since we do not need an index for edge label here
    private void createEdgeMixedIndex(String propertyName) {
        TitanManagement management = titanGraph.getManagementSystem();
        EdgeLabel edgeLabel = management.getEdgeLabel(propertyName);
        if (edgeLabel == null) {
            edgeLabel = management.makeEdgeLabel(propertyName).make();
            management.buildEdgeIndex(edgeLabel, propertyName, Direction.BOTH, Order.DEFAULT);
            LOG.info("Created index for edge label {}", propertyName);
        }
    } */
}
