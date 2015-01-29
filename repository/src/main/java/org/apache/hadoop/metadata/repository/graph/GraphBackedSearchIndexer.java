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

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.SearchIndexer;
import org.apache.hadoop.metadata.types.AttributeInfo;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.IDataType;
import org.apache.hadoop.metadata.types.StructType;
import org.apache.hadoop.metadata.types.TraitType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

/**
 * Adds index for properties of a given type when its added before any instances are added.
 */
public class GraphBackedSearchIndexer implements SearchIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedSearchIndexer.class);

    private final TitanGraph titanGraph;

    @Inject
    public GraphBackedSearchIndexer(GraphService graphService) throws MetadataException {
        this.titanGraph = ((TitanGraphService) graphService).getTitanGraph();

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
        try {
            createIndex(management, Constants.GUID_PROPERTY_KEY, String.class, true);
            createIndex(management, Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
            createIndex(management, Constants.TIMESTAMP_PROPERTY_KEY, Long.class);
        } finally {
            management.commit();
        }

        LOG.info("Index creation for global keys complete.");
        // dumpIndexKeys();
    }

/*
    private void dumpIndexKeys() {
        for (TitanGraphIndex index : management.getGraphIndexes(Vertex.class)) {
            System.out.println("index.getName() = " + index.getName());

            for (PropertyKey key : index.getFieldKeys()) {
                System.out.println("key.getName() = " + key.getName());
                System.out.println("key = " + key);
            }
        }

        for (TitanGraphIndex index : management.getGraphIndexes(Edge.class)) {
            System.out.println("index.getName() = " + index.getName());

            for (PropertyKey key : index.getFieldKeys()) {
                System.out.println("key.getName() = " + key.getName());
                System.out.println("key = " + key);
            }
        }
    }
*/

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
            TitanManagement management = titanGraph.getManagementSystem();
            addIndexForType(management, dataType);
            management.commit();
            LOG.info("Index creation for type {} complete", typeName);

        } catch (Exception e) {
            LOG.error("Error creating index for type {}", dataType, e);
            // management.rollback();
        }

        // dumpIndexKeys();
    }

    private void addIndexForType(TitanManagement management, IDataType dataType) {
        switch (dataType.getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
            case ARRAY:
            case MAP:
                // do nothing since these are NOT types
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

    private void createIndexForFields(TitanManagement management,
                                      IDataType dataType, Map<String, AttributeInfo> fields) {
        for (AttributeInfo field : fields.values()) {
            if (field.isIndexable) {
                createIndexForAttribute(management, dataType.getName(), field);
            }
        }
    }

    private void createIndexForAttribute(TitanManagement management,
                                         String typeName, AttributeInfo field) {
        final String propertyName = typeName + "." + field.name;
        switch (field.dataType().getTypeCategory()) {
            case PRIMITIVE:
                createIndex(management, propertyName,
                        getPrimitiveClass(field.dataType()), field.isUnique);
                break;

            case ENUM:
                createIndex(management, propertyName, Integer.class, field.isUnique);
                break;

            case ARRAY:
            case MAP:
                // index the property holder for element names
                createIndex(management, propertyName, String.class, field.isUnique);
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
                createEdgeIndex(management, propertyName, String.class);
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
        }

        throw new IllegalArgumentException("unknown data type " + dataType);
    }

    private static PropertyKey createIndex(TitanManagement management,
                                           String propertyName, Class propertyClass) {
        return createIndex(management,
                propertyName, propertyClass, Cardinality.SINGLE, false, Vertex.class);
    }

    private static PropertyKey createIndex(TitanManagement management,
                                           String propertyName,
                                           Class propertyClass, boolean isUnique) {
        return createIndex(management,
                propertyName, propertyClass, Cardinality.SINGLE, isUnique, Vertex.class);
    }

    private static PropertyKey createEdgeIndex(TitanManagement management,
                                               String propertyName, Class propertyClass) {
        return createIndex(management,
                propertyName, propertyClass, Cardinality.SINGLE, false, Edge.class);
    }

    private static PropertyKey createIndex(TitanManagement management,
                                           String propertyName, Class propertyClass,
                                           Cardinality cardinality, boolean isUnique,
                                           Class<? extends Element> elementClass) {
        PropertyKey propertyKey = management.getPropertyKey(propertyName);
        if (propertyKey == null) {
            propertyKey = management
                    .makePropertyKey(propertyName)
                    .dataType(propertyClass)
                    .cardinality(cardinality)
                    .make();

            TitanManagement.IndexBuilder indexBuilder = management
                    .buildIndex("index_" + propertyName, elementClass)
                    .addKey(propertyKey);

            if (isUnique) {
                indexBuilder = indexBuilder.unique();
            }

            indexBuilder.buildCompositeIndex();
        }

        return propertyKey;
    }
}
