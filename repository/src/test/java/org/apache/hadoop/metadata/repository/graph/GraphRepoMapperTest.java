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

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.Referenceable;
import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.Struct;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.types.EnumValue;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.IDataType;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.StructTypeDefinition;
import org.apache.hadoop.metadata.types.TraitType;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;

@Test
@Guice(modules = RepositoryMetadataModule.class)
public class GraphRepoMapperTest {

    private static final String DATABASE_TYPE = "hive_database";
    private static final String DATABASE_NAME = "foo";
    private static final String TABLE_TYPE = "hive_table";
    private static final String TABLE_NAME = "bar";

    @Inject
    private TitanGraphService titanGraphService;
    @Inject
    private GraphBackedMetadataRepository repositoryService;

    private TypeSystem typeSystem;

    @BeforeClass
    public void setUp() throws Exception {
        // start the injected graph service
        titanGraphService.start();
        // start the injected repository service
        repositoryService.start();

        typeSystem = TypeSystem.getInstance();

        createHiveTypes();
    }

    @AfterMethod
    public void tearDown() {
        dumpGraph();
    }

    @Test
    public void testSubmitEntity() throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");
        System.out.println("databaseInstance = " + databaseInstance);

        ClassType dbType = typeSystem.getDataType(ClassType.class, DATABASE_TYPE);
        ITypedReferenceableInstance db = dbType.convert(databaseInstance, Multiplicity.REQUIRED);
        System.out.println("db = " + db);

        String dbGUID = repositoryService.createEntity(db, DATABASE_TYPE);
        System.out.println("added db = " + dbGUID);

        Referenceable dbInstance = new Referenceable(
                dbGUID, DATABASE_TYPE, databaseInstance.getValuesMap());

        ITypedReferenceableInstance table = createHiveTableInstance(dbInstance);
        String tableGUID = repositoryService.createEntity(table, TABLE_TYPE);
        System.out.println("added table = " + tableGUID);
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinition() throws Exception {
        TitanGraph graph = titanGraphService.getTitanGraph();
        GraphQuery query = graph.query()
                .has("type", Compare.EQUAL, TABLE_TYPE)
                .has("type", Compare.EQUAL, TABLE_TYPE);
        Iterator<Vertex> results = query.vertices().iterator();
        // returning one since guid should be unique
        Vertex tableVertex = results.hasNext() ? results.next() : null;
        if (tableVertex == null) {
            Assert.fail();
        }

        String guid = tableVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        if (guid == null) {
            Assert.fail();
        }

        ITypedReferenceableInstance table = repositoryService.getEntityDefinition(guid);
        System.out.println("*** table = " + table);
    }

    private void dumpGraph() {
        TitanGraph graph = titanGraphService.getTitanGraph();
        System.out.println("*******************Graph Dump****************************");
        System.out.println("Vertices of " + graph);
        for (Vertex vertex : graph.getVertices()) {
//            System.out.println(GraphHelper.vertexString(vertex));
            System.out.print("vertex = " + vertex + " [");
            TitanVertex titanVertex = (TitanVertex) graph.getVertex(vertex.getId());
            System.out.print("guid= " + titanVertex.getProperty(Constants.GUID_PROPERTY_KEY));
            System.out.print("type= " + titanVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY));
            System.out.println("]");
        }

        System.out.println("Edges of " + graph);
        for (Edge edge : graph.getEdges()) {
            System.out.println(GraphHelper.edgeString(edge));
        }
        System.out.println("*******************Graph Dump****************************");
    }

    private void createHiveTypes() throws Exception {
        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef(DATABASE_TYPE,
                        ImmutableList.<String>of(),
                        createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition =
                new StructTypeDefinition("serdeType",
                        new AttributeDefinition[] {
                                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                                createRequiredAttrDef("serde", DataTypes.STRING_TYPE)
                        });

        EnumValue values[] = {
                new EnumValue("MANAGED", 1),
                new EnumValue("EXTERNAL", 2),
        };

        EnumTypeDefinition enumTypeDefinition =  new EnumTypeDefinition("tableType", values);
        typeSystem.defineEnumType(enumTypeDefinition);

        HierarchicalTypeDefinition<ClassType> columnsDefinition =
                createClassTypeDef("column_type",
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE));

        StructTypeDefinition partitionDefinition =
                new StructTypeDefinition("partition_type",
                        new AttributeDefinition[] {
                                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        });

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition =
                createClassTypeDef(TABLE_TYPE,
                        ImmutableList.<String>of(),
                        createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        // enum
                        new AttributeDefinition("tableType", "tableType",
                                Multiplicity.REQUIRED, false, null),
                        // array of strings
                        new AttributeDefinition("columnNames",
                                String.format("array<%s>", DataTypes.STRING_TYPE.getName()),
                                Multiplicity.COLLECTION, false, null),
                        // array of classes
                        new AttributeDefinition("columns",
                                String.format("array<%s>", "column_type"),
                                Multiplicity.COLLECTION, true, null),
                        // array of structs
                        new AttributeDefinition("partitions",
                                String.format("array<%s>", "partition_type"),
                                Multiplicity.COLLECTION, true, null),
                        // struct reference
                        new AttributeDefinition("serde1",
                                "serdeType", Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("serde2",
                                "serdeType", Multiplicity.REQUIRED, false, null),
                        // class reference
                        new AttributeDefinition("database",
                                DATABASE_TYPE, Multiplicity.REQUIRED, true, null));

        HierarchicalTypeDefinition<TraitType> classificationTypeDefinition =
                createTraitTypeDef("classification",
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        typeSystem.defineTypes(
                ImmutableList.of(structTypeDefinition, partitionDefinition),
                ImmutableList.of(classificationTypeDefinition),
                ImmutableList.of(databaseTypeDefinition, columnsDefinition, tableTypeDefinition));
    }

    private ITypedReferenceableInstance createHiveTableInstance(
            Referenceable databaseInstance) throws Exception {
/*
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");
*/

        Referenceable tableInstance = new Referenceable(TABLE_TYPE, "classification");
        tableInstance.set("name", TABLE_NAME);
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
        tableInstance.set("tableType", 1); // enum

        // refer to an existing class
        tableInstance.set("database", databaseInstance);

        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.add("first_name");
        columnNames.add("last_name");
        tableInstance.set("columnNames", columnNames);

        Struct traitInstance = (Struct) tableInstance.getTrait("classification");
        traitInstance.set("tag", "foundation_etl");

        Struct serde1Instance = new Struct("serdeType");
        serde1Instance.set("name", "serde1");
        serde1Instance.set("serde", "serde1");
        tableInstance.set("serde1", serde1Instance);

        Struct serde2Instance = new Struct("serdeType");
        serde2Instance.set("name", "serde2");
        serde2Instance.set("serde", "serde2");
        tableInstance.set("serde2", serde2Instance);

        ArrayList<Referenceable> columns = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            Referenceable columnInstance = new Referenceable("column_type");
            columnInstance.set("name", "column_" + index);
            columnInstance.set("type", "string");
            columns.add(columnInstance);
        }
        tableInstance.set("columns", columns);

        ArrayList<Struct> partitions = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            Struct partitionInstance = new Struct("partition_type");
            partitionInstance.set("name", "partition_" + index);
            partitions.add(partitionInstance);
        }
        tableInstance.set("partitions", partitions);

        ClassType tableType = typeSystem.getDataType(ClassType.class, TABLE_TYPE);
        return tableType.convert(tableInstance, Multiplicity.REQUIRED);
    }

    protected AttributeDefinition createUniqueRequiredAttrDef(String name,
                                                              IDataType dataType) {
        return new AttributeDefinition(name, dataType.getName(),
                Multiplicity.REQUIRED, false, true, true, null);
    }

    protected AttributeDefinition createRequiredAttrDef(String name,
                                                        IDataType dataType) {
        return new AttributeDefinition(name, dataType.getName(),
                Multiplicity.REQUIRED, false, null);
    }

    @SuppressWarnings("unchecked")
    protected HierarchicalTypeDefinition<TraitType> createTraitTypeDef(
            String name, ImmutableList<String> superTypes, AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition(TraitType.class, name, superTypes, attrDefs);
    }

    @SuppressWarnings("unchecked")
    protected HierarchicalTypeDefinition<ClassType> createClassTypeDef(
            String name, ImmutableList<String> superTypes, AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition(ClassType.class, name, superTypes, attrDefs);
    }
}
