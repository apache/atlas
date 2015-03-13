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
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.TestUtils;
import org.apache.hadoop.metadata.repository.RepositoryException;
import org.apache.hadoop.metadata.typesystem.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.ITypedStruct;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.Struct;
import org.apache.hadoop.metadata.typesystem.persistence.Id;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.EnumValue;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.StructTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;
import scala.actors.threadpool.Arrays;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * GraphBackedMetadataRepository test
 *
 * Guice loads the dependencies and injects the necessary objects
 *
 */
@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedMetadataRepositoryTest {

    private static final String ENTITY_TYPE = "Department";
    private static final String DATABASE_TYPE = "hive_database";
    private static final String DATABASE_NAME = "foo";
    private static final String TABLE_TYPE = "hive_table";
    private static final String TABLE_NAME = "bar";
    private static final String CLASSIFICATION = "classification";
    private static final String PII = "PII";

    @Inject
    private TitanGraphService titanGraphService;
    @Inject
    private GraphBackedMetadataRepository repositoryService;

    private TypeSystem typeSystem;
    private String guid;

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        // start the injected graph service
        titanGraphService.initialize();

        new GraphBackedSearchIndexer(titanGraphService);

        TestUtils.defineDeptEmployeeTypes(typeSystem);
        createHiveTypes();
    }

/*
    @AfterMethod
    public void tearDown() {
         dumpGraph();
    }
*/

    @Test
    public void testSubmitEntity() throws Exception {
        Referenceable hrDept = TestUtils.createDeptEg1(typeSystem);
        ClassType deptType = typeSystem.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        guid = repositoryService.createEntity(hrDept2, ENTITY_TYPE);
        Assert.assertNotNull(guid);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinitionForDepartment() throws Exception {
        ITypedReferenceableInstance entity = repositoryService.getEntityDefinition(guid);
        Assert.assertNotNull(entity);
    }

    @Test (expectedExceptions = RepositoryException.class)
    public void testGetEntityDefinitionNonExistent() throws Exception {
        repositoryService.getEntityDefinition("blah");
        Assert.fail();
    }

    @Test
    public void testGetEntityList() throws Exception {
        List<String> entityList = repositoryService.getEntityList(ENTITY_TYPE);
        System.out.println("entityList = " + entityList);
        Assert.assertNotNull(entityList);
        Assert.assertEquals(entityList.size(), 1); // one department
    }

    @Test
    public void testGetTypeAttributeName() throws Exception {
        Assert.assertEquals(
                repositoryService.getTypeAttributeName(), Constants.ENTITY_TYPE_PROPERTY_KEY);
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testGetTraitLabel() throws Exception {
        Assert.assertEquals(repositoryService.getTraitLabel(
                    typeSystem.getDataType(ClassType.class, TABLE_TYPE), CLASSIFICATION),
                TABLE_TYPE + "." + CLASSIFICATION);
    }

    @Test
    public void testCreateEntity() throws Exception {
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

    @Test(dependsOnMethods = "testCreateEntity")
    public void testGetEntityDefinition() throws Exception {
        String guid = getGUID();

        ITypedReferenceableInstance table = repositoryService.getEntityDefinition(guid);
        System.out.println("*** table = " + table);
    }

    private String getGUID() {
        Vertex tableVertex = getTableEntityVertex();

        String guid = tableVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        if (guid == null) {
            Assert.fail();
        }
        return guid;
    }

    private Vertex getTableEntityVertex() {
        TitanGraph graph = titanGraphService.getTitanGraph();
        GraphQuery query = graph.query()
                .has(Constants.ENTITY_TYPE_PROPERTY_KEY, Compare.EQUAL, TABLE_TYPE);
        Iterator<Vertex> results = query.vertices().iterator();
        // returning one since guid should be unique
        Vertex tableVertex = results.hasNext() ? results.next() : null;
        if (tableVertex == null) {
            Assert.fail();
        }

        return tableVertex;
    }

    @Test (dependsOnMethods = "testCreateEntity")
    public void testGetTraitNames() throws Exception {
        final List<String> traitNames = repositoryService.getTraitNames(getGUID());
        Assert.assertEquals(traitNames.size(), 1);
        Assert.assertEquals(traitNames, Arrays.asList(new String[]{CLASSIFICATION}));
    }

    @Test
    public void testGetTraitNamesForEmptyTraits() throws Exception {
        final List<String> traitNames = repositoryService.getTraitNames(guid);
        Assert.assertEquals(traitNames.size(), 0);
    }

    @Test (expectedExceptions = RepositoryException.class)
    public void testGetTraitNamesForBadEntity() throws Exception {
        repositoryService.getTraitNames(UUID.randomUUID().toString());
        Assert.fail();
    }

    @Test (dependsOnMethods = "testGetTraitNames")
    public void testAddTrait() throws Exception {
        final String aGUID = getGUID();

        List<String> traitNames = repositoryService.getTraitNames(aGUID);
        System.out.println("traitNames = " + traitNames);
        Assert.assertEquals(traitNames.size(), 1);
        Assert.assertTrue(traitNames.contains(CLASSIFICATION));
        Assert.assertFalse(traitNames.contains(PII));

        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef(PII, ImmutableList.<String>of());
        TraitType traitType = typeSystem.defineTraitType(piiTrait);
        ITypedStruct traitInstance = traitType.createInstance();

        repositoryService.addTrait(aGUID, traitInstance);

        // refresh trait names
        traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 2);
        Assert.assertTrue(traitNames.contains(PII));
        Assert.assertTrue(traitNames.contains(CLASSIFICATION));
    }

    @Test (expectedExceptions = NullPointerException.class)
    public void testAddTraitWithNullInstance() throws Exception {
        repositoryService.addTrait(getGUID(), null);
        Assert.fail();
    }

    @Test (dependsOnMethods = "testAddTrait", expectedExceptions = RepositoryException.class)
    public void testAddTraitForBadEntity() throws Exception {
        TraitType traitType = typeSystem.getDataType(TraitType.class, PII);
        ITypedStruct traitInstance = traitType.createInstance();

        repositoryService.addTrait(UUID.randomUUID().toString(), traitInstance);
        Assert.fail();
    }

    @Test (dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String aGUID = getGUID();

        List<String> traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 2);
        Assert.assertTrue(traitNames.contains(PII));
        Assert.assertTrue(traitNames.contains(CLASSIFICATION));

        repositoryService.deleteTrait(aGUID, PII);

        // refresh trait names
        traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 1);
        Assert.assertTrue(traitNames.contains(CLASSIFICATION));
        Assert.assertFalse(traitNames.contains(PII));
    }

    @Test (expectedExceptions = RepositoryException.class)
    public void testDeleteTraitForNonExistentEntity() throws Exception {
        repositoryService.deleteTrait(UUID.randomUUID().toString(), PII);
        Assert.fail();
    }

    @Test (expectedExceptions = RepositoryException.class)
    public void testDeleteTraitForNonExistentTrait() throws Exception {
        final String aGUID = getGUID();
        repositoryService.deleteTrait(aGUID, "PCI");
        Assert.fail();
    }

    @Test (dependsOnMethods = "testCreateEntity")
    public void testGetIdFromVertex() throws Exception {
        Vertex tableVertex = getTableEntityVertex();

        String guid = tableVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        if (guid == null) {
            Assert.fail();
        }

        Id expected = new Id(guid,
                tableVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), TABLE_TYPE);
        Assert.assertEquals(repositoryService.getIdFromVertex(TABLE_TYPE, tableVertex),
                expected);
    }

    @Test (dependsOnMethods = "testCreateEntity")
    public void testGetTypeName() throws Exception {
        Vertex tableVertex = getTableEntityVertex();
        Assert.assertEquals(repositoryService.getTypeName(tableVertex), TABLE_TYPE);
    }

/*
    private void dumpGraph() {
        TitanGraph graph = titanGraphService.getTitanGraph();
        System.out.println("*******************Graph Dump****************************");
        System.out.println("Vertices of " + graph);
        for (Vertex vertex : graph.getVertices()) {
            System.out.println(GraphHelper.vertexString(vertex));
        }

        System.out.println("Edges of " + graph);
        for (Edge edge : graph.getEdges()) {
            System.out.println(GraphHelper.edgeString(edge));
        }
        System.out.println("*******************Graph Dump****************************");
    }
*/

    private void createHiveTypes() throws Exception {
        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                TypesUtil.createClassTypeDef(DATABASE_TYPE,
                        ImmutableList.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition =
                new StructTypeDefinition("serdeType",
                        new AttributeDefinition[]{
                                TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                                TypesUtil.createRequiredAttrDef("serde", DataTypes.STRING_TYPE)
                        });

        EnumValue values[] = {
                new EnumValue("MANAGED", 1),
                new EnumValue("EXTERNAL", 2),
        };

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("tableType", values);
        typeSystem.defineEnumType(enumTypeDefinition);

        HierarchicalTypeDefinition<ClassType> columnsDefinition =
                TypesUtil.createClassTypeDef("column_type",
                        ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE));

        StructTypeDefinition partitionDefinition =
                new StructTypeDefinition("partition_type",
                        new AttributeDefinition[]{
                                TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        });

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition =
                TypesUtil.createClassTypeDef(TABLE_TYPE,
                        ImmutableList.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE),
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
                TypesUtil.createTraitTypeDef(CLASSIFICATION,
                        ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        typeSystem.defineTypes(
                ImmutableList.of(structTypeDefinition, partitionDefinition),
                ImmutableList.of(classificationTypeDefinition),
                ImmutableList.of(databaseTypeDefinition, columnsDefinition, tableTypeDefinition));
    }

    private ITypedReferenceableInstance createHiveTableInstance(
            Referenceable databaseInstance) throws Exception {
        Referenceable tableInstance = new Referenceable(TABLE_TYPE, CLASSIFICATION);
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

        Struct traitInstance = (Struct) tableInstance.getTrait(CLASSIFICATION);
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
}
