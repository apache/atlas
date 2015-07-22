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

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.repository.BaseTest;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.EntityNotFoundException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;
import scala.actors.threadpool.Arrays;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
    private static final String SUPER_TYPE_NAME = "Base";

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @Inject
    private GraphBackedMetadataRepository repositoryService;

    @Inject
    private GraphBackedDiscoveryService discoveryService;

    private TypeSystem typeSystem;
    private String guid;

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        new GraphBackedSearchIndexer(graphProvider);

        TestUtils.defineDeptEmployeeTypes(typeSystem);
        createHiveTypes();
    }


    @AfterClass
    public void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
        try {
            //TODO - Fix failure during shutdown while using BDB
            graphProvider.get().shutdown();
        } catch(Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testSubmitEntity() throws Exception {
        Referenceable hrDept = TestUtils.createDeptEg1(typeSystem);
        ClassType deptType = typeSystem.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        guid = repositoryService.createEntity(hrDept2);
        Assert.assertNotNull(guid);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinitionForDepartment() throws Exception {
        ITypedReferenceableInstance entity = repositoryService.getEntityDefinition(guid);
        Assert.assertNotNull(entity);
    }

    @Test(expectedExceptions = RepositoryException.class)
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
        Assert.assertEquals(repositoryService.getTypeAttributeName(), Constants.ENTITY_TYPE_PROPERTY_KEY);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetTraitLabel() throws Exception {
        Assert.assertEquals(
                repositoryService.getTraitLabel(typeSystem.getDataType(ClassType.class, TABLE_TYPE), CLASSIFICATION),
                TABLE_TYPE + "." + CLASSIFICATION);
    }

    @Test
    public void testCreateEntity() throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");
        databaseInstance.set("created", new Date(BaseTest.TEST_DATE_IN_LONG));

        databaseInstance.set("namespace", "colo:cluster:hive:db");
        databaseInstance.set("cluster", "cluster-1");
        databaseInstance.set("colo", "colo-1");
        System.out.println("databaseInstance = " + databaseInstance);

        ClassType dbType = typeSystem.getDataType(ClassType.class, DATABASE_TYPE);
        ITypedReferenceableInstance db = dbType.convert(databaseInstance, Multiplicity.REQUIRED);
        System.out.println("db = " + db);

        String dbGUID = repositoryService.createEntity(db);
        System.out.println("added db = " + dbGUID);

        Referenceable dbInstance = new Referenceable(dbGUID, DATABASE_TYPE, databaseInstance.getValuesMap());

        ITypedReferenceableInstance table = createHiveTableInstance(dbInstance);
        String tableGUID = repositoryService.createEntity(table);
        System.out.println("added table = " + tableGUID);
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testGetEntityDefinition() throws Exception {
        String guid = getGUID();

        ITypedReferenceableInstance table = repositoryService.getEntityDefinition(guid);
        Assert.assertEquals(table.getDate("created"), new Date(BaseTest.TEST_DATE_IN_LONG));
        System.out.println("*** table = " + table);
    }

    @GraphTransaction
    String getGUID() {
        Vertex tableVertex = getTableEntityVertex();

        String guid = tableVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        if (guid == null) {
            Assert.fail();
        }
        return guid;
    }

    @GraphTransaction
    Vertex getTableEntityVertex() {
        TitanGraph graph = graphProvider.get();
        GraphQuery query = graph.query().has(Constants.ENTITY_TYPE_PROPERTY_KEY, Compare.EQUAL, TABLE_TYPE);
        Iterator<Vertex> results = query.vertices().iterator();
        // returning one since guid should be unique
        Vertex tableVertex = results.hasNext() ? results.next() : null;
        if (tableVertex == null) {
            Assert.fail();
        }

        return tableVertex;
    }

    @Test(dependsOnMethods = "testCreateEntity")
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

    @Test(expectedExceptions = EntityNotFoundException.class)
    public void testGetTraitNamesForBadEntity() throws Exception {
        repositoryService.getTraitNames(UUID.randomUUID().toString());
        Assert.fail();
    }

    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTrait() throws Exception {
        final String aGUID = getGUID();

        List<String> traitNames = repositoryService.getTraitNames(aGUID);
        System.out.println("traitNames = " + traitNames);
        Assert.assertEquals(traitNames.size(), 1);
        Assert.assertTrue(traitNames.contains(CLASSIFICATION));
        Assert.assertFalse(traitNames.contains(PII));

        HierarchicalTypeDefinition<TraitType> piiTrait = TypesUtil.createTraitTypeDef(PII, ImmutableList.<String>of());
        TraitType traitType = typeSystem.defineTraitType(piiTrait);
        ITypedStruct traitInstance = traitType.createInstance();

        repositoryService.addTrait(aGUID, traitInstance);

        // refresh trait names
        traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 2);
        Assert.assertTrue(traitNames.contains(PII));
        Assert.assertTrue(traitNames.contains(CLASSIFICATION));
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testAddTraitWithAttribute() throws Exception {
        final String aGUID = getGUID();
        final String traitName = "P_I_I";

        HierarchicalTypeDefinition<TraitType> piiTrait = TypesUtil
                .createTraitTypeDef(traitName, ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE));
        TraitType traitType = typeSystem.defineTraitType(piiTrait);
        ITypedStruct traitInstance = traitType.createInstance();
        traitInstance.set("type", "SSN");

        repositoryService.addTrait(aGUID, traitInstance);

        TestUtils.dumpGraph(graphProvider.get());

        // refresh trait names
        List<String> traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 3);
        Assert.assertTrue(traitNames.contains(traitName));

        ITypedReferenceableInstance instance = repositoryService.getEntityDefinition(aGUID);
        IStruct traitInstanceRef = instance.getTrait(traitName);
        String type = (String) traitInstanceRef.get("type");
        Assert.assertEquals(type, "SSN");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testAddTraitWithNullInstance() throws Exception {
        repositoryService.addTrait(getGUID(), null);
        Assert.fail();
    }

    @Test(dependsOnMethods = "testAddTrait", expectedExceptions = RepositoryException.class)
    public void testAddTraitForBadEntity() throws Exception {
        TraitType traitType = typeSystem.getDataType(TraitType.class, PII);
        ITypedStruct traitInstance = traitType.createInstance();

        repositoryService.addTrait(UUID.randomUUID().toString(), traitInstance);
        Assert.fail();
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String aGUID = getGUID();

        List<String> traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 3);
        Assert.assertTrue(traitNames.contains(PII));
        Assert.assertTrue(traitNames.contains(CLASSIFICATION));
        Assert.assertTrue(traitNames.contains("P_I_I"));

        repositoryService.deleteTrait(aGUID, PII);

        // refresh trait names
        traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 2);
        Assert.assertTrue(traitNames.contains(CLASSIFICATION));
        Assert.assertFalse(traitNames.contains(PII));
    }

    @Test(expectedExceptions = RepositoryException.class)
    public void testDeleteTraitForNonExistentEntity() throws Exception {
        repositoryService.deleteTrait(UUID.randomUUID().toString(), PII);
        Assert.fail();
    }

    @Test(expectedExceptions = RepositoryException.class)
    public void testDeleteTraitForNonExistentTrait() throws Exception {
        final String aGUID = getGUID();
        repositoryService.deleteTrait(aGUID, "PCI");
        Assert.fail();
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testGetIdFromVertex() throws Exception {
        Vertex tableVertex = getTableEntityVertex();

        String guid = tableVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        if (guid == null) {
            Assert.fail();
        }

        Id expected = new Id(guid, tableVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), TABLE_TYPE);
        Assert.assertEquals(repositoryService.getIdFromVertex(TABLE_TYPE, tableVertex), expected);
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testGetTypeName() throws Exception {
        Vertex tableVertex = getTableEntityVertex();
        Assert.assertEquals(repositoryService.getTypeName(tableVertex), TABLE_TYPE);
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testSearchByDSLQuery() throws Exception {
        String dslQuery = "hive_database as PII";
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        Assert.assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        Assert.assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        Assert.assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        Assert.assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        Assert.assertNotNull(typeName);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertNotNull(rows);
        Assert.assertTrue(rows.length() > 0);

        for (int index = 0; index < rows.length(); index++) {
            JSONObject row = rows.getJSONObject(index);
            String type = row.getString("$typeName$");
            Assert.assertEquals(type, "hive_database");

            String name = row.getString("name");
            Assert.assertEquals(name, DATABASE_NAME);
        }
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testSearchByDSLWithInheritance() throws Exception {
        String dslQuery = "Person where name = 'Jane'";
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        Assert.assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        Assert.assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        Assert.assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        Assert.assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        Assert.assertEquals(typeName, "Person");

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertEquals(rows.length(), 1);

        JSONObject row = rows.getJSONObject(0);
        Assert.assertEquals(row.getString("$typeName$"), "Manager");
        Assert.assertEquals(row.getString("name"), "Jane");
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testBug37860() throws Exception {
        String dslQuery = "hive_table as t where name = 'bar' "
                + "database where name = 'foo' and description = 'foo database' select t";
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        Assert.assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        Assert.assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        Assert.assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        Assert.assertNotNull(dataType);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertEquals(rows.length(), 1);

    }

    /**
     * Full text search requires GraphBackedSearchIndexer, and GraphBackedSearchIndexer can't be enabled in
     * GraphBackedDiscoveryServiceTest because of its test data. So, test for full text search is in
     * GraphBackedMetadataRepositoryTest:(
     */
    @Test(dependsOnMethods = "testSubmitEntity")
    public void testFullTextSearch() throws Exception {
        //todo fix this
        //Weird: with lucene, the test passes without sleep
        //but with elasticsearch, doesn't work without sleep. why??
        long sleepInterval = 1000;

        //person in hr department whose name is john
        Thread.sleep(sleepInterval);
        String response = discoveryService.searchByFullText("john");
        Assert.assertNotNull(response);
        JSONArray results = new JSONArray(response);
        Assert.assertEquals(results.length(), 1);
        JSONObject row = (JSONObject) results.get(0);
        Assert.assertEquals(row.get("typeName"), "Person");

        //person in hr department who lives in santa clara
        response = discoveryService.searchByFullText("Jane AND santa AND clara");
        Assert.assertNotNull(response);
        results = new JSONArray(response);
        Assert.assertEquals(results.length(), 1);
        row = (JSONObject) results.get(0);
        Assert.assertEquals(row.get("typeName"), "Manager");

        //search for person in hr department whose name starts is john/jahn
        response = discoveryService.searchByFullText("hr AND (john OR jahn)");
        Assert.assertNotNull(response);
        results = new JSONArray(response);
        Assert.assertEquals(results.length(), 1);
        row = (JSONObject) results.get(0);
        Assert.assertEquals(row.get("typeName"), "Person");
    }

    private void createHiveTypes() throws Exception {
        HierarchicalTypeDefinition<ClassType> superTypeDefinition = TypesUtil
                .createClassTypeDef(SUPER_TYPE_NAME, ImmutableList.<String>of(),
                        TypesUtil.createOptionalAttrDef("namespace", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("cluster", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("colo", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition = TypesUtil
                .createClassTypeDef(DATABASE_TYPE, ImmutableList.of(SUPER_TYPE_NAME),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("created", DataTypes.DATE_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));


        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("serdeType",
                new AttributeDefinition[]{TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("serde", DataTypes.STRING_TYPE)});

        EnumValue values[] = {new EnumValue("MANAGED", 1), new EnumValue("EXTERNAL", 2),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("tableType", values);
        typeSystem.defineEnumType(enumTypeDefinition);

        HierarchicalTypeDefinition<ClassType> columnsDefinition = TypesUtil
                .createClassTypeDef("column_type", ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE));

        StructTypeDefinition partitionDefinition = new StructTypeDefinition("partition_type",
                new AttributeDefinition[]{TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),});

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition = TypesUtil
                .createClassTypeDef(TABLE_TYPE, ImmutableList.of(SUPER_TYPE_NAME),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("created", DataTypes.DATE_TYPE),
                        // enum
                        new AttributeDefinition("tableType", "tableType", Multiplicity.REQUIRED, false, null),
                        // array of strings
                        new AttributeDefinition("columnNames",
                                String.format("array<%s>", DataTypes.STRING_TYPE.getName()), Multiplicity.COLLECTION,
                                false, null),
                        // array of classes
                        new AttributeDefinition("columns", String.format("array<%s>", "column_type"),
                                Multiplicity.COLLECTION, true, null),
                        // array of structs
                        new AttributeDefinition("partitions", String.format("array<%s>", "partition_type"),
                                Multiplicity.COLLECTION, true, null),
                        // map of primitives
                        new AttributeDefinition("parametersMap",
                                DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE.getName()),
                                Multiplicity.COLLECTION, true, null),
                        // map of classes - todo - enable this
                        //                        new AttributeDefinition("columnsMap",
                        //                                DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                        //                                        "column_type"),
                        //                                Multiplicity.COLLECTION, true, null),
                        // map of structs   todo - enable this
                        //                        new AttributeDefinition("partitionsMap",
                        //                                DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                        //                                        "partition_type"),
                        //                                Multiplicity.COLLECTION, true, null),
                        // struct reference
                        new AttributeDefinition("serde1", "serdeType", Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("serde2", "serdeType", Multiplicity.REQUIRED, false, null),
                        // class reference
                        new AttributeDefinition("database", DATABASE_TYPE, Multiplicity.REQUIRED, true, null));

        HierarchicalTypeDefinition<TraitType> classificationTypeDefinition = TypesUtil
                .createTraitTypeDef(CLASSIFICATION, ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<TraitType> fetlClassificationTypeDefinition = TypesUtil
                .createTraitTypeDef("fetl" + CLASSIFICATION, ImmutableList.of(CLASSIFICATION),
                        TypesUtil.createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        typeSystem.defineTypes(ImmutableList.of(structTypeDefinition, partitionDefinition),
                ImmutableList.of(classificationTypeDefinition, fetlClassificationTypeDefinition),
                ImmutableList.of(superTypeDefinition, databaseTypeDefinition, columnsDefinition, tableTypeDefinition));
    }

    private ITypedReferenceableInstance createHiveTableInstance(Referenceable databaseInstance) throws Exception {
        Referenceable tableInstance = new Referenceable(TABLE_TYPE, CLASSIFICATION);
        tableInstance.set("name", TABLE_NAME);
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
        tableInstance.set("created", new Date(BaseTest.TEST_DATE_IN_LONG));
        tableInstance.set("tableType", 1); // enum

        // super type
        tableInstance.set("namespace", "colo:cluster:hive:db:table");
        tableInstance.set("cluster", "cluster-1");
        tableInstance.set("colo", "colo-1");

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

        // HashMap<String, Referenceable> columnsMap = new HashMap<>();
        ArrayList<Referenceable> columns = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            Referenceable columnInstance = new Referenceable("column_type");
            final String name = "column_" + index;
            columnInstance.set("name", name);
            columnInstance.set("type", "string");

            columns.add(columnInstance);
            // columnsMap.put(name, columnInstance);
        }
        tableInstance.set("columns", columns);
        // tableInstance.set("columnsMap", columnsMap);

        //        HashMap<String, Struct> partitionsMap = new HashMap<>();
        ArrayList<Struct> partitions = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            Struct partitionInstance = new Struct("partition_type");
            final String name = "partition_" + index;
            partitionInstance.set("name", name);

            partitions.add(partitionInstance);
            //            partitionsMap.put(name, partitionInstance);
        }
        tableInstance.set("partitions", partitions);
        //        tableInstance.set("partitionsMap", partitionsMap);

        HashMap<String, String> parametersMap = new HashMap<>();
        parametersMap.put("foo", "bar");
        parametersMap.put("bar", "baz");
        parametersMap.put("some", "thing");
        tableInstance.set("parametersMap", parametersMap);

        ClassType tableType = typeSystem.getDataType(ClassType.class, TABLE_TYPE);
        return tableType.convert(tableInstance, Multiplicity.REQUIRED);
    }

    private String random() {
        return RandomStringUtils.random(10);
    }

    @Test
    public void testUTFValues() throws Exception {
        Referenceable hrDept = new Referenceable("Department");
        Referenceable john = new Referenceable("Person");
        john.set("name", random());
        john.set("department", hrDept);

        hrDept.set("name", random());
        hrDept.set("employees", ImmutableList.of(john));

        ClassType deptType = typeSystem.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        guid = repositoryService.createEntity(hrDept2);
        Assert.assertNotNull(guid);
    }
}
