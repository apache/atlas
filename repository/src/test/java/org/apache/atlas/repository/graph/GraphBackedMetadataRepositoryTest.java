/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestUtils;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TraitNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createUniqueRequiredAttrDef;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * GraphBackedMetadataRepository test
 *
 * Guice loads the dependencies and injects the necessary objects
 *
 */
@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedMetadataRepositoryTest {

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @Inject
    private GraphBackedMetadataRepository repositoryService;

    @Inject
    private GraphBackedDiscoveryService discoveryService;

    private TypeSystem typeSystem;
    private String guid;
    private QueryParams queryParams = new QueryParams(100, 0);

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        new GraphBackedSearchIndexer(graphProvider);

        TestUtils.defineDeptEmployeeTypes(typeSystem);
        TestUtils.createHiveTypes(typeSystem);
    }

    @BeforeMethod
    public void setupContext() {
        RequestContext.createContext();
    }

    @AfterClass
    public void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
        try {
            //TODO - Fix failure during shutdown while using BDB
            graphProvider.get().shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //In some cases of parallel APIs, the edge is added, but get edge by label doesn't return the edge. ATLAS-1104
    @Test
    public void testConcurrentCalls() throws Exception {
        final HierarchicalTypeDefinition<ClassType> refType =
                createClassTypeDef(randomString(), ImmutableSet.<String>of());
        HierarchicalTypeDefinition<ClassType> type =
                createClassTypeDef(randomString(), ImmutableSet.<String>of(),
                        new AttributeDefinition("ref", refType.typeName, Multiplicity.OPTIONAL, true, null));
        typeSystem.defineClassType(refType);
        typeSystem.defineClassType(type);

        String refId1 = createEntity(new Referenceable(refType.typeName)).get(0);
        String refId2 = createEntity(new Referenceable(refType.typeName)).get(0);

        final Referenceable instance1 = new Referenceable(type.typeName);
        instance1.set("ref", new Referenceable(refId1, refType.typeName, null));

        final Referenceable instance2 = new Referenceable(type.typeName);
        instance2.set("ref", new Referenceable(refId2, refType.typeName, null));

        ExecutorService executor = Executors.newFixedThreadPool(3);
        List<Future<Object>> futures = new ArrayList<>();
        futures.add(executor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return createEntity(instance1).get(0);
            }
        }));
        futures.add(executor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return createEntity(instance2).get(0);
            }
        }));
        futures.add(executor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return discoveryService.searchByDSL(TestUtils.TABLE_TYPE, new QueryParams(10, 0));
            }
        }));

        String id1 = (String) futures.get(0).get();
        String id2 = (String) futures.get(1).get();
        futures.get(2).get();
        executor.shutdown();

        boolean validated1 = assertEdge(id1, type.typeName);
        boolean validated2 = assertEdge(id2, type.typeName);
        assertTrue(validated1 | validated2);
    }

    private boolean assertEdge(String id, String typeName) throws Exception {
        TitanGraph graph = graphProvider.get();
        Vertex vertex = (Vertex) graph.query().has(Constants.GUID_PROPERTY_KEY, id).vertices().iterator().next();
        Iterable<Edge> edges = vertex.getEdges(Direction.OUT, Constants.INTERNAL_PROPERTY_KEY_PREFIX + typeName + ".ref");
        if (!edges.iterator().hasNext()) {
            ITypedReferenceableInstance entity = repositoryService.getEntityDefinition(id);
            assertNotNull(entity.get("ref"));
            return true;
        }
        return false;
    }

    @Test
    public void testSubmitEntity() throws Exception {
        ITypedReferenceableInstance hrDept = TestUtils.createDeptEg1(typeSystem);

        List<String> guids = repositoryService.createEntities(hrDept);
        Assert.assertNotNull(guids);
        Assert.assertEquals(guids.size(), 5);
        guid = guids.get(4);
        Assert.assertNotNull(guid);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinitionForDepartment() throws Exception {
        ITypedReferenceableInstance entity = repositoryService.getEntityDefinition(guid);
        Assert.assertNotNull(entity);

        //entity state should be active by default
        Assert.assertEquals(entity.getId().getState(), Id.EntityState.ACTIVE);
    }

    @Test(expectedExceptions = EntityNotFoundException.class)
    public void testGetEntityDefinitionNonExistent() throws Exception {
        repositoryService.getEntityDefinition("blah");
        Assert.fail();
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityList() throws Exception {
        List<String> entityList = repositoryService.getEntityList(TestUtils.DEPARTMENT_TYPE);
        System.out.println("entityList = " + entityList);
        Assert.assertNotNull(entityList);
        Assert.assertTrue(entityList.contains(guid));
    }

    @Test
    public void testGetTypeAttributeName() throws Exception {
        Assert.assertEquals(repositoryService.getTypeAttributeName(), Constants.ENTITY_TYPE_PROPERTY_KEY);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetTraitLabel() throws Exception {
        Assert.assertEquals(
                repositoryService.getTraitLabel(typeSystem.getDataType(ClassType.class, TestUtils.TABLE_TYPE),
                        TestUtils.CLASSIFICATION), TestUtils.CLASSIFICATION);
    }

    @Test
    public void testCreateEntity() throws Exception {
        Referenceable databaseInstance = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance.set("name", TestUtils.DATABASE_NAME);
        databaseInstance.set("description", "foo database");
        databaseInstance.set("created", new Date(TestUtils.TEST_DATE_IN_LONG));

        databaseInstance.set("namespace", "colo:cluster:hive:db");
        databaseInstance.set("cluster", "cluster-1");
        databaseInstance.set("colo", "colo-1");
        System.out.println("databaseInstance = " + databaseInstance);

        ClassType dbType = typeSystem.getDataType(ClassType.class, TestUtils.DATABASE_TYPE);
        ITypedReferenceableInstance db = dbType.convert(databaseInstance, Multiplicity.REQUIRED);
        System.out.println("db = " + db);

        //Reuse the same database instance without id, with the same unique attribute
        ITypedReferenceableInstance table = createHiveTableInstance(databaseInstance);
        List<String> guids = createEntities(db, table);
        Assert.assertEquals(guids.size(), 7);   //1 db + 5 columns + 1 table. Shouldn't create db again
        System.out.println("added db = " + guids.get(0));
        System.out.println("added table = " + guids.get(6));
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testGetEntityDefinition() throws Exception {
        String guid = getGUID();

        ITypedReferenceableInstance table = repositoryService.getEntityDefinition(guid);
        Assert.assertEquals(table.getDate("created"), new Date(TestUtils.TEST_DATE_IN_LONG));
        System.out.println("*** table = " + table);
    }

    private List<String> createEntities(ITypedReferenceableInstance... instances) throws Exception {
        RequestContext.createContext();
        return repositoryService.createEntities(instances);
    }

    private List<String> createEntity(Referenceable entity) throws Exception {
        ClassType type = typeSystem.getDataType(ClassType.class, entity.getTypeName());
        ITypedReferenceableInstance instance = type.convert(entity, Multiplicity.REQUIRED);
        return createEntities(instance);
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
        GraphQuery query = graph.query().has(Constants.ENTITY_TYPE_PROPERTY_KEY, Compare.EQUAL, TestUtils.TABLE_TYPE);
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
        Assert.assertEquals(traitNames, Arrays.asList(new String[]{TestUtils.CLASSIFICATION}));
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

    @Test
    public void testMultipleTypesWithSameUniqueAttribute() throws Exception {
        //Two entities of different types(with same supertype that has the unique attribute) with same qualified name should succeed
        HierarchicalTypeDefinition<ClassType> supertype =
                createClassTypeDef(randomString(), ImmutableSet.<String>of(),
                        createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> t1 =
                createClassTypeDef(randomString(), ImmutableSet.of(supertype.typeName));
        HierarchicalTypeDefinition<ClassType> t2 =
                createClassTypeDef(randomString(), ImmutableSet.of(supertype.typeName));
        typeSystem.defineClassTypes(supertype, t1, t2);

        final String name = randomString();
        String id1 = createEntity(new Referenceable(t1.typeName) {{
            set("name", name);
        }}).get(0);
        String id2 = createEntity(new Referenceable(t2.typeName) {{
            set("name", name);
        }}).get(0);
        assertNotEquals(id1, id2);

        ITypedReferenceableInstance entity = repositoryService.getEntityDefinition(t1.typeName, "name", name);
        assertEquals(entity.getTypeName(), t1.typeName);
        assertEquals(entity.getId()._getId(), id1);

        entity = repositoryService.getEntityDefinition(t2.typeName, "name", name);
        assertEquals(entity.getTypeName(), t2.typeName);
        assertEquals(entity.getId()._getId(), id2);
    }

    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTrait() throws Exception {
        final String aGUID = getGUID();
        Vertex vertex = GraphHelper.getInstance().getVertexForGUID(aGUID);
        Long modificationTimestampPreUpdate = vertex.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);
        Assert.assertNotNull(modificationTimestampPreUpdate);

        List<String> traitNames = repositoryService.getTraitNames(aGUID);
        System.out.println("traitNames = " + traitNames);
        Assert.assertEquals(traitNames.size(), 1);
        Assert.assertTrue(traitNames.contains(TestUtils.CLASSIFICATION));
        Assert.assertFalse(traitNames.contains(TestUtils.PII));

        TraitType traitType = typeSystem.getDataType(TraitType.class, TestUtils.PII);
        ITypedStruct traitInstance = traitType.createInstance();

        repositoryService.addTrait(aGUID, traitInstance);

        // refresh trait names
        traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 2);
        Assert.assertTrue(traitNames.contains(TestUtils.PII));
        Assert.assertTrue(traitNames.contains(TestUtils.CLASSIFICATION));
        
        // Verify modification timestamp was updated.
        GraphHelper.getInstance().getVertexForGUID(aGUID);
        Long modificationTimestampPostUpdate = vertex.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);
        Assert.assertNotNull(modificationTimestampPostUpdate);
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testAddTraitWithAttribute() throws Exception {
        final String aGUID = getGUID();
        final String traitName = "P_I_I";

        HierarchicalTypeDefinition<TraitType> piiTrait = TypesUtil
            .createTraitTypeDef(traitName, ImmutableSet.<String>of(),
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
        TraitType traitType = typeSystem.getDataType(TraitType.class, TestUtils.PII);
        ITypedStruct traitInstance = traitType.createInstance();

        repositoryService.addTrait(UUID.randomUUID().toString(), traitInstance);
        Assert.fail();
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String aGUID = getGUID();
        Vertex vertex = GraphHelper.getInstance().getVertexForGUID(aGUID);
        Long modificationTimestampPreUpdate = vertex.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);
        Assert.assertNotNull(modificationTimestampPreUpdate);

        List<String> traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 3);
        Assert.assertTrue(traitNames.contains(TestUtils.PII));
        Assert.assertTrue(traitNames.contains(TestUtils.CLASSIFICATION));
        Assert.assertTrue(traitNames.contains("P_I_I"));

        repositoryService.deleteTrait(aGUID, TestUtils.PII);

        // refresh trait names
        traitNames = repositoryService.getTraitNames(aGUID);
        Assert.assertEquals(traitNames.size(), 2);
        Assert.assertTrue(traitNames.contains(TestUtils.CLASSIFICATION));
        Assert.assertFalse(traitNames.contains(TestUtils.PII));
        
        // Verify modification timestamp was updated.
        GraphHelper.getInstance().getVertexForGUID(aGUID);
        Long modificationTimestampPostUpdate = vertex.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);
        Assert.assertNotNull(modificationTimestampPostUpdate);
        Assert.assertTrue(modificationTimestampPostUpdate > modificationTimestampPreUpdate);
    }

    @Test(expectedExceptions = EntityNotFoundException.class)
    public void testDeleteTraitForNonExistentEntity() throws Exception {
        repositoryService.deleteTrait(UUID.randomUUID().toString(), TestUtils.PII);
        Assert.fail();
    }

    @Test(expectedExceptions = TraitNotFoundException.class)
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

        Id expected = new Id(guid, tableVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), TestUtils.TABLE_TYPE);
        Assert.assertEquals(GraphHelper.getIdFromVertex(TestUtils.TABLE_TYPE, tableVertex), expected);
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testGetTypeName() throws Exception {
        Vertex tableVertex = getTableEntityVertex();
        Assert.assertEquals(GraphHelper.getTypeName(tableVertex), TestUtils.TABLE_TYPE);
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testSearchByDSLQuery() throws Exception {
        String dslQuery = "hive_database as PII";
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery, queryParams);
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
            Assert.assertEquals(name, TestUtils.DATABASE_NAME);
        }
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testSearchByDSLWithInheritance() throws Exception {
        String dslQuery = "Person where name = 'Jane'";
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery, queryParams);
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

        TestUtils.dumpGraph(graphProvider.get());

        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery, queryParams);
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

        TestUtils.dumpGraph(graphProvider.get());

        //person in hr department whose name is john
        Thread.sleep(sleepInterval);
        String response = discoveryService.searchByFullText("john", queryParams);
        Assert.assertNotNull(response);
        JSONArray results = new JSONArray(response);
        Assert.assertEquals(results.length(), 1);
        JSONObject row = (JSONObject) results.get(0);
        Assert.assertEquals(row.get("typeName"), "Person");

        //person in hr department who lives in santa clara
        response = discoveryService.searchByFullText("Jane AND santa AND clara", queryParams);
        Assert.assertNotNull(response);
        results = new JSONArray(response);
        Assert.assertEquals(results.length(), 1);
        row = (JSONObject) results.get(0);
        Assert.assertEquals(row.get("typeName"), "Manager");

        //search for person in hr department whose name starts is john/jahn
        response = discoveryService.searchByFullText("hr AND (john OR jahn)", queryParams);
        Assert.assertNotNull(response);
        results = new JSONArray(response);
        Assert.assertEquals(results.length(), 1);
        row = (JSONObject) results.get(0);
        Assert.assertEquals(row.get("typeName"), "Person");

        //verify limit and offset
        //higher limit should return all results
        results = new JSONArray(discoveryService.searchByFullText("Department", queryParams));
        assertEquals(results.length(), 5);

        //smaller limit should return those many rows
        results = new JSONArray(discoveryService.searchByFullText("Department", new QueryParams(2, 0)));
        assertEquals(results.length(), 2);

        //offset should offset the results
        results = new JSONArray(discoveryService.searchByFullText("Department", new QueryParams(5, 2)));
        assertEquals(results.length(), 3);

        //higher offset shouldn't return any rows
        results = new JSONArray(discoveryService.searchByFullText("Department", new QueryParams(2, 6)));
        assertEquals(results.length(), 0);
    }

    private ITypedReferenceableInstance createHiveTableInstance(Referenceable databaseInstance) throws Exception {
        Referenceable tableInstance = new Referenceable(TestUtils.TABLE_TYPE, TestUtils.CLASSIFICATION);
        tableInstance.set("name", TestUtils.TABLE_NAME);
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
        tableInstance.set("created", new Date(TestUtils.TEST_DATE_IN_LONG));
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

        Struct traitInstance = (Struct) tableInstance.getTrait(TestUtils.CLASSIFICATION);
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
            Struct partitionInstance = new Struct(TestUtils.PARTITION_STRUCT_TYPE);
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

        ClassType tableType = typeSystem.getDataType(ClassType.class, TestUtils.TABLE_TYPE);
        return tableType.convert(tableInstance, Multiplicity.REQUIRED);
    }

    private String randomUTF() {
        return RandomStringUtils.random(10);
    }

    private String randomString() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    @Test
    public void testUTFValues() throws Exception {
        Referenceable hrDept = new Referenceable("Department");
        Referenceable john = new Referenceable("Person");
        john.set("name", randomUTF());
        john.set("department", hrDept);

        hrDept.set("name", randomUTF());
        hrDept.set("employees", ImmutableList.of(john));

        ClassType deptType = typeSystem.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        List<String> guids = repositoryService.createEntities(hrDept2);
        Assert.assertNotNull(guids);
        Assert.assertEquals(guids.size(), 2);
        Assert.assertNotNull(guids.get(0));
        Assert.assertNotNull(guids.get(1));
    }
}
