/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.CreateUpdateEntitiesResult;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtils;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

@Test
@Guice(modules = TestModules.TestOnlyModule.class)
public class GraphRepoMapperScaleTest {

    private static final String DATABASE_NAME = "foo";
    private static final String TABLE_NAME = "bar";

    @Inject
    private GraphBackedMetadataRepository repositoryService;

    @Inject
    private GraphBackedSearchIndexer searchIndexer;

    private TypeSystem typeSystem = TypeSystem.getInstance();

    private String dbGUID;

    @BeforeClass
    @GraphTransaction
    public void setUp() throws Exception {
        //force up front graph initialization
        TestUtils.getGraph();
        searchIndexer = new GraphBackedSearchIndexer(new AtlasGraphProvider(), ApplicationProperties.get(), new AtlasTypeRegistry());
        //Make sure we can cleanup the index directory
        Collection<IDataType> typesAdded = TestUtils.createHiveTypes(typeSystem);
        searchIndexer.onAdd(typesAdded);
    }

    @BeforeMethod
    public void setupContext() {
        TestUtils.resetRequestContext();
    }

    @AfterClass
    public void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
//        AtlasGraphProvider.cleanup();
    }

    @Test
    public void testSubmitEntity() throws Exception {
        Referenceable databaseInstance = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");
        // System.out.println("databaseInstance = " + databaseInstance);

        ClassType dbType = typeSystem.getDataType(ClassType.class, TestUtils.DATABASE_TYPE);
        ITypedReferenceableInstance db = dbType.convert(databaseInstance, Multiplicity.REQUIRED);

        dbGUID = result(db).getCreatedEntities().get(0);

        Referenceable dbInstance = new Referenceable(dbGUID, TestUtils.DATABASE_TYPE, databaseInstance.getValuesMap());

        for (int index = 0; index < 1000; index++) {
            ITypedReferenceableInstance table = createHiveTableInstance(dbInstance, index);
            result(table);
        }
    }

    private CreateUpdateEntitiesResult result(ITypedReferenceableInstance db)
            throws RepositoryException, EntityExistsException {
        return repositoryService.createEntities(db);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testSearchIndex() throws Exception {

        //Elasticsearch requires some time before index is updated
        Thread.sleep(5000);
        searchWithOutIndex(Constants.GUID_PROPERTY_KEY, dbGUID);
        searchWithOutIndex(Constants.ENTITY_TYPE_PROPERTY_KEY, "column_type");
        searchWithOutIndex(Constants.ENTITY_TYPE_PROPERTY_KEY, TestUtils.TABLE_TYPE);

        searchWithOutIndex("hive_table.name", "bar-999");
        searchWithIndex("hive_table.name", "bar-999");
        searchWithIndex("hive_table.created", ComparisionOperator.GREATER_THAN_EQUAL, TestUtils.TEST_DATE_IN_LONG, 1000);

        for (int index = 500; index < 600; index++) {
            searchWithIndex("hive_table.name", "bar-" + index);
        }

        searchWithIndex(Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
    }

    private void searchWithOutIndex(String key, String value) {
        AtlasGraph graph = TestUtils.getGraph();
        long start = System.currentTimeMillis();
        int count = 0;
        try {
            AtlasGraphQuery query = graph.query().has(key, ComparisionOperator.EQUAL, value);
            Iterable<AtlasVertex> result = query.vertices();
            for (AtlasVertex ignored : result) {
                count++;
            }
        } finally {
            System.out.println("Search on [" + key + "=" + value + "] returned results: " + count + ", took " + (
                    System.currentTimeMillis() - start) + " ms");
        }
    }


    private void searchWithIndex(String key, String value) {
        AtlasGraph graph = TestUtils.getGraph();
        long start = System.currentTimeMillis();
        int count = 0;
        try {
            String queryString = "v.\"" + key + "\":(" + value + ")";
            AtlasIndexQuery query = graph.indexQuery(Constants.VERTEX_INDEX, queryString);
            Iterator<AtlasIndexQuery.Result> result = query.vertices();
            while(result.hasNext()) {
                result.next();
                count++;
            }
        } finally {
            System.out.println("Search on [" + key + "=" + value + "] returned results: " + count + ", took " + (
                    System.currentTimeMillis() - start) + " ms");
        }
    }
    
    private void searchWithIndex(String key, ComparisionOperator op, Object value, int expectedResults) {
        AtlasGraph graph = TestUtils.getGraph();
        long start = System.currentTimeMillis();
        int count = 0;
        try {
            AtlasGraphQuery query = graph.query().has(key, op, value);
            Iterable<AtlasVertex> itrble = query.vertices();
            for (AtlasVertex ignored : itrble) {
                count++;
            }
        } finally {
            System.out.println("Search on [" + key + "=" + value + "] returned results: " + count + ", took " + (
                    System.currentTimeMillis() - start) + " ms");
            Assert.assertEquals(count, expectedResults);
        }
    }

    private ITypedReferenceableInstance createHiveTableInstance(Referenceable databaseInstance, int uberIndex)
    throws Exception {

        Referenceable tableInstance = new Referenceable(TestUtils.TABLE_TYPE);
        tableInstance.set("name", TABLE_NAME + "-" + uberIndex);
        tableInstance.set("description", "bar table" + "-" + uberIndex);
        tableInstance.set("type", "managed");
        tableInstance.set("created", new Date(TestUtils.TEST_DATE_IN_LONG));
        tableInstance.set("tableType", 1); // enum

        // refer to an existing class
        tableInstance.set("database", databaseInstance);

        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.add("first_name" + "-" + uberIndex);
        columnNames.add("last_name" + "-" + uberIndex);
        tableInstance.set("columnNames", columnNames);

        Struct serde1Instance = new Struct("serdeType");
        serde1Instance.set("name", "serde1" + "-" + uberIndex);
        serde1Instance.set("serde", "serde1" + "-" + uberIndex);
        tableInstance.set("serde1", serde1Instance);

        Struct serde2Instance = new Struct("serdeType");
        serde2Instance.set("name", "serde2" + "-" + uberIndex);
        serde2Instance.set("serde", "serde2" + "-" + uberIndex);
        tableInstance.set("serde2", serde2Instance);

        ArrayList<Referenceable> columns = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            Referenceable columnInstance = new Referenceable("column_type");
            columnInstance.set("name", "column_" + "-" + uberIndex + "-" + index);
            columnInstance.set("type", "string");
            columns.add(columnInstance);
        }
        tableInstance.set("columns", columns);

        ArrayList<Struct> partitions = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            Struct partitionInstance = new Struct(TestUtils.PARTITION_STRUCT_TYPE);
            partitionInstance.set("name", "partition_" + "-" + uberIndex + "-" + index);
            partitions.add(partitionInstance);
        }
        tableInstance.set("partitions", partitions);

        ClassType tableType = typeSystem.getDataType(ClassType.class, TestUtils.TABLE_TYPE);
        return tableType.convert(tableInstance, Multiplicity.REQUIRED);
    }
}

