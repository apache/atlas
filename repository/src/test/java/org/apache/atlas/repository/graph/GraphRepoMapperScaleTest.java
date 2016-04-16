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

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
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

@Test
@Guice(modules = RepositoryMetadataModule.class)
public class GraphRepoMapperScaleTest {

    private static final String DATABASE_NAME = "foo";
    private static final String TABLE_NAME = "bar";

    @Inject
    GraphProvider<TitanGraph> graphProvider;

    @Inject
    private GraphBackedMetadataRepository repositoryService;

    @Inject
    private GraphBackedSearchIndexer searchIndexer;

    private TypeSystem typeSystem = TypeSystem.getInstance();

    private String dbGUID;

    @BeforeClass
    @GraphTransaction
    public void setUp() throws Exception {
        //Make sure we can cleanup the index directory
        Collection<IDataType> typesAdded = TestUtils.createHiveTypes(typeSystem);
        searchIndexer.onAdd(typesAdded);
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

    @Test
    public void testSubmitEntity() throws Exception {
        Referenceable databaseInstance = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");
        // System.out.println("databaseInstance = " + databaseInstance);

        ClassType dbType = typeSystem.getDataType(ClassType.class, TestUtils.DATABASE_TYPE);
        ITypedReferenceableInstance db = dbType.convert(databaseInstance, Multiplicity.REQUIRED);

        dbGUID = repositoryService.createEntities(db).get(0);

        Referenceable dbInstance = new Referenceable(dbGUID, TestUtils.DATABASE_TYPE, databaseInstance.getValuesMap());

        for (int index = 0; index < 1000; index++) {
            ITypedReferenceableInstance table = createHiveTableInstance(dbInstance, index);
            repositoryService.createEntities(table);
        }
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
        searchWithIndex("hive_table.created", Compare.GREATER_THAN_EQUAL, TestUtils.TEST_DATE_IN_LONG, 1000);

        for (int index = 500; index < 600; index++) {
            searchWithIndex("hive_table.name", "bar-" + index);
        }
    }

    private void searchWithOutIndex(String key, String value) {
        TitanGraph graph = graphProvider.get();
        long start = System.currentTimeMillis();
        int count = 0;
        try {
            GraphQuery query = graph.query().has(key, Compare.EQUAL, value);
            for (Vertex ignored : query.vertices()) {
                count++;
            }
        } finally {
            System.out.println("Search on [" + key + "=" + value + "] returned results: " + count + ", took " + (
                    System.currentTimeMillis() - start) + " ms");
        }
    }

    private void searchWithIndex(String key, String value) {
        TitanGraph graph = graphProvider.get();
        long start = System.currentTimeMillis();
        int count = 0;
        try {
            String queryString = "v.\"" + key + "\":(" + value + ")";
            TitanIndexQuery query = graph.indexQuery(Constants.VERTEX_INDEX, queryString);
            for (TitanIndexQuery.Result<Vertex> ignored : query.vertices()) {
                count++;
            }
        } finally {
            System.out.println("Search on [" + key + "=" + value + "] returned results: " + count + ", took " + (
                System.currentTimeMillis() - start) + " ms");
        }
    }

    private void  searchWithIndex(String key, Predicate searchPredicate, Object value, int expectedResults) {
        TitanGraph graph = graphProvider.get();
        long start = System.currentTimeMillis();
        int count = 0;
        try {
            GraphQuery query = graph.query().has(key, searchPredicate, value);
            for (Vertex ignored : query.vertices()) {
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

