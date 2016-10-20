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

package org.apache.atlas.lineage;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.BaseRepositoryTest;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.discovery.EntityLineageService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.commons.collections.ArrayStack;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.*;

/**
 * Unit tests for the new v2 Instance LineageService.
 */
@Guice(modules = RepositoryMetadataModule.class)
public class EntityLineageServiceTest extends BaseRepositoryTest {

    @Inject
    private EntityLineageService lineageService;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterClass
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Circular Lineage Test.
     */
    @Test
    public void testCircularLineage() throws Exception{
        String entityGuid = getEntityId(HIVE_TABLE_TYPE, "name", "table2");
        AtlasLineageInfo circularLineage = getInputLineageInfo(entityGuid, 5);

        assertNotNull(circularLineage);
        System.out.println("circular lineage = " + circularLineage);

        Map<String, AtlasEntityHeader> entities = circularLineage.getGuidEntityMap();
        assertNotNull(entities);

        Set<LineageRelation> relations = circularLineage.getRelations();
        assertNotNull(relations);

        Assert.assertEquals(entities.size(), 4);
        Assert.assertEquals(relations.size(), 4);
        Assert.assertEquals(circularLineage.getLineageDepth(), 5);
        Assert.assertEquals(circularLineage.getLineageDirection(), LineageDirection.INPUT);

        assertTrue(entities.containsKey(circularLineage.getBaseEntityGuid()));
    }

    /**
     * Input Lineage Tests.
     */
    @Test(dataProvider = "invalidQueryParamsProvider")
    public void testGetInputLineageInfoInvalidParams(final String guid, final AtlasLineageInfo.LineageDirection direction, final int depth, AtlasErrorCode errorCode) throws Exception {
        testInvalidQueryParams(errorCode, new Invoker() {
            @Override
            void run() throws AtlasBaseException {
                lineageService.getAtlasLineageInfo(guid, direction, depth);
            }
        });
    }

    @Test
    public void testGetInputLineageInfo() throws Exception {
        String entityGuid = getEntityId(HIVE_TABLE_TYPE, "name", "sales_fact_monthly_mv");
        AtlasLineageInfo inputLineage = getInputLineageInfo(entityGuid, 4);

        assertNotNull(inputLineage);
        System.out.println("input lineage = " + inputLineage);

        Map<String, AtlasEntityHeader> entities = inputLineage.getGuidEntityMap();
        assertNotNull(entities);

        Set<LineageRelation> relations = inputLineage.getRelations();
        assertNotNull(relations);

        Assert.assertEquals(entities.size(), 6);
        Assert.assertEquals(relations.size(), 5);
        Assert.assertEquals(inputLineage.getLineageDepth(), 4);
        Assert.assertEquals(inputLineage.getLineageDirection(), LineageDirection.INPUT);

        assertTrue(entities.containsKey(inputLineage.getBaseEntityGuid()));
    }

    /**
     * Output Lineage Tests.
     */
    @Test(dataProvider = "invalidQueryParamsProvider")
    public void testGetOutputLineageInvalidParams(final String guid, final LineageDirection direction, final int depth, AtlasErrorCode errorCode) throws Exception {
        testInvalidQueryParams(errorCode, new Invoker() {
            @Override
            void run() throws AtlasBaseException {
                lineageService.getAtlasLineageInfo(guid, direction, depth);
            }
        });
    }

    @Test
    public void testGetOutputLineageInfo() throws Exception {
        String entityGuid = getEntityId(HIVE_TABLE_TYPE, "name", "sales_fact");
        AtlasLineageInfo outputLineage = getOutputLineageInfo(entityGuid, 4);

        assertNotNull(outputLineage);
        System.out.println("output lineage = " + outputLineage);

        Map<String, AtlasEntityHeader> entities = outputLineage.getGuidEntityMap();
        assertNotNull(entities);

        Set<LineageRelation> relations = outputLineage.getRelations();
        assertNotNull(relations);

        Assert.assertEquals(entities.size(), 5);
        Assert.assertEquals(relations.size(), 4);
        Assert.assertEquals(outputLineage.getLineageDepth(), 4);
        Assert.assertEquals(outputLineage.getLineageDirection(), LineageDirection.OUTPUT);

        assertTrue(entities.containsKey(outputLineage.getBaseEntityGuid()));
    }

    /**
     * Both Lineage Tests.
     */
    @Test(dataProvider = "invalidQueryParamsProvider")
    public void testGetLineageInfoInvalidParams(final String guid, final LineageDirection direction, final int depth, AtlasErrorCode errorCode) throws Exception {
        testInvalidQueryParams(errorCode, new Invoker() {
            @Override
            void run() throws AtlasBaseException {
                lineageService.getAtlasLineageInfo(guid, direction, depth);
            }
        });
    }

    @Test
    public void testGetLineageInfo() throws Exception {
        String entityGuid = getEntityId(HIVE_TABLE_TYPE, "name", "sales_fact_monthly_mv");
        AtlasLineageInfo bothLineage = getBothLineageInfo(entityGuid, 5);

        assertNotNull(bothLineage);
        System.out.println("both lineage = " + bothLineage);

        Map<String, AtlasEntityHeader> entities = bothLineage.getGuidEntityMap();
        assertNotNull(entities);

        Set<LineageRelation> relations = bothLineage.getRelations();
        assertNotNull(relations);

        Assert.assertEquals(entities.size(), 6);
        Assert.assertEquals(relations.size(), 5);
        Assert.assertEquals(bothLineage.getLineageDepth(), 5);
        Assert.assertEquals(bothLineage.getLineageDirection(), AtlasLineageInfo.LineageDirection.BOTH);

        assertTrue(entities.containsKey(bothLineage.getBaseEntityGuid()));
    }

    @DataProvider(name = "invalidQueryParamsProvider")
    private Object[][] params() throws Exception {
        String entityGuid = getEntityId(HIVE_TABLE_TYPE, "name", "sales_fact_monthly_mv");

        // String guid, LineageDirection direction, int depth, AtlasErrorCode errorCode

        return new Object[][]{
                {"", null, 0, AtlasErrorCode.INSTANCE_GUID_NOT_FOUND},
                {" ", null, 0, AtlasErrorCode.INSTANCE_GUID_NOT_FOUND},
                {null, null, 0, AtlasErrorCode.INSTANCE_GUID_NOT_FOUND},
                {"invalidGuid", LineageDirection.OUTPUT, 6, AtlasErrorCode.INSTANCE_GUID_NOT_FOUND},
                {entityGuid, null, -10, AtlasErrorCode.INSTANCE_LINEAGE_INVALID_PARAMS},
                {entityGuid, null, 5, AtlasErrorCode.INSTANCE_LINEAGE_INVALID_PARAMS}
        };
    }

    abstract class Invoker {
        abstract void run() throws AtlasBaseException;
    }

    public void testInvalidQueryParams(AtlasErrorCode expectedErrorCode, Invoker Invoker) throws Exception {
        try {
            Invoker.run();
            fail("Expected " + expectedErrorCode.toString());
        } catch(AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), expectedErrorCode);
        }
    }

    private AtlasLineageInfo getInputLineageInfo(String guid, int depth) throws Exception {
        return lineageService.getAtlasLineageInfo(guid, LineageDirection.INPUT, depth);
    }

    private AtlasLineageInfo getOutputLineageInfo(String guid, int depth) throws Exception {
        return lineageService.getAtlasLineageInfo(guid, AtlasLineageInfo.LineageDirection.OUTPUT, depth);
    }

    private AtlasLineageInfo getBothLineageInfo(String guid, int depth) throws Exception {
        return lineageService.getAtlasLineageInfo(guid, AtlasLineageInfo.LineageDirection.BOTH, depth);
    }

    @Test
    public void testNewLineageWithDelete() throws Exception {
        String tableName = "table" + random();
        createTable(tableName, 3, true);
        String entityGuid = getEntityId(HIVE_TABLE_TYPE, "name", tableName);

        AtlasLineageInfo inputLineage = getInputLineageInfo(entityGuid, 5);
        assertNotNull(inputLineage);
        System.out.println("input lineage = " + inputLineage);

        Map<String, AtlasEntityHeader> entitiesInput = inputLineage.getGuidEntityMap();
        assertNotNull(entitiesInput);
        assertEquals(entitiesInput.size(), 3);

        Set<LineageRelation> relationsInput = inputLineage.getRelations();
        assertNotNull(relationsInput);
        assertEquals(relationsInput.size(), 2);

        AtlasEntityHeader tableEntityInput = entitiesInput.get(entityGuid);
        assertEquals(tableEntityInput.getStatus(), Status.STATUS_ACTIVE);

        AtlasLineageInfo outputLineage = getOutputLineageInfo(entityGuid, 5);
        assertNotNull(outputLineage);
        System.out.println("output lineage = " + outputLineage);

        Map<String, AtlasEntityHeader> entitiesOutput = outputLineage.getGuidEntityMap();
        assertNotNull(entitiesOutput);
        assertEquals(entitiesOutput.size(), 3);

        Set<LineageRelation> relationsOutput = outputLineage.getRelations();
        assertNotNull(relationsOutput);
        assertEquals(relationsOutput.size(), 2);

        AtlasEntityHeader tableEntityOutput = entitiesOutput.get(entityGuid);
        assertEquals(tableEntityOutput.getStatus(), Status.STATUS_ACTIVE);

        AtlasLineageInfo bothLineage = getBothLineageInfo(entityGuid, 5);
        assertNotNull(bothLineage);
        System.out.println("both lineage = " + bothLineage);

        Map<String, AtlasEntityHeader> entitiesBoth = bothLineage.getGuidEntityMap();
        assertNotNull(entitiesBoth);
        assertEquals(entitiesBoth.size(), 5);

        Set<LineageRelation> relationsBoth = bothLineage.getRelations();
        assertNotNull(relationsBoth);
        assertEquals(relationsBoth.size(), 4);

        AtlasEntityHeader tableEntityBoth = entitiesBoth.get(entityGuid);
        assertEquals(tableEntityBoth.getStatus(), Status.STATUS_ACTIVE);

        //Delete the table entity. Lineage for entity returns the same results as before.
        //Lineage for table name throws EntityNotFoundException
        AtlasClient.EntityResult deleteResult = repository.deleteEntities(Arrays.asList(entityGuid));
        assertTrue(deleteResult.getDeletedEntities().contains(entityGuid));

        inputLineage = getInputLineageInfo(entityGuid, 5);
        tableEntityInput = inputLineage.getGuidEntityMap().get(entityGuid);
        assertEquals(tableEntityInput.getStatus(), Status.STATUS_DELETED);
        assertEquals(inputLineage.getGuidEntityMap().size(), 3);

        outputLineage = getOutputLineageInfo(entityGuid, 5);
        tableEntityOutput = outputLineage.getGuidEntityMap().get(entityGuid);
        assertEquals(tableEntityOutput.getStatus(), Status.STATUS_DELETED);
        assertEquals(outputLineage.getGuidEntityMap().size(), 3);

        bothLineage = getBothLineageInfo(entityGuid, 5);
        tableEntityBoth = bothLineage.getGuidEntityMap().get(entityGuid);
        assertEquals(tableEntityBoth.getStatus(), Status.STATUS_DELETED);
        assertEquals(bothLineage.getGuidEntityMap().size(), 5);

    }

    private void createTable(String tableName, int numCols, boolean createLineage) throws Exception {
        String dbId = getEntityId(DATABASE_TYPE, "name", "Sales");
        Id salesDB = new Id(dbId, 0, DATABASE_TYPE);

        //Create the entity again and schema should return the new schema
        List<Referenceable> columns = new ArrayStack();
        for (int i = 0; i < numCols; i++) {
            columns.add(column("col" + random(), "int", "column descr"));
        }

        Referenceable sd =
                storageDescriptor("hdfs://host:8000/apps/warehouse/sales", "TextInputFormat", "TextOutputFormat", true,
                        ImmutableList.of(column("time_id", "int", "time id")));

        Id table = table(tableName, "test table", salesDB, sd, "fetl", "External", columns);
        if (createLineage) {
            Id inTable = table("table" + random(), "test table", salesDB, sd, "fetl", "External", columns);
            Id outTable = table("table" + random(), "test table", salesDB, sd, "fetl", "External", columns);
            loadProcess("process" + random(), "hive query for monthly summary", "Tim ETL", ImmutableList.of(inTable),
                    ImmutableList.of(table), "create table as select ", "plan", "id", "graph", "ETL");
            loadProcess("process" + random(), "hive query for monthly summary", "Tim ETL", ImmutableList.of(table),
                    ImmutableList.of(outTable), "create table as select ", "plan", "id", "graph", "ETL");
        }
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(5);
    }

    private String getEntityId(String typeName, String attributeName, String attributeValue) throws Exception {
        return repository.getEntityDefinition(typeName, attributeName, attributeValue).getId()._getId();
    }
}
