/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.discovery;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.BasicTestSetup;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.commons.collections.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.atlas.model.discovery.SearchParameters.*;
import static org.testng.Assert.assertEquals;

@Guice(modules = TestModules.TestOnlyModule.class)
public class BasicSearchClassificationTest extends BasicTestSetup {

    @Inject
    private AtlasDiscoveryService discoveryService;

    private int    totalEntities                        = 0;
    private int    totalClassifiedEntities              = 0;
    private int    getTotalClassifiedEntitiesHistorical = 0;
    private int    dimensionTagEntities                 = 10;
    private String dimensionTagDeleteGuid;
    private String dimensionalTagGuid;

    @BeforeClass
    public void setup() throws AtlasBaseException {
        setupTestData();
        createDimensionTaggedEntityAndDelete();
        createDimensionalTaggedEntityWithAttr();
    }

    @Test(priority = -1)
    public void searchByALLTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(ALL_CLASSIFICATION_TYPES);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));

        totalEntities = getEntityCount();
        totalClassifiedEntities = entityHeaders.size();
        getTotalClassifiedEntitiesHistorical = getEntityWithTagCountHistorical();
    }

    @Test
    public void searchByALLTagAndIndexSysFilters() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(ALL_CLASSIFICATION_TYPES);
        FilterCriteria filterCriteria = getSingleFilterCondition("__timestamp", Operator.LT, String.valueOf(System.currentTimeMillis()));
        params.setTagFilters(filterCriteria);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        assertEquals(entityHeaders.size(), totalClassifiedEntities);
    }

    @Test
    public void searchByALLTagAndIndexSysFiltersToTestLimit() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(ALL_CLASSIFICATION_TYPES);
        FilterCriteria filterCriteria = getSingleFilterCondition("__timestamp", Operator.LT, String.valueOf(System.currentTimeMillis()));
        params.setTagFilters(filterCriteria);
        params.setLimit(totalClassifiedEntities - 2);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        assertEquals(entityHeaders.size(), totalClassifiedEntities - 2);
    }

    @Test
    public void searchByNOTCLASSIFIED() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(NO_CLASSIFICATIONS);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        assertEquals(entityHeaders.size(), totalEntities - totalClassifiedEntities);
    }

    @Test
    public void searchByTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(DIMENSION_CLASSIFICATION);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        assertEquals(entityHeaders.size(), dimensionTagEntities);
    }

    @Test
    public void searchByTagAndTagFilters() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(DIMENSIONAL_CLASSIFICATION);
        FilterCriteria filterCriteria = getSingleFilterCondition("attr1", Operator.EQ, "Test");
        params.setTagFilters(filterCriteria);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        assertEquals(entityHeaders.size(), 1);
        assertEquals(entityHeaders.get(0).getGuid(), dimensionalTagGuid);

    }

    @Test
    public void searchByTagAndIndexSysFilters() throws AtlasBaseException {

        SearchParameters params = new SearchParameters();
        params.setClassification(DIMENSION_CLASSIFICATION);
        FilterCriteria filterCriteria = getSingleFilterCondition("__timestamp", Operator.LT, String.valueOf(System.currentTimeMillis()));
        params.setTagFilters(filterCriteria);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        assertEquals(entityHeaders.size(), dimensionTagEntities);
    }

    @Test
    public void searchByWildcardTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification("Dimension*");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        assertEquals(entityHeaders.size(), dimensionTagEntities + 1);

    }

    //@Test
    public void searchByTagAndGraphSysFilters() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(DIMENSION_CLASSIFICATION);
        FilterCriteria filterCriteria = getSingleFilterCondition("__entityStatus", Operator.EQ, "DELETED");
        params.setTagFilters(filterCriteria);
        params.setExcludeDeletedEntities(false);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        assertEquals(entityHeaders.size(), 1);
        assertEquals(entityHeaders.get(0).getGuid(), dimensionTagDeleteGuid);

    }

    private void createDimensionTaggedEntityAndDelete() throws AtlasBaseException {
        AtlasEntity entityToDelete = new AtlasEntity(HIVE_TABLE_TYPE);
        entityToDelete.setAttribute("name", "entity to be deleted");
        entityToDelete.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "entity.tobedeleted");

        List<AtlasClassification> cls = new ArrayList<>();
        cls.add(new AtlasClassification(DIMENSION_CLASSIFICATION));
        entityToDelete.setClassifications(cls);

        //create entity
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(new AtlasEntity.AtlasEntitiesWithExtInfo(entityToDelete)), false);
        AtlasEntityHeader entityHeader = response.getCreatedEntities().get(0);
        dimensionTagDeleteGuid = entityHeader.getGuid();

        //delete entity
        entityStore.deleteById(dimensionTagDeleteGuid);
    }

    private void createDimensionalTaggedEntityWithAttr() throws AtlasBaseException {
        AtlasEntity entityToDelete = new AtlasEntity(HIVE_TABLE_TYPE);
        entityToDelete.setAttribute("name", "Entity1");
        entityToDelete.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "entity.one");

        List<AtlasClassification> cls = new ArrayList<>();
        cls.add(new AtlasClassification(DIMENSIONAL_CLASSIFICATION, new HashMap<String, Object>() {{
            put("attr1", "Test");
        }}));
        entityToDelete.setClassifications(cls);

        //create entity
        final EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(new AtlasEntity.AtlasEntitiesWithExtInfo(entityToDelete)), false);
        AtlasEntityHeader entityHeader = response.getCreatedEntities().get(0);
        dimensionalTagGuid = entityHeader.getGuid();

    }

    private int getEntityCount() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(ALL_ENTITY_TYPES);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();
        return entityHeaders.size();
    }

    private int getEntityWithTagCountHistorical() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(ALL_CLASSIFICATION_TYPES);
        params.setExcludeDeletedEntities(false);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();
        return entityHeaders.size();
    }

}
