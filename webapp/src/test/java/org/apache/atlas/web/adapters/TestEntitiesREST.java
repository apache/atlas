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
package org.apache.atlas.web.adapters;

import static org.apache.atlas.TestUtilsV2.CLASSIFICATION;
import static org.apache.atlas.TestUtilsV2.COLUMN_TYPE;
import static org.apache.atlas.TestUtilsV2.DATABASE_TYPE;
import static org.apache.atlas.TestUtilsV2.FETL_CLASSIFICATION;
import static org.apache.atlas.TestUtilsV2.PHI;
import static org.apache.atlas.TestUtilsV2.PII;
import static org.apache.atlas.TestUtilsV2.TABLE_TYPE;
import static org.apache.atlas.model.discovery.SearchParameters.FilterCriteria.Condition.AND;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.ClassificationAssociateRequest;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.web.rest.DiscoveryREST;
import org.apache.atlas.web.rest.EntityREST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Guice(modules = {TestModules.TestOnlyModule.class})
public class TestEntitiesREST {

    private static final Logger LOG = LoggerFactory.getLogger(TestEntitiesREST.class);

    @Inject
    private AtlasTypeRegistry typeRegistry;
    @Inject
    private AtlasTypeDefStore typeStore;
    @Inject
    private DiscoveryREST     discoveryREST;
    @Inject
    private EntityREST        entityREST;

    private AtlasEntity                       dbEntity;
    private AtlasEntity                       tableEntity;
    private AtlasEntity                       tableEntity2;
    private List<AtlasEntity>                 columns;
    private List<AtlasEntity>                 columns2;
    private SearchParameters                  searchParameters;
    private Map<String, List<String>>         createdGuids     = new HashMap<>();
    private Map<String, AtlasClassification>  tagMap           = new HashMap<>();

    @BeforeClass
    public void setUp() throws Exception {
        AtlasTypesDef[] testTypesDefs = new AtlasTypesDef[] {  TestUtilsV2.defineHiveTypes() };

        for (AtlasTypesDef typesDef : testTypesDefs) {
            AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(typesDef, typeRegistry);

            if (!typesToCreate.isEmpty()) {
                typeStore.createTypesDef(typesToCreate);
            }
        }

        createEntities();

        initTagMap();

        registerEntities();

        addTagTo(CLASSIFICATION, TABLE_TYPE);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        RequestContext.clear();
    }

    @Test
    public void testBasicSearch() throws Exception {
        // search entities with classification named classification
        searchParameters = new SearchParameters();
        searchParameters.setIncludeSubClassifications(true);
        searchParameters.setClassification(TestUtilsV2.CLASSIFICATION);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);
    }

    @Test(dependsOnMethods = "testBasicSearch")
    public void testWildCardBasicSearch() throws Exception {

        //table - classification
        searchParameters = new SearchParameters();

        searchParameters.setClassification("*");
        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        searchParameters.setClassification("_CLASSIFIED");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        // Test wildcard usage of basic search
        searchParameters.setClassification("cl*");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        searchParameters.setClassification("*ion");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        searchParameters.setClassification("*l*");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        searchParameters.setClassification("_NOT_CLASSIFIED");
        searchParameters.setTypeName(DATABASE_TYPE);
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 1);
    }

    @Test(dependsOnMethods = "testWildCardBasicSearch")
    public void testBasicSearchWithAttr() throws Exception{
        searchParameters = new SearchParameters();
        searchParameters.setClassification("cla*");
        searchParameters.setTypeName(TABLE_TYPE);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);

        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        // table - classification
        // column - phi
        addTagTo(PHI, COLUMN_TYPE);

        FilterCriteria filterCriteria = new FilterCriteria();
        filterCriteria.setAttributeName("stringAttr");
        filterCriteria.setOperator(SearchParameters.Operator.CONTAINS);
        filterCriteria.setAttributeValue("sample");

        // basic search with tag filterCriteria
        searchParameters = new SearchParameters();
        searchParameters.setClassification(PHI);
        searchParameters.setTagFilters(filterCriteria);

        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        filterCriteria.setAttributeName("stringAttr");
        filterCriteria.setOperator(SearchParameters.Operator.EQ);
        filterCriteria.setAttributeValue("sample_string");

        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        filterCriteria.setAttributeValue("SAMPLE_STRING");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNull(res.getEntities());
    }

    @Test(dependsOnMethods = "testWildCardBasicSearch")
    public void testBasicSearchWithSubTypes() throws Exception{

        // basic search with subtypes
        searchParameters = new SearchParameters();
        searchParameters.setClassification(TestUtilsV2.CLASSIFICATION);
        searchParameters.setIncludeSubClassifications(true);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);

        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        // table - classification
        // database - fetl_classification
        addTagTo(FETL_CLASSIFICATION, DATABASE_TYPE);

        final AtlasClassification result_tag = entityREST.getClassification(createdGuids.get(DATABASE_TYPE).get(0), TestUtilsV2.FETL_CLASSIFICATION);
        Assert.assertNotNull(result_tag);
        Assert.assertEquals(result_tag.getTypeName(), FETL_CLASSIFICATION);

        // basic search with subtypes
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 3);

        // basic search without subtypes
        searchParameters.setIncludeSubClassifications(false);
        res = discoveryREST.searchWithParameters(searchParameters);

        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);
    }

    @Test(dependsOnMethods = "testWildCardBasicSearch")
    public void testGraphQueryFilter() throws Exception {

        // database - pii, felt_classification
        // table - pii, classification,
        // col - phi
        searchParameters = new SearchParameters();
        searchParameters.setQuery("sample_string");
        searchParameters.setClassification(PHI);

        SearchParameters.FilterCriteria fc = new SearchParameters.FilterCriteria();
        fc.setOperator(SearchParameters.Operator.EQ);
        fc.setAttributeName("booleanAttr");
        fc.setAttributeValue("true");

        searchParameters.setTagFilters(fc);
        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);
        Assert.assertEquals(res.getEntities().get(0).getTypeName(), COLUMN_TYPE);

        AtlasClassification cls = new AtlasClassification(TestUtilsV2.PHI, new HashMap<String, Object>() {{
            put("stringAttr", "sample_string");
            put("booleanAttr", false);
            put("integerAttr", 100);
        }});

        ClassificationAssociateRequest clsAssRequest = new ClassificationAssociateRequest(Collections.singletonList(createdGuids.get(TABLE_TYPE).get(0)), cls);
        entityREST.addClassification(clsAssRequest);

        final AtlasClassification result_tag = entityREST.getClassification(createdGuids.get(TABLE_TYPE).get(0), TestUtilsV2.PHI);
        Assert.assertNotNull(result_tag);
        Assert.assertEquals(result_tag.getTypeName(), PHI);

        fc.setAttributeValue("false");
        res = discoveryREST.searchWithParameters(searchParameters);

        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 1);
        Assert.assertEquals(res.getEntities().get(0).getTypeName(), TABLE_TYPE);
    }

    @Test(dependsOnMethods = "testBasicSearch")
    public void testBasicSearchWithFilter() throws Exception {

        //table - classification
        searchParameters = new SearchParameters();
        searchParameters.setIncludeSubClassifications(false);
        searchParameters.setClassification(TestUtilsV2.CLASSIFICATION);

        SearchParameters.FilterCriteria fc = new SearchParameters.FilterCriteria();
        fc.setOperator(SearchParameters.Operator.CONTAINS);
        fc.setAttributeValue("new comments");
        fc.setAttributeName("tag");

        searchParameters.setTagFilters(fc);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNull(res.getEntities());

        fc.setOperator(SearchParameters.Operator.ENDS_WITH);
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNull(res.getEntities());

        fc.setOperator(SearchParameters.Operator.STARTS_WITH);
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNull(res.getEntities());
    }

    @Test
    public void testSearchByMultiSystemAttributes() throws Exception {

        searchParameters = new SearchParameters();
        searchParameters.setTypeName("_ALL_ENTITY_TYPES");
        SearchParameters.FilterCriteria fc = new SearchParameters.FilterCriteria();
        SearchParameters.FilterCriteria subFc1 = new SearchParameters.FilterCriteria();
        SearchParameters.FilterCriteria subFc2 = new SearchParameters.FilterCriteria();

        subFc1.setAttributeName(MODIFICATION_TIMESTAMP_PROPERTY_KEY);
        subFc1.setOperator(SearchParameters.Operator.LT);
        subFc1.setAttributeValue(String.valueOf(System.currentTimeMillis()));

        subFc2.setAttributeName(TIMESTAMP_PROPERTY_KEY);
        subFc2.setOperator(SearchParameters.Operator.LT);
        subFc2.setAttributeValue(String.valueOf(System.currentTimeMillis()));

        fc.setCriterion(Arrays.asList(subFc1, subFc2));
        fc.setCondition(AND);
        searchParameters.setEntityFilters(fc);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertTrue(res.getEntities().size() > 5);
    }

    @Test
    public void testSearchBySingleSystemAttribute() throws Exception {
        searchParameters = new SearchParameters();
        searchParameters.setTypeName("_ALL_ENTITY_TYPES");

        SearchParameters.FilterCriteria fc = new SearchParameters.FilterCriteria();
        fc.setAttributeName(STATE_PROPERTY_KEY);
        fc.setOperator(SearchParameters.Operator.EQ);
        fc.setAttributeValue("DELETED");

        searchParameters.setEntityFilters(fc);

        entityREST.deleteByGuid(createdGuids.get(DATABASE_TYPE).get(0));

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 1);

        searchParameters.setTypeName("_ALL_ENTITY_TYPES");
        fc.setAttributeName(TYPE_NAME_PROPERTY_KEY);
        fc.setOperator(SearchParameters.Operator.EQ);
        fc.setAttributeValue(TABLE_TYPE);

        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        searchParameters = new SearchParameters();
        searchParameters.setTypeName(TABLE_TYPE);
        fc.setAttributeName(STATE_PROPERTY_KEY);
        fc.setOperator(SearchParameters.Operator.EQ);
        fc.setAttributeValue("DELETED");

        searchParameters.setEntityFilters(fc);
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNull(res.getEntities());
    }

    @Test
    public void testSearchBySystemAttributesWithQuery() throws Exception {
        searchParameters = new SearchParameters();
        searchParameters.setTypeName("_ALL_ENTITY_TYPES");
        SearchParameters.FilterCriteria fc = new SearchParameters.FilterCriteria();
        SearchParameters.FilterCriteria subFc1 = new SearchParameters.FilterCriteria();
        SearchParameters.FilterCriteria subFc2 = new SearchParameters.FilterCriteria();

        subFc1.setAttributeName(TIMESTAMP_PROPERTY_KEY);
        subFc1.setOperator(SearchParameters.Operator.LT);
        subFc1.setAttributeValue(String.valueOf(System.currentTimeMillis()));

        subFc2.setAttributeName(STATE_PROPERTY_KEY);
        subFc2.setOperator(SearchParameters.Operator.EQ);
        subFc2.setAttributeValue("DELETED");

        fc.setCriterion(Arrays.asList(subFc1, subFc2));
        fc.setCondition(AND);

        searchParameters.setEntityFilters(fc);
        searchParameters.setQuery("sample_string");

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNull(res.getEntities());
    }

    @Test(dependsOnMethods = "testBasicSearchWithSubTypes")
    public void testUpdateWithSerializedEntities() throws  Exception {

        //Check with serialization and deserialization of entity attributes for the case
        // where attributes which are de-serialized into a map
        AtlasEntity dbEntity    = TestUtilsV2.createDBEntity();
        AtlasEntity tableEntity = TestUtilsV2.createTableEntity(dbEntity);

        final AtlasEntity colEntity = TestUtilsV2.createColumnEntity(tableEntity);
        List<AtlasEntity> columns = new ArrayList<AtlasEntity>() {{ add(colEntity); }};
        tableEntity.setAttribute("columns", getObjIdList(columns));

        AtlasEntity newDBEntity = serDeserEntity(dbEntity);
        AtlasEntity newTableEntity = serDeserEntity(tableEntity);

        AtlasEntitiesWithExtInfo newEntities = new AtlasEntitiesWithExtInfo();
        newEntities.addEntity(newDBEntity);
        newEntities.addEntity(newTableEntity);
        for (AtlasEntity column : columns) {
            newEntities.addReferredEntity(serDeserEntity(column));
        }

        EntityMutationResponse response2 = entityREST.createOrUpdate(newEntities);

        List<AtlasEntityHeader> newGuids = response2.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);
        Assert.assertNotNull(newGuids);
        Assert.assertEquals(newGuids.size(), 3);
    }

    @Test
    public void testGetEntities() throws Exception {

        final AtlasEntitiesWithExtInfo response = entityREST.getByGuids(createdGuids.get(DATABASE_TYPE), false, false);
        final List<AtlasEntity> entities = response.getEntities();

        Assert.assertNotNull(entities);
        Assert.assertEquals(entities.size(), 1);
        verifyAttributes(entities);
    }

	/* Disabled until EntityREST.deleteByIds() is implemented
	 *
    @Test(dependsOnMethods = "testGetEntities")
    public void testDeleteEntities() throws Exception {

        final EntityMutationResponse response = entityREST.deleteByGuids(createdGuids);
        final List<AtlasEntityHeader> entities = response.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE);

        Assert.assertNotNull(entities);
        Assert.assertEquals(entities.size(), 3);
    }
	*
	*/

    private void createEntities() {
        dbEntity     = TestUtilsV2.createDBEntity();
        tableEntity  = TestUtilsV2.createTableEntity(dbEntity);
        tableEntity2 = TestUtilsV2.createTableEntity(dbEntity);

        final AtlasEntity colEntity  = TestUtilsV2.createColumnEntity(tableEntity);
        final AtlasEntity colEntity2 = TestUtilsV2.createColumnEntity(tableEntity2);
        columns  = new ArrayList<AtlasEntity>() {{ add(colEntity); }};
        columns2 = new ArrayList<AtlasEntity>() {{ add(colEntity2); }};

        tableEntity.setAttribute("columns", getObjIdList(columns));
        tableEntity2.setAttribute("columns", getObjIdList(columns2));
    }

    private void initTagMap() {
        tagMap.put(PHI, new AtlasClassification(TestUtilsV2.PHI, new HashMap<String, Object>() {{
            put("stringAttr", "sample_string");
            put("booleanAttr", true);
            put("integerAttr", 100);
        }}));

        tagMap.put(PII, new AtlasClassification(TestUtilsV2.PII, new HashMap<String, Object>() {{ put("cls", "clsName"); }}));

        tagMap.put(CLASSIFICATION, new AtlasClassification(TestUtilsV2.CLASSIFICATION, new HashMap<String, Object>() {{ put("tag", "tagName"); }}));

        tagMap.put(FETL_CLASSIFICATION, new AtlasClassification(FETL_CLASSIFICATION, new HashMap<String, Object>() {{ put("cls", "clsName"); put("tag", "tagName"); }}));
    }

    private void addTagTo(String tagName, String type) throws Exception {
        AtlasClassification tag = tagMap.get(tagName);
        ClassificationAssociateRequest classificationAssociateRequest = new ClassificationAssociateRequest(createdGuids.get(type), tag);
        entityREST.addClassification(classificationAssociateRequest);

        for (int i = 0; i < createdGuids.get(type).size() - 1; i++) {
            final AtlasClassification result_tag = entityREST.getClassification(createdGuids.get(type).get(i), tagName);
            Assert.assertNotNull(result_tag);
            Assert.assertEquals(result_tag.getTypeName(), tag.getTypeName());
        }
    }

    private void registerEntities() throws Exception {
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();

        entities.addEntity(dbEntity);
        entities.addEntity(tableEntity);
        entities.addEntity(tableEntity2);

        for (AtlasEntity column : columns) {
            entities.addReferredEntity(column);
        }

        for (AtlasEntity column : columns2) {
            entities.addReferredEntity(column);
        }

        EntityMutationResponse response = entityREST.createOrUpdate(entities);
        List<AtlasEntityHeader> guids = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        Assert.assertNotNull(guids);
        Assert.assertEquals(guids.size(), 5);

        for (AtlasEntityHeader header : guids) {
            if (!createdGuids.containsKey(header.getTypeName())) {
                createdGuids.put(header.getTypeName(), new ArrayList<>());
            }
            createdGuids.get(header.getTypeName()).add(header.getGuid());
        }
    }

    private void verifyAttributes(List<AtlasEntity> retrievedEntities) throws Exception {
        AtlasEntity retrievedDBEntity = null;
        AtlasEntity retrievedTableEntity = null;
        AtlasEntity retrievedColumnEntity = null;
        for (AtlasEntity entity:  retrievedEntities ) {
            if ( entity.getTypeName().equals(DATABASE_TYPE)) {
                retrievedDBEntity = entity;
            }

            if ( entity.getTypeName().equals(TABLE_TYPE)) {
                retrievedTableEntity = entity;
            }

            if ( entity.getTypeName().equals(TestUtilsV2.COLUMN_TYPE)) {
                retrievedColumnEntity = entity;
            }
        }

        if ( retrievedDBEntity != null) {
            LOG.info("verifying entity of type {} ", dbEntity.getTypeName());
            verifyAttributes(retrievedDBEntity.getAttributes(), dbEntity.getAttributes());
        }

        if ( retrievedColumnEntity != null) {
            LOG.info("verifying entity of type {} ", columns.get(0).getTypeName());
            Assert.assertEquals(columns.get(0).getAttribute(AtlasClient.NAME), retrievedColumnEntity.getAttribute(AtlasClient.NAME));
            Assert.assertEquals(columns.get(0).getAttribute("type"), retrievedColumnEntity.getAttribute("type"));
        }

        if ( retrievedTableEntity != null) {
            LOG.info("verifying entity of type {} ", tableEntity.getTypeName());

            //String
            Assert.assertEquals(tableEntity.getAttribute(AtlasClient.NAME), retrievedTableEntity.getAttribute(AtlasClient.NAME));
            //Map
            Assert.assertEquals(tableEntity.getAttribute("parametersMap"), retrievedTableEntity.getAttribute("parametersMap"));
            //enum
            Assert.assertEquals(tableEntity.getAttribute("tableType"), retrievedTableEntity.getAttribute("tableType"));
            //date
            Assert.assertEquals(tableEntity.getAttribute("created"), retrievedTableEntity.getAttribute("created"));
            //array of Ids
            Assert.assertEquals(((List<AtlasObjectId>) retrievedTableEntity.getAttribute("columns")).get(0).getGuid(), retrievedColumnEntity.getGuid());
            //array of structs
            Assert.assertEquals(((List<AtlasStruct>) retrievedTableEntity.getAttribute("partitions")), tableEntity.getAttribute("partitions"));
        }
    }

    public static void verifyAttributes(Map<String, Object> actual, Map<String, Object> expected) throws Exception {
        for (String name : actual.keySet() ) {
            LOG.info("verifying attribute {} ", name);

            if ( expected.get(name) != null) {
                Assert.assertEquals(actual.get(name), expected.get(name));
            }
        }
    }

    AtlasEntity serDeserEntity(AtlasEntity entity) throws IOException {
        //Convert from json to object and back to trigger the case where it gets translated to a map for attributes instead of AtlasEntity
        String      jsonString = AtlasType.toJson(entity);
        AtlasEntity newEntity  = AtlasType.fromJson(jsonString, AtlasEntity.class);

        return newEntity;
    }

    private List<AtlasObjectId> getObjIdList(Collection<AtlasEntity> entities) {
        List<AtlasObjectId> ret = new ArrayList<>();

        for (AtlasEntity entity : entities) {
            ret.add(AtlasTypeUtil.getAtlasObjectId(entity));
        }

        return ret;
    }
}