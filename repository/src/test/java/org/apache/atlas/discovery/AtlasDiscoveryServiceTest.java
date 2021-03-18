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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.BasicTestSetup;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.commons.collections.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.atlas.model.discovery.SearchParameters.*;
import static org.testng.Assert.*;
import static org.testng.Assert.assertNotNull;

@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasDiscoveryServiceTest extends BasicTestSetup {

    @Inject
    private AtlasDiscoveryService discoveryService;

    @BeforeClass
    public void setup() throws Exception {
        super.initialize();

        ApplicationProperties.get().setProperty(ApplicationProperties.ENABLE_FREETEXT_SEARCH_CONF, true);
        setupTestData();
        createDimensionalTaggedEntity("sales");
        createSpecialCharTestEntities();
    }

    /*  TermSearchProcessor(TSP),
        FreeTextSearchProcessor(FSP),
        ClassificationSearchProcessor(CSP),
        EntitySearchProcessor(ESP)  */

    @Test
    public void term() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTermName(SALES_TERM+"@"+SALES_GLOSSARY);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 10);
    }

    // TSP execute and CSP,ESP filter
    @Test
    public void term_tag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTermName(SALES_TERM+"@"+SALES_GLOSSARY);
        params.setClassification(METRIC_CLASSIFICATION);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        for(AtlasEntityHeader e : entityHeaders){
            System.out.println(e.toString());
        }
        assertEquals(entityHeaders.size(), 4);
    }

    @Test
    public void term_entity() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTermName(SALES_TERM+"@"+SALES_GLOSSARY);
        params.setTypeName(HIVE_TABLE_TYPE);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 10);
    }

    @Test
    public void term_entity_tag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTermName(SALES_TERM+"@"+SALES_GLOSSARY);
        params.setTypeName(HIVE_TABLE_TYPE);
        params.setClassification(DIMENSIONAL_CLASSIFICATION);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isEmpty(entityHeaders));
    }

    //FSP execute and CSP,ESP filter
    @Test
    public void query_ALLTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(ALL_CLASSIFICATION_TYPES);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 5);
    }

    @Test
    public void query_ALLTag_tagFilter() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(ALL_CLASSIFICATION_TYPES);
        //typeName will check for only classification name not propogated classification
        SearchParameters.FilterCriteria fc = getSingleFilterCondition("__typeName", Operator.NOT_CONTAINS, METRIC_CLASSIFICATION);
        params.setTagFilters(fc);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 4);
    }

    @Test
    public void query_NOTCLASSIFIEDTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(NO_CLASSIFICATIONS);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 1);
    }


    @Test
    public void query_ALLWildcardTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification("*");
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 5);
    }

    @Test
    public void query_wildcardTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification("Dimen*on");
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 2);
    }

    @Test
    public void query_tag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(METRIC_CLASSIFICATION);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 3);
    }

    @Test
    public void query_tag_tagFilter() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setClassification(METRIC_CLASSIFICATION);
        SearchParameters.FilterCriteria fc = getSingleFilterCondition("__timestamp", SearchParameters.Operator.LT, String.valueOf(System.currentTimeMillis()));
        params.setTagFilters(fc);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 3);
    }

    @Test
    public void query_entity() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 4);
    }

    @Test
    public void query_entity_entityFilter() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria fc = getSingleFilterCondition("tableType", Operator.NOT_NULL, "null");
        params.setEntityFilters(fc);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 3);
    }

    @Test
    public void query_entity_entityFilter_tag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria fc = getSingleFilterCondition("tableType", Operator.IS_NULL, "null");
        params.setEntityFilters(fc);
        params.setClassification(DIMENSIONAL_CLASSIFICATION);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 1);
    }

    @Test
    public void query_entity_entityFilter_tag_tagFilter() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria fcE = getSingleFilterCondition("tableType", Operator.IS_NULL, "null");
        params.setEntityFilters(fcE);
        params.setClassification(DIMENSIONAL_CLASSIFICATION);
        params.setQuery("sales");
        SearchParameters.FilterCriteria fcC = getSingleFilterCondition("attr1", Operator.EQ, "value1");
        params.setTagFilters(fcC);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 1);
    }

    @Test
    public void query_entity_tag_tagFilter() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        params.setClassification(METRIC_CLASSIFICATION);
        SearchParameters.FilterCriteria fc = getSingleFilterCondition("__timestamp", SearchParameters.Operator.LT, String.valueOf(System.currentTimeMillis()));
        params.setTagFilters(fc);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 2);

    }

    @Test
    public void query_entity_tag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        params.setClassification(METRIC_CLASSIFICATION);
        params.setQuery("sales");

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 2);
    }

    // CSP Execute and ESP filter
    @Test
    public void entity_entityFilter_tag_tagFilter() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria fcE = getSingleFilterCondition("tableType", Operator.EQ, "Managed");
        params.setEntityFilters(fcE);
        params.setClassification(METRIC_CLASSIFICATION);
        SearchParameters.FilterCriteria fcC = getSingleFilterCondition("__timestamp", SearchParameters.Operator.LT, String.valueOf(System.currentTimeMillis()));
        params.setTagFilters(fcC);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 4);

    }

    @Test
    public void entity_tag_tagFilter() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        params.setClassification(METRIC_CLASSIFICATION);
        SearchParameters.FilterCriteria fc = getSingleFilterCondition("__timestamp", SearchParameters.Operator.LT, String.valueOf(System.currentTimeMillis()));
        params.setTagFilters(fc);

        List<AtlasEntityHeader> entityHeaders = discoveryService.searchWithParameters(params).getEntities();

        Assert.assertTrue(CollectionUtils.isNotEmpty(entityHeaders));
        assertEquals(entityHeaders.size(), 4);
    }

    String spChar1   = "default.test_dot_name";
    String spChar2   = "default.test_dot_name@db.test_db";
    String spChar3   = "default.test_dot_name_12.col1@db1";
    String spChar4   = "default_.test_dot_name";

    String spChar5   = "default.test_colon_name:test_db";
    String spChar6   = "default.test_colon_name:-test_db";
    String spChar7   = "crn:def:default:name-76778-87e7-23@test";
    String spChar8   = "default.:test_db_name";

    String spChar9   = "default.customer's_name";
    String spChar10  = "default.customers'_data_name";

    String spChar11  = "search_with space@name";
    String spChar12  = "search_with space 123@name";

    //SearchProcessor.isIndexQuerySpecialChar
    String spChar13  = "search_with_special-char#having$and%inthename=attr";
    String spChar14  = "search_with_specialChar!name";
    String spChar15  = "search_with_star*in_name";
    String spChar16  = "search_with_star5.*5_inname";
    String spChar17  = "search_quest?n_name";

    String spChar18  = "/warehouse/tablespace/external/hive/name/hortonia_bank";
    String spChar19  = "/warehouse/tablespace/external/hive/name/exis_bank";


    @DataProvider(name = "specialCharSearchContains")
    private Object[][] specialCharSearchContains() {
        return new Object[][]{
                {"name",Operator.CONTAINS,"test_dot",4},
                {"name",Operator.CONTAINS,"test_dot_name_",1},
                {"name",Operator.CONTAINS,"test_colon_name",2},
                {"name",Operator.CONTAINS,"def:default:name",1},
                {"name",Operator.CONTAINS,"space 12",1},
                {"name",Operator.CONTAINS,"with space",2},
                {"name",Operator.CONTAINS,"Char!name",1},
                {"name",Operator.CONTAINS,"with_star",2},
                {"name",Operator.CONTAINS,"/external/hive/name/",2},

                {"name",Operator.CONTAINS,"test_dot_name@db",1},
                {"name",Operator.CONTAINS,"name@db",1},
                {"name",Operator.CONTAINS,"def:default:name-",1},
                {"name",Operator.CONTAINS,"star*in",1},
                {"name",Operator.CONTAINS,"Char!na",1},
                {"name",Operator.CONTAINS,"ith spac",2},
                {"name",Operator.CONTAINS,"778-87",1},

                {"qualifiedName",Operator.CONTAINS,"test_dot",4},
                {"qualifiedName",Operator.CONTAINS,"test_dot_qf_",1},
                {"qualifiedName",Operator.CONTAINS,"test_colon_qf",2},
                {"qualifiedName",Operator.CONTAINS,"def:default:qf",1},
                {"qualifiedName",Operator.CONTAINS,"space 12",1},
                {"qualifiedName",Operator.CONTAINS,"with space",2},
                {"qualifiedName",Operator.CONTAINS,"Char!qf",1},
                {"qualifiedName",Operator.CONTAINS,"with_star",2},
                {"qualifiedName",Operator.CONTAINS,"/external/hive/qf/",2},

                {"qualifiedName",Operator.CONTAINS,"test_dot_qf@db",1},
                {"qualifiedName",Operator.CONTAINS,"qf@db",1},
                {"qualifiedName",Operator.CONTAINS,"def:default:qf-",1},
                {"qualifiedName",Operator.CONTAINS,"star*in",1},
                {"qualifiedName",Operator.CONTAINS,"Char!q",1},
                {"qualifiedName",Operator.CONTAINS,"ith spac",2},
                {"qualifiedName",Operator.CONTAINS,"778-87",1},
        };
    }

    @DataProvider(name = "specialCharSearchName")
    private Object[][] specialCharSearchName() {
        return new Object[][]{

                {"name",Operator.STARTS_WITH,"default.test_dot_",3},

                {"name",Operator.STARTS_WITH,"default.test_dot_name@db.test",1},
                {"name",Operator.STARTS_WITH,"default.test_dot_name@db.",1},
                {"name",Operator.STARTS_WITH,"default.test_dot_name",3},
                {"name",Operator.ENDS_WITH,"test_db",3},

                {"name",Operator.STARTS_WITH,"default.test_dot_name_12.col1@db",1},
                {"name",Operator.STARTS_WITH,"default.test_dot_name_12.col1@",1},
                {"name",Operator.STARTS_WITH,"default.test_dot_name_12.col1",1},
                {"name",Operator.STARTS_WITH,"default.test_dot_name_12.col",1},
                {"name",Operator.STARTS_WITH,"default.test_dot_name_12.",1},
                {"name",Operator.STARTS_WITH,"default.test_dot_name_12",1},
                {"name",Operator.ENDS_WITH,"col1@db1",1},

                {"name",Operator.STARTS_WITH,"default_.test_dot",1},
                {"name",Operator.ENDS_WITH,"test_dot_name",2},

                {"name",Operator.STARTS_WITH,"default.test_colon_name:test_",1},

                {"name",Operator.STARTS_WITH,"default.test_colon_name:-test_",1},
                {"name",Operator.STARTS_WITH,"default.test_colon_name:-",1},
                {"name",Operator.STARTS_WITH,"default.test_colon",2},

                {"name",Operator.STARTS_WITH,"crn:def:default:name-76778-87e7-23@",1},
                {"name",Operator.STARTS_WITH,"crn:def:default:name-76778-87e7-",1},
                {"name",Operator.STARTS_WITH,"crn:def:default:",1},

                {"name",Operator.STARTS_WITH,"default.:test_db",1},
                {"name",Operator.ENDS_WITH,"test_db_name",1},

                {"name",Operator.STARTS_WITH,"default.customer's",1},
                {"name",Operator.ENDS_WITH,"mer's_name",1},

                {"name",Operator.STARTS_WITH,"default.customers'_data",1},
                {"name",Operator.ENDS_WITH,"customers'_data_name",1},

                {"name",Operator.STARTS_WITH,"search_with space",2},
                {"name",Operator.STARTS_WITH,"search_with space ",1},
                {"name",Operator.STARTS_WITH,"search_with space 123@",1},
                {"name",Operator.STARTS_WITH,"search_with space 1",1},

                {"name",Operator.STARTS_WITH,"search_with_special-char#having$and%inthename=",1},
                {"name",Operator.STARTS_WITH,"search_with_special-char#having$and%in",1},
                {"name",Operator.STARTS_WITH,"search_with_special-char#having$",1},
                {"name",Operator.STARTS_WITH,"search_with_special-char#h",1},
                {"name",Operator.STARTS_WITH,"search_with_special",2},
                {"name",Operator.STARTS_WITH,"search_with_spe",2},

                {"name",Operator.STARTS_WITH,"search_with_specialChar!",1},

                {"name",Operator.STARTS_WITH,"search_with_star*in",1},

                {"name",Operator.ENDS_WITH,"5.*5_inname",1},
                {"name",Operator.STARTS_WITH,"search_with_star5.*5_",1},

                {"name",Operator.STARTS_WITH,"search_quest?n_",1},

                {"name",Operator.STARTS_WITH,"/warehouse/tablespace/external/hive/name/hortonia",1},
                {"name",Operator.STARTS_WITH,"/warehouse/tablespace/external/hive/name/",2},

        };
    }

    @DataProvider(name = "specialCharSearchQFName")
    private Object[][] specialCharSearchQFName() {
        return new Object[][]{

                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_",3},

                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_qf@db.test",1},
                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_qf@db.",1},
                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_qf",3},
                {"qualifiedName",Operator.ENDS_WITH,"test_db",3},

                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_qf_12.col1@db",1},
                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_qf_12.col1@",1},
                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_qf_12.col1",1},
                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_qf_12.col",1},
                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_qf_12.",1},
                {"qualifiedName",Operator.STARTS_WITH,"default.test_dot_qf_12",1},
                {"qualifiedName",Operator.ENDS_WITH,"col1@db1",1},

                {"qualifiedName",Operator.STARTS_WITH,"default_.test_dot",1},
                {"qualifiedName",Operator.ENDS_WITH,"test_dot_qf",2},

                {"qualifiedName",Operator.STARTS_WITH,"default.test_colon_qf:test_",1},

                {"qualifiedName",Operator.STARTS_WITH,"default.test_colon_qf:-test_",1},
                {"qualifiedName",Operator.STARTS_WITH,"default.test_colon_qf:-",1},
                {"qualifiedName",Operator.STARTS_WITH,"default.test_colon",2},

                {"qualifiedName",Operator.STARTS_WITH,"crn:def:default:qf-76778-87e7-23@",1},
                {"qualifiedName",Operator.STARTS_WITH,"crn:def:default:qf-76778-87e7-",1},
                {"qualifiedName",Operator.STARTS_WITH,"crn:def:default:",1},

                {"qualifiedName",Operator.STARTS_WITH,"default.:test_db",1},
                {"qualifiedName",Operator.ENDS_WITH,"test_db_qf",1},

                {"qualifiedName",Operator.STARTS_WITH,"default.customer's",1},
                {"qualifiedName",Operator.ENDS_WITH,"mer's_qf",1},

                {"qualifiedName",Operator.STARTS_WITH,"default.customers'_data",1},
                {"qualifiedName",Operator.ENDS_WITH,"customers'_data_qf",1},

                {"qualifiedName",Operator.STARTS_WITH,"search_with space",2},
                {"qualifiedName",Operator.STARTS_WITH,"search_with space ",1},
                {"qualifiedName",Operator.STARTS_WITH,"search_with space 123@",1},
                {"qualifiedName",Operator.STARTS_WITH,"search_with space 1",1},

                {"qualifiedName",Operator.STARTS_WITH,"search_with_special-char#having$and%intheqf=",1},
                {"qualifiedName",Operator.STARTS_WITH,"search_with_special-char#having$and%in",1},
                {"qualifiedName",Operator.STARTS_WITH,"search_with_special-char#having$",1},
                {"qualifiedName",Operator.STARTS_WITH,"search_with_special-char#h",1},
                {"qualifiedName",Operator.STARTS_WITH,"search_with_special",2},
                {"qualifiedName",Operator.STARTS_WITH,"search_with_spe",2},

                {"qualifiedName",Operator.STARTS_WITH,"search_with_specialChar!",1},

                {"qualifiedName",Operator.STARTS_WITH,"search_with_star*in",1},

                {"qualifiedName",Operator.ENDS_WITH,"5.*5_inqf",1},
                {"qualifiedName",Operator.STARTS_WITH,"search_with_star5.*5_",1},

                {"qualifiedName",Operator.STARTS_WITH,"search_quest?n_",1},

                {"qualifiedName",Operator.STARTS_WITH,"/warehouse/tablespace/external/hive/qf/hortonia",1},
                {"qualifiedName",Operator.STARTS_WITH,"/warehouse/tablespace/external/hive/qf/",2},

        };
    }


    @DataProvider(name = "specialCharSearchEQ")
    private Object[][] specialCharSearch() {
        return new Object[][]{
                {"name",Operator.EQ,spChar1,1},
                {"name",Operator.EQ,spChar2,1},
                {"name",Operator.EQ,spChar3,1},
                {"name",Operator.EQ,spChar4,1},
                {"name",Operator.EQ,spChar5,1},
                {"name",Operator.EQ,spChar6,1},
                {"name",Operator.EQ,spChar7,1},
                {"name",Operator.EQ,spChar8,1},
                {"name",Operator.EQ,spChar9,1},
                {"name",Operator.EQ,spChar10,1},
                {"name",Operator.EQ,spChar11,1},
                {"name",Operator.EQ,spChar12,1},
                {"name",Operator.EQ,spChar13,1},
                {"name",Operator.EQ,spChar14,1},
                {"name",Operator.EQ,spChar15,1},
                {"name",Operator.EQ,spChar16,1},
                {"name",Operator.EQ,spChar17,1},
                {"name",Operator.EQ,spChar18,1},
                {"name",Operator.EQ,spChar19,1},

                {"qualifiedName",Operator.EQ,spChar1.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar2.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar3.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar4.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar5.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar6.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar7.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar8.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar9.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar10.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar11.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar12.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar13.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar14.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar15.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar16.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar17.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar18.replace("name","qf"),1},
                {"qualifiedName",Operator.EQ,spChar19.replace("name","qf"),1},
        };
    }


    public void createSpecialCharTestEntities() throws AtlasBaseException {

        List<String> nameList = Arrays.asList(spChar1,spChar2,spChar3,spChar4,spChar5,spChar6,spChar7,spChar8,spChar9,spChar10,spChar11,spChar12,spChar13,spChar14,spChar15,spChar16,spChar17,spChar18,spChar19);
        for (String nameStr : nameList) {
            AtlasEntity entityToDelete = new AtlasEntity(HIVE_TABLE_TYPE);
            entityToDelete.setAttribute("name", nameStr);
            entityToDelete.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "qualifiedName"+System.currentTimeMillis());

            //create entity
            EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(new AtlasEntity.AtlasEntitiesWithExtInfo(entityToDelete)), false);
        }

        List<String> qfList = nameList;

        for (String qfStr : qfList) {
            qfStr = qfStr.replace("name","qf");
            AtlasEntity entityToDelete = new AtlasEntity(HIVE_TABLE_TYPE);
            entityToDelete.setAttribute("name", "name"+System.currentTimeMillis());
            entityToDelete.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, qfStr);

            //create entity
            EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(new AtlasEntity.AtlasEntitiesWithExtInfo(entityToDelete)), false);

        }
    }

    @Test(dataProvider = "specialCharSearchEQ")
    public void specialCharSearchAssertEq(String attrName, SearchParameters.Operator operator, String attrValue, int expected) throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition(attrName,operator, attrValue);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

       AtlasSearchResult searchResult = discoveryService.searchWithParameters(params);
        assertSearchResult(searchResult,expected, attrValue);
    }

    @Test(dataProvider = "specialCharSearchContains")
    public void specialCharSearchAssertContains(String attrName, SearchParameters.Operator operator, String attrValue, int expected) throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition(attrName,operator, attrValue);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        AtlasSearchResult searchResult = discoveryService.searchWithParameters(params);
        assertSearchResult(searchResult,expected, attrValue);
    }

    @Test(dataProvider = "specialCharSearchName")
    public void specialCharSearchAssertName(String attrName, SearchParameters.Operator operator, String attrValue, int expected) throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition(attrName,operator, attrValue);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        AtlasSearchResult searchResult = discoveryService.searchWithParameters(params);
        assertSearchResult(searchResult,expected, attrValue);
    }

    @Test(dataProvider = "specialCharSearchQFName")
    public void specialCharSearchAssertQFName(String attrName, SearchParameters.Operator operator, String attrValue, int expected) throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition(attrName,operator, attrValue);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        AtlasSearchResult searchResult = discoveryService.searchWithParameters(params);
        assertSearchResult(searchResult,expected, attrValue);
    }

    private void assertSearchResult(AtlasSearchResult searchResult, int expected, String query) {
        assertNotNull(searchResult);
        if(expected == 0) {
            assertTrue(searchResult.getAttributes() == null || CollectionUtils.isEmpty(searchResult.getAttributes().getValues()));
            assertNull(searchResult.getEntities(), query);
        } else if(searchResult.getEntities() != null) {
            assertEquals(searchResult.getEntities().size(), expected, query);
        } else {
            assertNotNull(searchResult.getAttributes());
            assertNotNull(searchResult.getAttributes().getValues());
            assertEquals(searchResult.getAttributes().getValues().size(), expected, query);
        }
    }

    private void createDimensionalTaggedEntity(String name) throws AtlasBaseException {
        EntityMutationResponse resp = createDummyEntity(name, HIVE_TABLE_TYPE);
        AtlasEntityHeader entityHeader = resp.getCreatedEntities().get(0);
        String guid = entityHeader.getGuid();
        HashMap<String,Object> attr = new HashMap<>();
        attr.put("attr1","value1");
        entityStore.addClassification(Arrays.asList(guid), new AtlasClassification(DIMENSIONAL_CLASSIFICATION, attr));
    }

    @AfterClass
    public void teardown() throws Exception {
        AtlasGraphProvider.cleanup();

        super.cleanup();
    }
}
