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
import org.apache.atlas.BasicTestSetup;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.commons.collections.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.atlas.model.discovery.SearchParameters.*;
import static org.testng.Assert.assertEquals;

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
        assignGlossary();
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
