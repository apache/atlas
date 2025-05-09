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

import com.google.common.collect.Sets;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.BasicTestSetup;
import org.apache.atlas.SortOrder;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules = TestModules.TestOnlyModule.class)
public class EntitySearchProcessorTest extends BasicTestSetup {
    private static final Logger LOG = LoggerFactory.getLogger(EntitySearchProcessorTest.class);

    private static final SimpleDateFormat FORMATTED_DATE       = new SimpleDateFormat("dd-MMM-yyyy");
    private static final String           EXPECTED_ENTITY_NAME = "hive_Table_Null_tableType";

    @Inject
    public  GraphBackedSearchIndexer indexer;

    @Inject
    private AtlasGraph graph;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private EntityGraphRetriever entityRetriever;

    private String cjkGuid1;
    private       String cjkGuid2;

    @BeforeClass
    public void setup() throws Exception {
        super.initialize();

        setupTestData();
        createJapaneseEntityWithDescription();
        createChineseEntityWithDescription();
        FORMATTED_DATE.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Test
    public void searchTablesByClassification() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();

        params.setTypeName("hive_column");
        params.setClassification("PII");
        params.setLimit(10);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, Collections.emptySet());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);

        assertEquals(processor.getResultCount(), 4);
        assertEquals(processor.execute().size(), 4);
    }

    @Test
    public void searchByClassificationSortBy() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();

        params.setTypeName("hive_table");
        params.setClassification("Metric");
        params.setLimit(10);
        params.setSortBy("createTime");
        params.setSortOrder(SortOrder.ASCENDING);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, Collections.emptySet());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(processor.getResultCount(), 4);
        assertEquals(vertices.size(), 4);

        AtlasVertex firstVertex = vertices.get(0);
        Date        firstDate   = (Date) entityRetriever.toAtlasEntityHeader(firstVertex, Sets.newHashSet(params.getSortBy())).getAttribute(params.getSortBy());

        AtlasVertex secondVertex = vertices.get(1);
        Date        secondDate   = (Date) entityRetriever.toAtlasEntityHeader(secondVertex, Sets.newHashSet(params.getSortBy())).getAttribute(params.getSortBy());

        assertTrue(firstDate.before(secondDate));
    }

    @Test
    public void emptySearchByClassification() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();

        params.setTypeName("hive_table");
        params.setClassification("PII");
        params.setLimit(10);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, Collections.emptySet());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);

        assertEquals(processor.getResultCount(), 0);
        assertEquals(processor.execute().size(), 0);
    }

    @Test(expectedExceptions = AtlasBaseException.class, expectedExceptionsMessageRegExp = "NotExisting: Unknown/invalid classification")
    public void searchByNonExistingClassification() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();

        params.setTypeName("hive_process");
        params.setClassification("NotExisting");
        params.setLimit(10);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());

        new EntitySearchProcessor(context);
    }

    @Test(priority = -1)
    public void searchWithNEQ_stringAttr() throws AtlasBaseException {
        createDummyEntity(EXPECTED_ENTITY_NAME, HIVE_TABLE_TYPE);

        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("tableType", SearchParameters.Operator.NEQ, "Managed");
        SearchParameters                params         = new SearchParameters();

        params.setTypeName(HIVE_TABLE_TYPE);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 3);

        List<String> nameList = new ArrayList<>();

        for (AtlasVertex vertex : vertices) {
            nameList.add((String) entityRetriever.toAtlasEntityHeader(vertex, Collections.singleton("name")).getAttribute("name"));
        }

        assertTrue(nameList.contains(EXPECTED_ENTITY_NAME));
    }

    @Test
    public void searchWithNEQ_pipeSeperatedAttr() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("__classificationNames", SearchParameters.Operator.NEQ, METRIC_CLASSIFICATION);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 7);

        List<String> nameList = new ArrayList<>();
        for (AtlasVertex vertex : vertices) {
            nameList.add((String) entityRetriever.toAtlasEntityHeader(vertex, Collections.singleton("name")).getAttribute("name"));
        }

        assertTrue(nameList.contains(EXPECTED_ENTITY_NAME));
    }

    @Test
    public void searchWithNEQ_doubleAttr() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("retention", SearchParameters.Operator.NEQ, "5");
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 1);

        List<String> nameList = new ArrayList<>();
        for (AtlasVertex vertex : vertices) {
            nameList.add((String) entityRetriever.toAtlasEntityHeader(vertex, Collections.singleton("name")).getAttribute("name"));
        }

        assertTrue(nameList.contains("hive_Table_Null_tableType"));
    }

    @Test
    public void allEntityType() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(SearchParameters.ALL_ENTITY_TYPES);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());

        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        assertEquals(processor.execute().size(), 20);
    }

    @Test
    public void allEntityTypeWithTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(SearchParameters.ALL_ENTITY_TYPES);
        params.setClassification(FACT_CLASSIFICATION);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());

        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        assertEquals(processor.execute().size(), 5);
    }

    @Test
    public void entityType() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(DATABASE_TYPE);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());

        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        assertEquals(processor.execute().size(), 3);
    }

    @Test
    public void entityTypes() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(DATABASE_TYPE + "," + HIVE_TABLE_TYPE);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());

        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        assertEquals(processor.execute().size(), 14);
    }

    @Test(expectedExceptions = AtlasBaseException.class, expectedExceptionsMessageRegExp = "Not_Exists: Unknown/invalid typename")
    public void entityTypesNotAllowed() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName("Not_Exists");
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());
    }

    @Test(expectedExceptions = AtlasBaseException.class, expectedExceptionsMessageRegExp = "Attribute tableType not found for type " + DATABASE_TYPE)
    public void entityFiltersNotAllowed() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(DATABASE_TYPE + "," + HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("tableType", SearchParameters.Operator.CONTAINS, "ETL");
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());
    }

    @Test
    public void entityTypesAndTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(DATABASE_TYPE + "," + HIVE_TABLE_TYPE);
        params.setClassification(FACT_CLASSIFICATION);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());

        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        assertEquals(processor.execute().size(), 3);
    }

    @Test
    public void searchWithEntityTypesAndEntityFilters() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(DATABASE_TYPE + "," + HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("owner", SearchParameters.Operator.CONTAINS, "ETL");
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());

        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        assertEquals(processor.execute().size(), 4);
    }

    @Test
    public void searchWithEntityTypesAndEntityFiltersAndTag() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(DATABASE_TYPE + "," + HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("owner", SearchParameters.Operator.CONTAINS, "ETL");
        params.setEntityFilters(filterCriteria);
        params.setClassification(LOGDATA_CLASSIFICATION);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.emptySet());

        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        assertEquals(processor.execute().size(), 2);
    }

    @Test
    public void searchWithNotContains_stringAttr() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("tableType", SearchParameters.Operator.NOT_CONTAINS, "Managed");
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 3);

        List<String> nameList = new ArrayList<>();
        for (AtlasVertex vertex : vertices) {
            nameList.add((String) entityRetriever.toAtlasEntityHeader(vertex, Collections.singleton("name")).getAttribute("name"));
        }

        assertTrue(nameList.contains(EXPECTED_ENTITY_NAME));
    }

    @Test
    public void searchWithNotContains_pipeSeperatedAttr() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("__classificationNames", SearchParameters.Operator.NOT_CONTAINS, METRIC_CLASSIFICATION);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 7);

        List<String> nameList = new ArrayList<>();
        for (AtlasVertex vertex : vertices) {
            nameList.add((String) entityRetriever.toAtlasEntityHeader(vertex, Collections.singleton("name")).getAttribute("name"));
        }

        assertTrue(nameList.contains(EXPECTED_ENTITY_NAME));
    }

    @AfterClass
    public void teardown() throws Exception {
        AtlasGraphProvider.cleanup();

        super.cleanup();
    }

    @Test
    public void testLast7Days() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret = filtercriteriaDateRange("LAST_7_DAYS", typeRegistry, graph);
        ret.setAttributeName("createTime");
        GregorianCalendar startDate = new GregorianCalendar();
        GregorianCalendar endDate   = new GregorianCalendar();
        startDate.add(Calendar.DATE, -6);

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(startDate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(endDate.getTime()));
    }

    @Test
    public void testLastMonth() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret               = filtercriteriaDateRange("LAST_MONTH", typeRegistry, graph);
        Calendar                        originalstartdate = Calendar.getInstance();
        Calendar                        originalenddate   = Calendar.getInstance();

        originalstartdate.add(Calendar.MONTH, -1);
        originalstartdate.set(Calendar.DAY_OF_MONTH, originalstartdate.getActualMinimum(Calendar.DAY_OF_MONTH));
        originalenddate.add(Calendar.MONTH, -1);
        originalenddate.set(Calendar.DAY_OF_MONTH, originalenddate.getActualMaximum(Calendar.DAY_OF_MONTH));

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(originalstartdate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(originalenddate.getTime()));
    }

    @Test
    public void testLast30Days() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret       = filtercriteriaDateRange("LAST_30_DAYS", typeRegistry, graph);
        GregorianCalendar               startDate = new GregorianCalendar();
        GregorianCalendar               endDate   = new GregorianCalendar();
        startDate.add(Calendar.DATE, -29);

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(startDate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(endDate.getTime()));
    }

    @Test
    public void testYesterday() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret           = filtercriteriaDateRange("YESTERDAY", typeRegistry, graph);
        GregorianCalendar               yesterdayDate = new GregorianCalendar();
        yesterdayDate.add(Calendar.DATE, -1);

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(yesterdayDate.getTime()));
    }

    @Test
    public void testThisMonth() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret               = filtercriteriaDateRange("THIS_MONTH", typeRegistry, graph);
        Calendar                        originalstartdate = Calendar.getInstance();
        Calendar                        originalenddate   = Calendar.getInstance();

        originalstartdate.set(Calendar.DAY_OF_MONTH, originalstartdate.getActualMinimum(Calendar.DAY_OF_MONTH));
        originalenddate.set(Calendar.DAY_OF_MONTH, originalenddate.getActualMaximum(Calendar.DAY_OF_MONTH));

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(originalstartdate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(originalenddate.getTime()));
    }

    @Test
    public void testThisQuarter() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret               = filtercriteriaDateRange("THIS_QUARTER", typeRegistry, graph);
        Calendar                        originalstartdate = Calendar.getInstance();
        Calendar                        originalenddate   = Calendar.getInstance();
        originalstartdate.add(Calendar.MONTH, -1);
        originalstartdate.set(Calendar.DAY_OF_MONTH, originalstartdate.getActualMinimum(Calendar.DAY_OF_MONTH));
        originalenddate.add(Calendar.MONTH, 1);
        originalenddate.set(Calendar.DAY_OF_MONTH, originalenddate.getActualMaximum(Calendar.DAY_OF_MONTH));

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(originalstartdate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(originalenddate.getTime()));
    }

    @Test
    public void testLastQuarter() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret               = filtercriteriaDateRange("LAST_QUARTER", typeRegistry, graph);
        Calendar                        originalstartdate = Calendar.getInstance();
        Calendar                        originalenddate   = Calendar.getInstance();
        originalstartdate.add(Calendar.MONTH, -4);
        originalstartdate.set(Calendar.DAY_OF_MONTH, originalstartdate.getActualMinimum(Calendar.DAY_OF_MONTH));
        originalenddate.add(Calendar.MONTH, -2);
        originalenddate.set(Calendar.DAY_OF_MONTH, originalenddate.getActualMaximum(Calendar.DAY_OF_MONTH));

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(originalstartdate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(originalenddate.getTime()));
    }

    @Test
    public void testLast3Months() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret               = filtercriteriaDateRange("LAST_3_MONTHS", typeRegistry, graph);
        Calendar                        originalstartdate = Calendar.getInstance();
        Calendar                        originalenddate   = Calendar.getInstance();

        originalstartdate.add(Calendar.MONTH, -3);
        originalstartdate.set(Calendar.DAY_OF_MONTH, originalstartdate.getActualMinimum(Calendar.DAY_OF_MONTH));
        originalenddate.add(Calendar.MONTH, -1);
        originalenddate.set(Calendar.DAY_OF_MONTH, originalenddate.getActualMaximum(Calendar.DAY_OF_MONTH));

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(originalstartdate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(originalenddate.getTime()));
    }

    @Test
    public void testThisYear() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret               = filtercriteriaDateRange("THIS_YEAR", typeRegistry, graph);
        Calendar                        originalstartdate = Calendar.getInstance();
        Calendar                        originalenddate   = Calendar.getInstance();

        originalstartdate.set(Calendar.MONTH, 0);
        originalstartdate.set(Calendar.DAY_OF_MONTH, originalstartdate.getActualMinimum(Calendar.DAY_OF_MONTH));

        originalenddate.set(Calendar.MONTH, 11);
        originalenddate.set(Calendar.DAY_OF_MONTH, originalenddate.getActualMaximum(Calendar.DAY_OF_MONTH));

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(originalstartdate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(originalenddate.getTime()));
    }

    @Test
    public void testLastYear() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret               = filtercriteriaDateRange("LAST_YEAR", typeRegistry, graph);
        Calendar                        originalstartdate = Calendar.getInstance();
        Calendar                        originalenddate   = Calendar.getInstance();

        originalstartdate.add(Calendar.YEAR, -1);
        originalstartdate.set(Calendar.MONTH, 0);
        originalstartdate.set(Calendar.DAY_OF_MONTH, originalstartdate.getActualMinimum(Calendar.DAY_OF_MONTH));

        originalenddate.add(Calendar.YEAR, -1);
        originalenddate.set(Calendar.MONTH, 11);
        originalenddate.set(Calendar.DAY_OF_MONTH, originalenddate.getActualMaximum(Calendar.DAY_OF_MONTH));

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(originalstartdate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(originalenddate.getTime()));
    }

    @Test
    public void testLast12Months() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret               = filtercriteriaDateRange("LAST_12_MONTHS", typeRegistry, graph);
        Calendar                        originalstartdate = Calendar.getInstance();
        Calendar                        originalenddate   = Calendar.getInstance();

        originalstartdate.add(Calendar.MONTH, -12);
        originalstartdate.set(Calendar.DAY_OF_MONTH, originalstartdate.getActualMinimum(Calendar.DAY_OF_MONTH));

        originalenddate.add(Calendar.MONTH, -1);
        originalenddate.set(Calendar.DAY_OF_MONTH, originalenddate.getActualMaximum(Calendar.DAY_OF_MONTH));

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(originalstartdate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(originalenddate.getTime()));
    }

    @Test
    public void testLast6Months() throws AtlasBaseException {
        SearchParameters.FilterCriteria ret               = filtercriteriaDateRange("LAST_6_MONTHS", typeRegistry, graph);
        Calendar                        originalstartdate = Calendar.getInstance();
        Calendar                        originalenddate   = Calendar.getInstance();

        originalstartdate.add(Calendar.MONTH, -6);
        originalstartdate.set(Calendar.DAY_OF_MONTH, originalstartdate.getActualMinimum(Calendar.DAY_OF_MONTH));

        originalenddate.add(Calendar.MONTH, -1);
        originalenddate.set(Calendar.DAY_OF_MONTH, originalenddate.getActualMaximum(Calendar.DAY_OF_MONTH));

        String[] dates      = ret.getAttributeValue().split(",");
        String   attrValue1 = dates[0];
        String   attrValue2 = dates[1];

        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue1)))), FORMATTED_DATE.format(originalstartdate.getTime()));
        assertEquals(FORMATTED_DATE.format(new Date((Long.parseLong(attrValue2)))), FORMATTED_DATE.format(originalenddate.getTime()));
    }

    @Test
    public void allEntityTypeForPagination() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();

        params.setTypeName(SearchParameters.ALL_ENTITY_TYPES);
        params.setLimit(10);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, Collections.emptySet());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     v         = processor.execute();
        List                  a         = v.stream().map(v1 -> v1.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).collect(Collectors.toList());

        SearchParameters params2 = new SearchParameters();

        params2.setTypeName(SearchParameters.ALL_ENTITY_TYPES);
        params2.setLimit(10);
        params2.setOffset(10);

        SearchContext         context2   = new SearchContext(params2, typeRegistry, graph, Collections.emptySet());
        EntitySearchProcessor processor2 = new EntitySearchProcessor(context2);

        List<AtlasVertex> v2 = processor2.execute();
        List              b  = v2.stream().map(v3 -> v3.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).collect(Collectors.toList());

        assertFalse(a.stream().anyMatch(b::contains));
    }

    //Description EQUALS chinese multiple char
    @Test
    public void searchChineseDescription() throws AtlasBaseException {
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("path", SearchParameters.Operator.EQ, "我说中文");
        SearchParameters                params         = new SearchParameters();

        filterCriteria.getCriterion();

        params.setTypeName(HDFS_PATH);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 1);

        List<String> guids = vertices.stream().map(g -> {
            try {
                return entityRetriever.toAtlasEntityHeader(g).getGuid();
            } catch (AtlasBaseException e) {
                fail("Failure in mapping vertex to AtlasEntityHeader");
            }
            return "";
        }).collect(Collectors.toList());

        assertTrue(guids.contains(cjkGuid2));
    }

    //Description contains chinese multiple char
    @Test
    public void searchChineseDescriptionCONTAINS() throws AtlasBaseException {
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("path", SearchParameters.Operator.CONTAINS, "我说中文");
        SearchParameters                params         = new SearchParameters();

        filterCriteria.getCriterion();

        params.setTypeName(HDFS_PATH);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 1);

        List<String> guids = vertices.stream().map(g -> {
            try {
                return entityRetriever.toAtlasEntityHeader(g).getGuid();
            } catch (AtlasBaseException e) {
                fail("Failure in mapping vertex to AtlasEntityHeader");
            }
            return "";
        }).collect(Collectors.toList());

        assertTrue(guids.contains(cjkGuid2));
    }

    //Description contains japanese
    @Test
    public void searchJapaneseDescription() throws AtlasBaseException {
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("path", SearchParameters.Operator.EQ, "私は日本語を話します");
        SearchParameters                params         = new SearchParameters();

        filterCriteria.getCriterion();

        params.setTypeName(HDFS_PATH);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 1);

        List<String> guids = vertices.stream().map(g -> {
            try {
                return entityRetriever.toAtlasEntityHeader(g).getGuid();
            } catch (AtlasBaseException e) {
                fail("Failure in mapping vertex to AtlasEntityHeader");
            }
            return "";
        }).collect(Collectors.toList());

        assertTrue(guids.contains(cjkGuid1));
    }

    @Test
    public void searchWithQualifiedNameEQ() throws AtlasBaseException {
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("qualifiedName", SearchParameters.Operator.EQ, "h3qualified");
        SearchParameters                params         = new SearchParameters();

        filterCriteria.getCriterion();

        params.setTypeName(HDFS_PATH);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 1);

        List<String> guids = vertices.stream().map(g -> {
            try {
                return entityRetriever.toAtlasEntityHeader(g).getGuid();
            } catch (AtlasBaseException e) {
                fail("Failure in mapping vertex to AtlasEntityHeader");
            }

            return "";
        }).collect(Collectors.toList());

        assertTrue(guids.contains(cjkGuid1));
    }

    @Test
    public void searchWithNameBeginswith() throws AtlasBaseException {
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("name", SearchParameters.Operator.STARTS_WITH, "hdfs");
        SearchParameters                params         = new SearchParameters();

        filterCriteria.getCriterion();

        params.setTypeName(HDFS_PATH);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex>     vertices  = processor.execute();

        assertEquals(vertices.size(), 1);

        List<String> guids = vertices.stream().map(g -> {
            try {
                return entityRetriever.toAtlasEntityHeader(g).getGuid();
            } catch (AtlasBaseException e) {
                fail("Failure in mapping vertex to AtlasEntityHeader");
            }

            return "";
        }).collect(Collectors.toList());

        assertTrue(guids.contains(cjkGuid2));
    }

    private static SearchParameters.FilterCriteria filtercriteriaDateRange(String attributeValue, AtlasTypeRegistry typeRegistry, AtlasGraph graph) throws AtlasBaseException {
        SearchParameters.FilterCriteria filterCriteria = new SearchParameters.FilterCriteria();
        SearchParameters                params         = new SearchParameters();

        params.setTypeName(HIVE_TABLE_TYPE);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext         context   = new SearchContext(params, typeRegistry, graph, Collections.emptySet());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);

        filterCriteria.setCondition(SearchParameters.FilterCriteria.Condition.AND);
        filterCriteria.setOperator(SearchParameters.Operator.TIME_RANGE);
        filterCriteria.setAttributeValue(attributeValue);

        return processor.processDateRange(filterCriteria);
    }

    private void createJapaneseEntityWithDescription() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity(HDFS_PATH);

        entity.setAttribute("name", "h3");
        entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "h3qualified");
        entity.setAttribute("path", "私は日本語を話します");

        List<AtlasClassification> cls = new ArrayList<>();

        cls.add(new AtlasClassification(JDBC_CLASSIFICATION, Collections.singletonMap("attr1", "attr1")));
        entity.setClassifications(cls);

        //create entity
        final EntityMutationResponse response     = entityStore.createOrUpdate(new AtlasEntityStream(new AtlasEntity.AtlasEntitiesWithExtInfo(entity)), false);
        AtlasEntityHeader            entityHeader = response.getCreatedEntities().get(0);

        cjkGuid1 = entityHeader.getGuid();
    }

    private void createChineseEntityWithDescription() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity(HDFS_PATH);

        entity.setAttribute("name", "hdfs_chinese_test");
        entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "hdfs_chinese_test_qualified");
        entity.setAttribute("path", "我说中文");

        //create entity
        final EntityMutationResponse response     = entityStore.createOrUpdate(new AtlasEntityStream(new AtlasEntity.AtlasEntitiesWithExtInfo(entity)), false);
        AtlasEntityHeader            entityHeader = response.getCreatedEntities().get(0);

        cjkGuid2 = entityHeader.getGuid();
    }
}
