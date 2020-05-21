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
import org.apache.atlas.BasicTestSetup;
import org.apache.atlas.SortOrder;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class EntitySearchProcessorTest extends BasicTestSetup {

    @Inject
    private AtlasGraph graph;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private EntityGraphRetriever entityRetriever;

    @BeforeClass
    public void setup() {
        setupTestData();
    }

    @Inject
    public GraphBackedSearchIndexer indexer;

    @Test
    public void searchTablesByClassification() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName("hive_column");
        params.setClassification("PII");
        params.setLimit(10);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.<String>emptySet());

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

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.<String>emptySet());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);

        List<AtlasVertex> vertices = processor.execute();

        assertEquals(processor.getResultCount(), 4);
        assertEquals(vertices.size(), 4);


        AtlasVertex firstVertex = vertices.get(0);

        Date firstDate = (Date) entityRetriever.toAtlasEntityHeader(firstVertex, Sets.newHashSet(params.getSortBy())).getAttribute(params.getSortBy());

        AtlasVertex secondVertex = vertices.get(1);
        Date secondDate = (Date) entityRetriever.toAtlasEntityHeader(secondVertex, Sets.newHashSet(params.getSortBy())).getAttribute(params.getSortBy());

        assertTrue(firstDate.before(secondDate));
    }

    @Test
    public void emptySearchByClassification() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName("hive_table");
        params.setClassification("PII");
        params.setLimit(10);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.<String>emptySet());

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

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.<String>emptySet());
        new EntitySearchProcessor(context);
    }

    @Test
    public void searchWithNEQ_stringAttr() throws AtlasBaseException {
        String expectedEntityName = "hive_Table_Null_tableType";
        createDummyEntity(expectedEntityName,HIVE_TABLE_TYPE);
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("tableType", SearchParameters.Operator.NEQ, "Managed");
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex> vertices = processor.execute();

        assertEquals(vertices.size(), 3);

        List<String> nameList = new ArrayList<>();
        for (AtlasVertex vertex : vertices) {
            nameList.add((String) entityRetriever.toAtlasEntityHeader(vertex, Collections.singleton("name")).getAttribute("name"));
        }

        assertTrue(nameList.contains(expectedEntityName));
    }

    @Test(dependsOnMethods = "searchWithNEQ_stringAttr")
    public void searchWithNEQ_pipeSeperatedAttr() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("__classificationNames", SearchParameters.Operator.NEQ, METRIC_CLASSIFICATION);
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex> vertices = processor.execute();

        assertEquals(vertices.size(), 7);

        List<String> nameList = new ArrayList<>();
        for (AtlasVertex vertex : vertices) {
            nameList.add((String) entityRetriever.toAtlasEntityHeader(vertex, Collections.singleton("name")).getAttribute("name"));
        }

        assertTrue(nameList.contains("hive_Table_Null_tableType"));
    }

    @Test(dependsOnMethods = "searchWithNEQ_stringAttr")
    public void searchWithNEQ_doubleAttr() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName(HIVE_TABLE_TYPE);
        SearchParameters.FilterCriteria filterCriteria = getSingleFilterCondition("retention", SearchParameters.Operator.NEQ, "5");
        params.setEntityFilters(filterCriteria);
        params.setLimit(20);

        SearchContext context = new SearchContext(params, typeRegistry, graph, indexer.getVertexIndexKeys());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);
        List<AtlasVertex> vertices = processor.execute();

        assertEquals(vertices.size(), 1);

        List<String> nameList = new ArrayList<>();
        for (AtlasVertex vertex : vertices) {
            nameList.add((String) entityRetriever.toAtlasEntityHeader(vertex, Collections.singleton("name")).getAttribute("name"));
        }

        assertTrue(nameList.contains("hive_Table_Null_tableType"));
    }
}
