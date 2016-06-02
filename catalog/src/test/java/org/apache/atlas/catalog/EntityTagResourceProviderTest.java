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

package org.apache.atlas.catalog;

import org.apache.atlas.catalog.exception.CatalogException;
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.exception.ResourceAlreadyExistsException;
import org.apache.atlas.catalog.exception.ResourceNotFoundException;
import org.apache.atlas.catalog.query.AtlasQuery;
import org.apache.atlas.catalog.query.QueryFactory;
import org.easymock.Capture;
import org.testng.annotations.Test;

import java.util.*;

import static org.easymock.EasyMock.*;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.replay;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for EntityTagResourceProvider.
 */
public class EntityTagResourceProviderTest {
    @Test
    public void testGetResource() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow = new HashMap<>();
        queryResult.add(queryResultRow);
        queryResultRow.put("name", "taxonomyName.termName");
        queryResultRow.put("description", "test term description");

        // mock expectations
        expect(queryFactory.createEntityTagQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        EntityTagResourceProvider provider = new EntityTagResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "taxonomyName.termName");
        requestProperties.put("id", "1");
        Request userRequest = new InstanceRequest(requestProperties);

        Result result = provider.getResourceById(userRequest);

        assertEquals(1, result.getPropertyMaps().size());
        assertEquals(queryResultRow, result.getPropertyMaps().iterator().next());

        Request request = requestCapture.getValue();
        assertNull(request.getQueryString());
        assertEquals(0, request.getAdditionalSelectProperties().size());
        assertEquals(2, request.getProperties().size());
        assertEquals("taxonomyName.termName", request.getProperties().get("name"));
        assertEquals(Request.Cardinality.INSTANCE, request.getCardinality());

        verify(typeSystem, queryFactory, query);
    }

    @Test(expectedExceptions = ResourceNotFoundException.class)
    public void testGetResource_404() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        // empty response should result in a ResourceNotFoundException
        Collection<Map<String, Object>> emptyResponse = new ArrayList<>();

        // mock expectations
        expect(queryFactory.createEntityTagQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(emptyResponse);
        replay(typeSystem, queryFactory, query);

        EntityTagResourceProvider provider = new EntityTagResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "taxonomyName.termName");
        requestProperties.put("id", "1");
        Request request = new InstanceRequest(requestProperties);

        provider.getResourceById(request);
    }

    @Test
    public void testGetResources() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow1 = new HashMap<>();
        queryResult.add(queryResultRow1);
        queryResultRow1.put("name", "testTaxonomy.termName");
        queryResultRow1.put("description", "test term description");

        Map<String, Object> queryResultRow2 = new HashMap<>();
        queryResult.add(queryResultRow2);
        queryResultRow2.put("name", "testTaxonomy.termName2");
        queryResultRow2.put("description", "test term 2 description");
        // mock expectations
        expect(queryFactory.createEntityTagQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        EntityTagResourceProvider provider = new EntityTagResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("id", "1");
        Request userRequest = new CollectionRequest(requestProperties, "name:testTaxonomy.*");
        // invoke test method
        Result result = provider.getResources(userRequest);

        assertEquals(2, result.getPropertyMaps().size());
        assertTrue(result.getPropertyMaps().contains(queryResultRow1));
        assertTrue(result.getPropertyMaps().contains(queryResultRow2));

        Request request = requestCapture.getValue();
        assertEquals("name:testTaxonomy.*", request.getQueryString());
        assertEquals(0, request.getAdditionalSelectProperties().size());

        verify(typeSystem, queryFactory, query);
    }

    @Test
    public void testGetResources_noResults() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> queryResult = new ArrayList<>();

        // mock expectations
        expect(queryFactory.createEntityTagQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        EntityTagResourceProvider provider = new EntityTagResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("id", "1");
        Request userRequest = new CollectionRequest(requestProperties, "name:testTaxonomy.*");
        // invoke test method
        Result result = provider.getResources(userRequest);

        assertEquals(0, result.getPropertyMaps().size());

        Request request = requestCapture.getValue();
        assertEquals("name:testTaxonomy.*", request.getQueryString());
        assertEquals(0, request.getAdditionalSelectProperties().size());

        verify(typeSystem, queryFactory, query);
    }

    @Test(expectedExceptions = InvalidPayloadException.class)
    public void testCreateResource_invalidRequest__noName() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);

        replay(typeSystem, queryFactory, query);

        Map<String, Object> requestProperties = new HashMap<>();
        // missing name name should result in InvalidPayloadException
        requestProperties.put("description", "description");
        Request userRequest = new InstanceRequest(requestProperties);

        EntityTagResourceProvider provider = new EntityTagResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.createResource(userRequest);
    }

    @Test
    public void testCreateResource() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        ResourceProvider termResourceProvider = createStrictMock(TermResourceProvider.class);
        Capture<Request> termRequestCapture = newCapture();

        Collection<Map<String, Object>> termQueryResult = new ArrayList<>();
        Map<String, Object> termQueryResultRow = new HashMap<>();
        termQueryResult.add(termQueryResultRow);
        termQueryResultRow.put("name", "testTaxonomy.termName");
        termQueryResultRow.put("type", "testTaxonomy.termName");
        termQueryResultRow.put("available_as_tag", true);
        termQueryResultRow.put("description", "term description");
        Result termResult = new Result(termQueryResult);

        // mock expectations
        expect(termResourceProvider.getResourceById(capture(termRequestCapture))).andReturn(termResult);
        Map<String, Object> tagProperties = new HashMap<>();
        tagProperties.put("name", "testTaxonomy.termName");
        tagProperties.put("description", "term description");
        typeSystem.createTraitInstance("11-22-33", "testTaxonomy.termName", tagProperties);
        replay(typeSystem, queryFactory, query, termResourceProvider);

        EntityTagResourceProvider provider = new TestEntityTagResourceProvider(typeSystem, termResourceProvider);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "testTaxonomy.termName");
        requestProperties.put("id", "11-22-33");
        Request userRequest = new InstanceRequest(requestProperties);

        provider.createResource(userRequest);

        Request termRequest = termRequestCapture.getValue();
        Map<String, Object> termRequestProps = termRequest.getProperties();
        assertEquals(1, termRequestProps.size());
        TermPath termPath = (TermPath) termRequestProps.get("termPath");
        assertEquals("testTaxonomy.termName", termPath.getFullyQualifiedName());
        assertEquals(1, termRequest.getAdditionalSelectProperties().size());
        assertEquals("type", termRequest.getAdditionalSelectProperties().iterator().next());
        assertNull(termRequest.getQueryString());

        verify(typeSystem, queryFactory, query, termResourceProvider);
    }

    @Test(expectedExceptions = CatalogException.class)
    public void testCreateResource_invalidRequest__termNotAvailableForTagging() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        ResourceProvider termResourceProvider = createStrictMock(TermResourceProvider.class);
        Capture<Request> termRequestCapture = newCapture();

        Collection<Map<String, Object>> termQueryResult = new ArrayList<>();
        Map<String, Object> termQueryResultRow = new HashMap<>();
        termQueryResult.add(termQueryResultRow);
        termQueryResultRow.put("name", "testTaxonomy.termName");
        termQueryResultRow.put("type", "testTaxonomy.termName");
        // false value for 'available_as_tag' should result in an exception
        termQueryResultRow.put("available_as_tag", false);
        termQueryResultRow.put("description", "term description");
        Result termResult = new Result(termQueryResult);

        // mock expectations
        expect(termResourceProvider.getResourceById(capture(termRequestCapture))).andReturn(termResult);
        replay(typeSystem, queryFactory, query, termResourceProvider);

        EntityTagResourceProvider provider = new TestEntityTagResourceProvider(typeSystem, termResourceProvider);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "testTaxonomy.termName");
        requestProperties.put("id", "11-22-33");
        Request userRequest = new InstanceRequest(requestProperties);

        provider.createResource(userRequest);
    }

    @Test(expectedExceptions = ResourceAlreadyExistsException.class)
    public void testCreateResource_invalidRequest__alreadyExists() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        ResourceProvider termResourceProvider = createStrictMock(TermResourceProvider.class);
        Capture<Request> termRequestCapture = newCapture();

        Collection<Map<String, Object>> termQueryResult = new ArrayList<>();
        Map<String, Object> termQueryResultRow = new HashMap<>();
        termQueryResult.add(termQueryResultRow);
        termQueryResultRow.put("name", "testTaxonomy.termName");
        termQueryResultRow.put("type", "testTaxonomy.termName");
        termQueryResultRow.put("available_as_tag", true);
        termQueryResultRow.put("description", "term description");
        Result termResult = new Result(termQueryResult);

        // mock expectations
        expect(termResourceProvider.getResourceById(capture(termRequestCapture))).andReturn(termResult);
        Map<String, Object> tagProperties = new HashMap<>();
        tagProperties.put("name", "testTaxonomy.termName");
        tagProperties.put("description", "term description");
        typeSystem.createTraitInstance("11-22-33", "testTaxonomy.termName", tagProperties);
        expectLastCall().andThrow(new ResourceAlreadyExistsException(""));
        replay(typeSystem, queryFactory, query, termResourceProvider);

        EntityTagResourceProvider provider = new TestEntityTagResourceProvider(typeSystem, termResourceProvider);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "testTaxonomy.termName");
        requestProperties.put("id", "11-22-33");
        Request userRequest = new InstanceRequest(requestProperties);

        provider.createResource(userRequest);
    }

    @Test
    public void testCreateResources() throws Exception {
        AtlasTypeSystem typeSystem = createMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery entityQuery = createMock(AtlasQuery.class);
        ResourceProvider termResourceProvider = createMock(TermResourceProvider.class);
        Capture<Request> entityRequestCapture = newCapture();
        Capture<Request> termRequestCapture1 = newCapture();
        Capture<Request> termRequestCapture2 = newCapture();

        Collection<Map<String, Object>> entityQueryResult = new ArrayList<>();
        Map<String, Object> entityQueryResultRow = new HashMap<>();
        entityQueryResultRow.put("id", "1");
        entityQueryResult.add(entityQueryResultRow);

        Map<String, Object> entityQueryResultRow2 = new HashMap<>();
        entityQueryResultRow2.put("id", "2");
        entityQueryResult.add(entityQueryResultRow2);

        Collection<Map<String, Object>> termQueryResult1 = new ArrayList<>();
        Map<String, Object> termQueryResultRow1 = new HashMap<>();
        termQueryResult1.add(termQueryResultRow1);
        termQueryResultRow1.put("name", "testTaxonomy.termName1");
        termQueryResultRow1.put("type", "testTaxonomy.termName1");
        termQueryResultRow1.put("available_as_tag", true);
        termQueryResultRow1.put("description", "term description");
        Result termResult1 = new Result(termQueryResult1);

        Collection<Map<String, Object>> termQueryResult2 = new ArrayList<>();
        Map<String, Object> termQueryResultRow2 = new HashMap<>();
        termQueryResult2.add(termQueryResultRow2);
        termQueryResultRow2.put("name", "testTaxonomy.termName2");
        termQueryResultRow2.put("type", "testTaxonomy.termName2");
        termQueryResultRow2.put("available_as_tag", true);
        termQueryResultRow2.put("description", "term 2 description");
        Result termResult2 = new Result(termQueryResult2);

        // mock expectations
        expect(queryFactory.createEntityQuery(capture(entityRequestCapture))).andReturn(entityQuery);
        expect(entityQuery.execute()).andReturn(entityQueryResult);

        expect(termResourceProvider.getResourceById(capture(termRequestCapture1))).andReturn(termResult1);
        expect(termResourceProvider.getResourceById(capture(termRequestCapture2))).andReturn(termResult2);

        Map<String, Object> tagProperties1 = new HashMap<>();
        tagProperties1.put("name", "testTaxonomy.termName1");
        tagProperties1.put("description", "term description");
        // each tag is associated with each entity
        typeSystem.createTraitInstance("1", "testTaxonomy.termName1", tagProperties1);
        typeSystem.createTraitInstance("2", "testTaxonomy.termName1", tagProperties1);

        Map<String, Object> tagProperties2 = new HashMap<>();
        tagProperties2.put("name", "testTaxonomy.termName2");
        tagProperties2.put("description", "term 2 description");
        // each tag is associated with each entity
        typeSystem.createTraitInstance("1", "testTaxonomy.termName2", tagProperties2);
        typeSystem.createTraitInstance("2", "testTaxonomy.termName2", tagProperties2);

        replay(typeSystem, queryFactory, entityQuery, termResourceProvider);
        // end mock expectations

        EntityTagResourceProvider provider = new TestEntityTagResourceProvider(typeSystem, termResourceProvider);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProps = new HashMap<>();
        Collection<Map<String, String>> tagMaps = new ArrayList<>();
        requestProps.put("tags", tagMaps);
        Map<String, String> tagMap1 = new HashMap<>();
        tagMap1.put("name", "testTaxonomy.termName1");
        tagMaps.add(tagMap1);
        Map<String, String> tagMap2 = new HashMap<>();
        tagMap2.put("name", "testTaxonomy.termName2");
        tagMaps.add(tagMap2);

        Request userRequest = new CollectionRequest(requestProps, "name:foo*");
        // invoke method being tested
        Collection<String> createResult = provider.createResources(userRequest);

        assertEquals(4, createResult.size());
        assertTrue(createResult.contains("v1/entities/1/tags/testTaxonomy.termName1"));
        assertTrue(createResult.contains("v1/entities/1/tags/testTaxonomy.termName2"));
        assertTrue(createResult.contains("v1/entities/2/tags/testTaxonomy.termName1"));
        assertTrue(createResult.contains("v1/entities/2/tags/testTaxonomy.termName2"));

        Request entityRequest = entityRequestCapture.getValue();
        assertEquals("name:foo*", entityRequest.getQueryString());
        assertEquals(Request.Cardinality.COLLECTION, entityRequest.getCardinality());

        Request termRequest1 = termRequestCapture1.getValue();
        assertNull(termRequest1.getQueryString());
        assertEquals(Request.Cardinality.INSTANCE, termRequest1.getCardinality());
        Map<String, Object> termRequestProps = termRequest1.getProperties();
        assertEquals(1, termRequestProps.size());
        TermPath termPath = (TermPath) termRequestProps.get("termPath");
        assertEquals("testTaxonomy.termName1", termPath.getFullyQualifiedName());

        Request termRequest2 = termRequestCapture2.getValue();
        assertNull(termRequest2.getQueryString());
        assertEquals(Request.Cardinality.INSTANCE, termRequest2.getCardinality());
        Map<String, Object> termRequestProps2 = termRequest2.getProperties();
        assertEquals(1, termRequestProps2.size());
        TermPath termPath2 = (TermPath) termRequestProps2.get("termPath");
        assertEquals("testTaxonomy.termName2", termPath2.getFullyQualifiedName());

        verify(typeSystem, queryFactory, entityQuery, termResourceProvider);
    }

    @Test
    public void testDeleteResourceById() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);

        // mock expectations
        typeSystem.deleteTag("1", "taxonomyName.termName");
        replay(typeSystem);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "taxonomyName.termName");
        requestProperties.put("id", "1");
        Request userRequest = new InstanceRequest(requestProperties);

        // instantiate EntityTagResourceProvider and invoke method being tested
        EntityTagResourceProvider provider = new EntityTagResourceProvider(typeSystem);
        provider.setQueryFactory(null);
        provider.deleteResourceById(userRequest);

        verify(typeSystem);
    }

    //todo: test behavior of createResources in case of partial success after behavior is defined


    private static class TestEntityTagResourceProvider extends EntityTagResourceProvider {

        private ResourceProvider testTermResourceProvider;

        public TestEntityTagResourceProvider(AtlasTypeSystem typeSystem, ResourceProvider termResourceProvider) {
            super(typeSystem);
            testTermResourceProvider = termResourceProvider;
        }

        @Override
        protected synchronized ResourceProvider getTermResourceProvider() {
            return testTermResourceProvider;
        }
    }
}
