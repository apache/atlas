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

import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.exception.ResourceAlreadyExistsException;
import org.apache.atlas.catalog.exception.ResourceNotFoundException;
import org.apache.atlas.catalog.query.AtlasQuery;
import org.apache.atlas.catalog.query.QueryFactory;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.testng.annotations.Test;

import java.util.*;

import static org.easymock.EasyMock.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for TermResourceProvider.
 */
public class TermResourceProviderTest {
    @Test
    public void testGetResourceById() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        TermPath termPath = new TermPath("testTaxonomy", "termName");

        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow = new HashMap<>();
        queryResult.add(queryResultRow);
        queryResultRow.put("name", "testTaxonomy.termName");
        queryResultRow.put("description", "test term description");
        queryResultRow.put("creation_time", "04/20/2016");
        queryResultRow.put("acceptable_use", "anything");
        queryResultRow.put("available_as_tag", true);
        Map<String, Object> hierarchyMap = new HashMap<>();
        queryResultRow.put("hierarchy", hierarchyMap);
        hierarchyMap.put("path", "/");
        hierarchyMap.put("short_name", "termName");
        hierarchyMap.put("taxonomy", "testTaxonomy");

        // mock expectations
        expect(queryFactory.createTermQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        TermResourceProvider provider = new TermResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", termPath);
        Request userRequest = new InstanceRequest(requestProperties);

        Result result = provider.getResourceById(userRequest);

        assertEquals(result.getPropertyMaps().size(), 1);
        assertEquals(result.getPropertyMaps().iterator().next(), queryResultRow);

        Request request = requestCapture.getValue();
        assertNull(request.getQueryString());
        assertEquals(request.getAdditionalSelectProperties().size(), 0);
        assertEquals(request.getQueryProperties().size(), 2);
        assertEquals(request.getQueryProperties().get("termPath"), termPath);
        assertEquals(request.getQueryProperties().get("name"), termPath.getFullyQualifiedName());

        verify(typeSystem, queryFactory, query);
    }

    @Test(expectedExceptions = ResourceNotFoundException.class)
    public void testGetResourceById_404() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        // empty response should result in a ResourceNotFoundException
        Collection<Map<String, Object>> emptyResponse = new ArrayList<>();

        // mock expectations
        expect(queryFactory.createTermQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(emptyResponse);
        replay(typeSystem, queryFactory, query);

        TermResourceProvider provider = new TermResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", new TermPath("taxonomyName.badTermName"));
        Request request = new InstanceRequest(requestProperties);

        provider.getResourceById(request);
    }

    @Test
    public void testGetResources() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        TermPath termPath = new TermPath("testTaxonomy", null);

        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow1 = new HashMap<>();
        queryResult.add(queryResultRow1);
        queryResultRow1.put("name", "testTaxonomy.termName");
        queryResultRow1.put("description", "test term description");
        queryResultRow1.put("creation_time", "04/20/2016");
        queryResultRow1.put("acceptable_use", "anything");
        queryResultRow1.put("available_as_tag", true);
        Map<String, Object> hierarchyMap = new HashMap<>();
        queryResultRow1.put("hierarchy", hierarchyMap);
        hierarchyMap.put("path", "/");
        hierarchyMap.put("short_name", "termName");
        hierarchyMap.put("taxonomy", "testTaxonomy");

        Map<String, Object> queryResultRow2 = new HashMap<>();
        queryResult.add(queryResultRow2);
        queryResultRow2.put("name", "testTaxonomy.termName2");
        queryResultRow2.put("description", "test term 2 description");
        queryResultRow2.put("creation_time", "04/21/2016");
        queryResultRow2.put("acceptable_use", "anything");
        queryResultRow2.put("available_as_tag", true);
        Map<String, Object> hierarchyMap2 = new HashMap<>();
        queryResultRow2.put("hierarchy", hierarchyMap2);
        hierarchyMap2.put("path", "/");
        hierarchyMap2.put("short_name", "termName2");
        hierarchyMap2.put("taxonomy", "testTaxonomy");

        // mock expectations
        expect(queryFactory.createTermQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        TermResourceProvider provider = new TermResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", termPath);
        Request userRequest = new CollectionRequest(requestProperties, "name:taxonomy*");
        // invoke test method
        Result result = provider.getResources(userRequest);

        assertEquals(result.getPropertyMaps().size(), 2);
        assertTrue(result.getPropertyMaps().contains(queryResultRow1));
        assertTrue(result.getPropertyMaps().contains(queryResultRow2));

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryString(), "name:taxonomy*");
        assertEquals(request.getAdditionalSelectProperties().size(), 0);
        assertEquals(request.getQueryProperties().size(), 1);

        verify(typeSystem, queryFactory, query);
    }

    @Test
    public void testGetResources_noResults() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        TermPath termPath = new TermPath("testTaxonomy", "termName");

        // empty result shouldn't result in exception for collection query
        Collection<Map<String, Object>> queryResult = new ArrayList<>();

        // mock expectations
        expect(queryFactory.createTermQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        TermResourceProvider provider = new TermResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", termPath);
        Request userRequest = new CollectionRequest(requestProperties, "name:taxonomy*");
        // invoke test method
        Result result = provider.getResources(userRequest);

        assertEquals(0, result.getPropertyMaps().size());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryString(), "name:taxonomy*");
        assertEquals(request.getAdditionalSelectProperties().size(), 0);
        assertEquals(request.getQueryProperties().size(), 1);

        verify(typeSystem, queryFactory, query);
    }

    @Test(expectedExceptions = InvalidPayloadException.class)
    public void testCreateResource_invalidRequest__noName() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);

        // null term name should result in InvalidPayloadException
        TermPath termPath = new TermPath("testTaxonomy", null);

        // mock expectations
        replay(typeSystem, queryFactory, query);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", termPath);
        Request userRequest = new InstanceRequest(requestProperties);

        TermResourceProvider provider = new TermResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.createResource(userRequest);
    }

    @Test
    public void testCreateResource() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<ResourceDefinition> resourceDefinitionCapture = newCapture();
        ResourceProvider taxonomyResourceProvider = createStrictMock(TaxonomyResourceProvider.class);
        Capture<Request> taxonomyRequestCapture = newCapture();

        Collection<Map<String, Object>> taxonomyQueryResult = new ArrayList<>();
        Map<String, Object> taxonomyQueryResultRow = new HashMap<>();
        taxonomyQueryResult.add(taxonomyQueryResultRow);
        taxonomyQueryResultRow.put("name", "testTaxonomy");
        taxonomyQueryResultRow.put("id", "11-22-33");
        Result taxonomyResult = new Result(taxonomyQueryResult);

        Map<String, Object> expectedRequestProps = new HashMap<>();
        expectedRequestProps.put("name", "testTaxonomy.termName");
        // when not specified, the default value of 'true' should be set
        expectedRequestProps.put("available_as_tag", true);

        // mock expectations
        expect(taxonomyResourceProvider.getResourceById(capture(taxonomyRequestCapture))).andReturn(taxonomyResult);
        typeSystem.createTraitType(capture(resourceDefinitionCapture), eq("testTaxonomy.termName"), EasyMock.<String>isNull());
        typeSystem.createTraitInstance("11-22-33", "testTaxonomy.termName", expectedRequestProps);
        replay(typeSystem, queryFactory, query, taxonomyResourceProvider);

        TermResourceProvider provider = new TestTermResourceProvider(typeSystem, taxonomyResourceProvider);
        provider.setQueryFactory(queryFactory);

        TermPath termPath = new TermPath("testTaxonomy", "termName");
        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", termPath);
        Request userRequest = new InstanceRequest(requestProperties);

        provider.createResource(userRequest);

        Request taxonomyRequest = taxonomyRequestCapture.getValue();
        Map<String, Object> taxonomyRequestProps = taxonomyRequest.getQueryProperties();
        assertEquals(taxonomyRequestProps.size(), 1);
        assertEquals(taxonomyRequestProps.get("name"), "testTaxonomy");
        assertEquals(taxonomyRequest.getAdditionalSelectProperties().size(), 1);
        assertEquals(taxonomyRequest.getAdditionalSelectProperties().iterator().next(), "id");
        assertNull(taxonomyRequest.getQueryString());

        ResourceDefinition resourceDefinition = resourceDefinitionCapture.getValue();
        assertEquals(resourceDefinition.getTypeName(), "Term");

        verify(typeSystem, queryFactory, query, taxonomyResourceProvider);
    }

    @Test(expectedExceptions = ResourceAlreadyExistsException.class)
    public void testCreateResource_invalidRequest__alreadyExists() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<ResourceDefinition> resourceDefinitionCapture = newCapture();
        ResourceProvider taxonomyResourceProvider = createStrictMock(TaxonomyResourceProvider.class);
        Capture<Request> taxonomyRequestCapture = newCapture();

        Collection<Map<String, Object>> taxonomyQueryResult = new ArrayList<>();
        Map<String, Object> taxonomyQueryResultRow = new HashMap<>();
        taxonomyQueryResult.add(taxonomyQueryResultRow);
        taxonomyQueryResultRow.put("name", "testTaxonomy");
        taxonomyQueryResultRow.put("id", "11-22-33");
        Result taxonomyResult = new Result(taxonomyQueryResult);

        // mock expectations
        expect(taxonomyResourceProvider.getResourceById(capture(taxonomyRequestCapture))).andReturn(taxonomyResult);
        typeSystem.createTraitType(capture(resourceDefinitionCapture), eq("testTaxonomy.termName"), EasyMock.<String>isNull());
        expectLastCall().andThrow(new ResourceAlreadyExistsException(""));

        replay(typeSystem, queryFactory, query, taxonomyResourceProvider);

        TermResourceProvider provider = new TestTermResourceProvider(typeSystem, taxonomyResourceProvider);
        provider.setQueryFactory(queryFactory);

        TermPath termPath = new TermPath("testTaxonomy", "termName");
        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", termPath);
        Request userRequest = new InstanceRequest(requestProperties);

        provider.createResource(userRequest);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCreateResources() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);

        // mock expectations
        replay(typeSystem, queryFactory);

        TermPath termPath = new TermPath("testTaxonomy", "termName");
        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", termPath);
        Request userRequest = new InstanceRequest(requestProperties);

        TermResourceProvider provider = new TermResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.createResources(userRequest);
    }

    @Test
    public void testDeleteResourceById() throws Exception {
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider entityResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider entityTagResourceProvider = createStrictMock(ResourceProvider.class);
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> taxonomyRequestCapture = newCapture();
        Capture<Request> termRequestCapture = newCapture();

        // root term being deleted
        TermPath termPath = new TermPath("testTaxonomy.termName");

        // entity requests to get id's of entities tagged with terms
        Request entityRequest1 = new CollectionRequest(Collections.<String, Object>emptyMap(),
                "tags/name:testTaxonomy.termName.child1");
        Request entityRequest2 = new CollectionRequest(Collections.<String, Object>emptyMap(),
                "tags/name:testTaxonomy.termName.child2");
        Request entityRequest3 = new CollectionRequest(Collections.<String, Object>emptyMap(),
                "tags/name:testTaxonomy.termName");

        // entity tag requests to delete entity tags
        Map<String, Object> entityTagRequestMap1 = new HashMap<>();
        entityTagRequestMap1.put("id", "111");
        entityTagRequestMap1.put("name", "testTaxonomy.termName.child1");
        Request entityTagRequest1 = new InstanceRequest(entityTagRequestMap1);
        Map<String, Object> entityTagRequestMap2 = new HashMap<>();
        entityTagRequestMap2.put("id", "222");
        entityTagRequestMap2.put("name", "testTaxonomy.termName.child1");
        Request entityTagRequest2 = new InstanceRequest(entityTagRequestMap2);
        Map<String, Object> entityTagRequestMap3 = new HashMap<>();
        entityTagRequestMap3.put("id", "333");
        entityTagRequestMap3.put("name", "testTaxonomy.termName.child2");
        Request entityTagRequest3 = new InstanceRequest(entityTagRequestMap3);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", termPath);
        Request userRequest = new InstanceRequest(requestProperties);

        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow = new HashMap<>();
        queryResult.add(queryResultRow);
        queryResultRow.put("name", "testTaxonomy.termName");
        queryResultRow.put("id", "111-222-333");

        Collection<Map<String, Object>> taxonomyResultMaps = new ArrayList<>();
        Map<String, Object> taxonomyResultMap = new HashMap<>();
        taxonomyResultMap.put("name", "testTaxonomy");
        taxonomyResultMap.put("id", "12345");
        taxonomyResultMaps.add(taxonomyResultMap);
        Result taxonomyResult = new Result(taxonomyResultMaps);

        Collection<Map<String, Object>> childResult = new ArrayList<>();
        Map<String, Object> childResultRow = new HashMap<>();
        childResult.add(childResultRow);
        childResultRow.put("name", "testTaxonomy.termName.child1");
        childResultRow.put("id", "1-1-1");
        Map<String, Object> childResultRow2 = new HashMap<>();
        childResult.add(childResultRow2);
        childResultRow2.put("name", "testTaxonomy.termName.child2");
        childResultRow2.put("id", "2-2-2");

        Collection<Map<String, Object>> entityResults1 = new ArrayList<>();
        Map<String, Object> entityResult1Map1 = new HashMap<>();
        entityResult1Map1.put("name", "entity1");
        entityResult1Map1.put("id", "111");
        entityResults1.add(entityResult1Map1);
        Map<String, Object> entityResult1Map2 = new HashMap<>();
        entityResult1Map2.put("name", "entity2");
        entityResult1Map2.put("id", "222");
        entityResults1.add(entityResult1Map2);
        Result entityResult1 = new Result(entityResults1);

        Collection<Map<String, Object>> entityResults2 = new ArrayList<>();
        Map<String, Object> entityResult2Map = new HashMap<>();
        entityResult2Map.put("name", "entity3");
        entityResult2Map.put("id", "333");
        entityResults2.add(entityResult2Map);
        Result entityResult2 = new Result(entityResults2);

        // mock expectations
        // ensure term exists
        expect(queryFactory.createTermQuery(userRequest)).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        // taxonomy query
        expect(taxonomyResourceProvider.getResourceById(capture(taxonomyRequestCapture))).andReturn(taxonomyResult);
        // get term children
        expect(queryFactory.createTermQuery(capture(termRequestCapture))).andReturn(query);
        expect(query.execute()).andReturn(childResult);
        // entities with child1 tag
        expect(entityResourceProvider.getResources(eq(entityRequest1))).andReturn(entityResult1);
//        typeSystem.deleteTag("111", "testTaxonomy.termName.child1");
//        typeSystem.deleteTag("222", "testTaxonomy.termName.child1");
        entityTagResourceProvider.deleteResourceById(entityTagRequest1);
        entityTagResourceProvider.deleteResourceById(entityTagRequest2);
        // delete child1 from taxonomy
        typeSystem.deleteTag("12345", "testTaxonomy.termName.child1");
        // entities with child2 tag
        expect(entityResourceProvider.getResources(eq(entityRequest2))).andReturn(entityResult2);
        //typeSystem.deleteTag("333", "testTaxonomy.termName.child2");
        entityTagResourceProvider.deleteResourceById(entityTagRequest3);
        // delete child2 from taxonomy
        typeSystem.deleteTag("12345", "testTaxonomy.termName.child2");
        // root term being deleted which has no associated tags
        expect(entityResourceProvider.getResources(eq(entityRequest3))).andReturn(
                new Result(Collections.<Map<String, Object>>emptyList()));
        // delete root term from taxonomy
        typeSystem.deleteTag("12345", "testTaxonomy.termName");

        replay(taxonomyResourceProvider, entityResourceProvider, entityTagResourceProvider, typeSystem, queryFactory, query);

        TermResourceProvider provider = new TestTermResourceProvider(
                typeSystem, taxonomyResourceProvider, entityResourceProvider, entityTagResourceProvider);
        provider.setQueryFactory(queryFactory);

        // invoke method being tested
        provider.deleteResourceById(userRequest);

        Request taxonomyRequest = taxonomyRequestCapture.getValue();
        assertEquals(taxonomyRequest.getQueryProperties().get("name"), "testTaxonomy");
        assertEquals(taxonomyRequest.getAdditionalSelectProperties().size(), 1);
        assertTrue(taxonomyRequest.getAdditionalSelectProperties().contains("id"));

        Request childTermRequest = termRequestCapture.getValue();
        assertEquals(childTermRequest.<TermPath>getProperty("termPath").getFullyQualifiedName(), "testTaxonomy.termName.");
        verify(taxonomyResourceProvider, entityResourceProvider, entityTagResourceProvider, typeSystem, queryFactory, query);
    }

    @Test
    public void testUpdateResourceById() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> termRequestCapture = newCapture();
        Capture<Request> tagRequestCapture = newCapture();

        TermPath termPath = new TermPath("testTaxonomy", "termName");

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("termPath", termPath);
        Map<String, Object> requestUpdateProperties = new HashMap<>();
        requestUpdateProperties.put("description", "updatedValue");
        Request userRequest = new InstanceRequest(requestProperties, requestUpdateProperties);

        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow = new HashMap<>();
        queryResult.add(queryResultRow);
        queryResultRow.put("name", "testTaxonomy.termName");

        // mock expectations
        // term update
        expect(queryFactory.createTermQuery(capture(termRequestCapture))).andReturn(query);
        expect(query.execute(requestUpdateProperties)).andReturn(queryResult);
        // tag updates
        expect(queryFactory.createEntityTagQuery(capture(tagRequestCapture))).andReturn(query);
        // query response isn't used so just returning null
        expect(query.execute(requestUpdateProperties)).andReturn(null);
        replay(typeSystem, queryFactory, query);

        TermResourceProvider provider = new TermResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.updateResourceById(userRequest);

        Request request = termRequestCapture.getValue();
        assertNull(request.getQueryString());
        assertTrue(request.getAdditionalSelectProperties().isEmpty());
        assertEquals(request.getQueryProperties().size(), 2);
        assertEquals(request.getQueryProperties().get("termPath"), termPath);
        assertEquals(request.getQueryProperties().get("name"), termPath.getFullyQualifiedName());

        Request tagRequest = tagRequestCapture.getValue();
        assertEquals(tagRequest.getQueryString(), "name:testTaxonomy.termName");
        assertEquals(tagRequest.getQueryProperties().size(), 1);
        assertEquals(tagRequest.getQueryProperties().get("id"), "*");

        verify(typeSystem, queryFactory, query);
    }

    private static class TestTermResourceProvider extends TermResourceProvider {

        private ResourceProvider testTaxonomyResourceProvider;
        private ResourceProvider testEntityResourceProvider;
        private ResourceProvider testEntityTagResourceProvider;

        public TestTermResourceProvider(AtlasTypeSystem typeSystem,
                                        ResourceProvider taxonomyResourceProvider) {
            super(typeSystem);
            testTaxonomyResourceProvider = taxonomyResourceProvider;
        }

        public TestTermResourceProvider(AtlasTypeSystem typeSystem,
                                        ResourceProvider taxonomyResourceProvider,
                                        ResourceProvider entityResourceProvider,
                                        ResourceProvider entityTagResourceProvider) {
            super(typeSystem);
            testTaxonomyResourceProvider = taxonomyResourceProvider;
            testEntityResourceProvider = entityResourceProvider;
            testEntityTagResourceProvider = entityTagResourceProvider;
        }

        @Override
        protected synchronized ResourceProvider getTaxonomyResourceProvider() {
            return testTaxonomyResourceProvider;
        }

        @Override
        protected synchronized ResourceProvider getEntityResourceProvider() {
            return testEntityResourceProvider;
        }

        @Override
        protected synchronized ResourceProvider getEntityTagResourceProvider() {
            return testEntityTagResourceProvider;
        }
    }
}
