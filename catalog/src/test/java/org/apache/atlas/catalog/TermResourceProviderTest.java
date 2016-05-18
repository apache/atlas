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
    public void testGetResource() throws Exception {
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
        assertEquals(request.getProperties().size(), 2);
        assertEquals(request.getProperties().get("termPath"), termPath);
        assertEquals(request.getProperties().get("name"), termPath.getFullyQualifiedName());

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
        assertEquals(request.getProperties().size(), 1);

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
        assertEquals(request.getProperties().size(), 1);

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
        Map<String, Object> taxonomyRequestProps = taxonomyRequest.getProperties();
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

    private static class TestTermResourceProvider extends TermResourceProvider {

        private ResourceProvider testTaxonomyResourceProvider;

        public TestTermResourceProvider(AtlasTypeSystem typeSystem, ResourceProvider taxonomyResourceProvider) {
            super(typeSystem);
            testTaxonomyResourceProvider = taxonomyResourceProvider;
        }

        @Override
        protected synchronized ResourceProvider getTaxonomyResourceProvider() {
            return testTaxonomyResourceProvider;
        }
    }
}
