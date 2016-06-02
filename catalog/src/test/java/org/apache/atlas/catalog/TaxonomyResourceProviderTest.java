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
import org.apache.atlas.catalog.definition.TaxonomyResourceDefinition;
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.exception.ResourceAlreadyExistsException;
import org.apache.atlas.catalog.exception.ResourceNotFoundException;
import org.apache.atlas.catalog.query.AtlasQuery;
import org.apache.atlas.catalog.query.QueryFactory;
import org.easymock.Capture;
import org.testng.annotations.Test;

import java.util.*;

import static org.easymock.EasyMock.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for TaxonomyResourceProvider.
 */
public class TaxonomyResourceProviderTest {
    @Test
    public void testGetResource() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow = new HashMap<>();
        queryResult.add(queryResultRow);
        queryResultRow.put("name", "taxonomyName");
        queryResultRow.put("description", "test taxonomy description");
        queryResultRow.put("creation_time", "04/20/2016");

        // mock expectations
        expect(queryFactory.createTaxonomyQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        TaxonomyResourceProvider provider = new TaxonomyResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "taxonomyName");
        Request userRequest = new InstanceRequest(requestProperties);

        Result result = provider.getResourceById(userRequest);

        assertEquals(1, result.getPropertyMaps().size());
        assertEquals(queryResultRow, result.getPropertyMaps().iterator().next());

        Request request = requestCapture.getValue();
        assertNull(request.getQueryString());
        assertEquals(0, request.getAdditionalSelectProperties().size());
        assertEquals(requestProperties, request.getProperties());

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
        expect(queryFactory.createTaxonomyQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(emptyResponse);
        replay(typeSystem, queryFactory, query);

        TaxonomyResourceProvider provider = new TaxonomyResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "taxonomyName");
        Request request = new InstanceRequest(requestProperties);

        provider.getResourceById(request);

        verify(typeSystem, queryFactory, query);
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
        queryResultRow1.put("mame", "taxonomyName1");
        queryResultRow1.put("description", "test taxonomy description");
        queryResultRow1.put("creation_time", "04/20/2016");

        Map<String, Object> queryResultRow2 = new HashMap<>();
        queryResult.add(queryResultRow2);
        queryResultRow2.put("mame", "taxonomyName2");
        queryResultRow2.put("description", "test taxonomy description 2");
        queryResultRow2.put("creation_time", "04/21/2016");

        // mock expectations
        expect(queryFactory.createTaxonomyQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        TaxonomyResourceProvider provider = new TaxonomyResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Request userRequest = new CollectionRequest(Collections.<String, Object>emptyMap(), "name:taxonomy*");
        Result result = provider.getResources(userRequest);

        assertEquals(2, result.getPropertyMaps().size());
        assertTrue(result.getPropertyMaps().contains(queryResultRow1));
        assertTrue(result.getPropertyMaps().contains(queryResultRow2));

        Request request = requestCapture.getValue();
        assertEquals("name:taxonomy*", request.getQueryString());
        assertEquals(0, request.getAdditionalSelectProperties().size());
        assertEquals(0, request.getProperties().size());

        verify(typeSystem, queryFactory, query);
    }

    @Test
    public void testGetResources_noResults() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        // empty result shouldn't result in exception for collection query
        Collection<Map<String, Object>> queryResult = new ArrayList<>();

        // mock expectations
        expect(queryFactory.createTaxonomyQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        TaxonomyResourceProvider provider = new TaxonomyResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Request userRequest = new CollectionRequest(Collections.<String, Object>emptyMap(), "name:taxonomy*");
        Result result = provider.getResources(userRequest);

        assertEquals(0, result.getPropertyMaps().size());

        Request request = requestCapture.getValue();
        assertEquals("name:taxonomy*", request.getQueryString());
        assertEquals(0, request.getAdditionalSelectProperties().size());
        assertEquals(0, request.getProperties().size());

        verify(typeSystem, queryFactory, query);
    }

    @Test(expectedExceptions = InvalidPayloadException.class)
    public void testCreateResource_invalidRequest__noName() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);

        // mock expectations
        replay(typeSystem, queryFactory, query);

        // taxonomy create request must contain 'name' property
        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("description", "test");
        Request userRequest = new InstanceRequest(requestProperties);

        TaxonomyResourceProvider provider = new TaxonomyResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.createResource(userRequest);
    }

    @Test(expectedExceptions = ResourceAlreadyExistsException.class)
    public void testCreateResource_invalidRequest__alreadyExists() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        // query is executed to see if resource already exists
        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow = new HashMap<>();
        queryResult.add(queryResultRow);
        queryResultRow.put("mame", "taxonomyName");
        queryResultRow.put("description", "test taxonomy description");
        queryResultRow.put("creation_time", "04/20/2016");

        // mock expectations
        expect(queryFactory.createTaxonomyQuery(capture(requestCapture))).andReturn(query);
        // returning result for query should result in ResourceAlreadyExistsException
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        // taxonomy create request must contain 'name' property
        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "taxonomyName");
        Request userRequest = new InstanceRequest(requestProperties);

        TaxonomyResourceProvider provider = new TaxonomyResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.createResource(userRequest);
    }

    @Test
    public void testCreateResource() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<ResourceDefinition> resourceDefinitionCapture = newCapture();
        Capture<Request> requestCapture = newCapture();

        // empty response indicates that resource doesn't already exist
        Collection<Map<String, Object>> queryResult = new ArrayList<>();

        // mock expectations
        expect(queryFactory.createTaxonomyQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        typeSystem.createEntity(capture(resourceDefinitionCapture), capture(requestCapture));
        replay(typeSystem, queryFactory, query);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "taxonomyName");
        Request userRequest = new InstanceRequest(requestProperties);

        TaxonomyResourceProvider provider = new TaxonomyResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.createResource(userRequest);

        assertEquals(new TaxonomyResourceDefinition().getTypeName(),
                resourceDefinitionCapture.getValue().getTypeName());

        Request request = requestCapture.getValue();
        assertNull(request.getQueryString());
        assertEquals(requestProperties, request.getProperties());

        verify(typeSystem, queryFactory, query);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCreateResources() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);

        // mock expectations
        replay(typeSystem, queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "taxonomyName");
        Request userRequest = new InstanceRequest(requestProperties);

        TaxonomyResourceProvider provider = new TaxonomyResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.createResources(userRequest);
    }

    @Test
    public void testDeleteResourceById() throws Exception {
        TermResourceProvider termResourceProvider = createStrictMock(TermResourceProvider.class);
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> getRequestCapture = newCapture();
        Capture<TermPath> termPathCapture = newCapture();
        Capture<ResourceDefinition> resourceDefinitionCapture = newCapture();
        Capture<Request> deleteRequestCapture = newCapture();

        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow = new HashMap<>();
        queryResult.add(queryResultRow);
        queryResultRow.put("name", "testTaxonomy");
        queryResultRow.put("id", "111-222-333");

        // mock expectations
        expect(queryFactory.createTaxonomyQuery(capture(getRequestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        termResourceProvider.deleteChildren(eq("111-222-333"), capture(termPathCapture));
        typeSystem.deleteEntity(capture(resourceDefinitionCapture), capture(deleteRequestCapture));
        replay(termResourceProvider, typeSystem, queryFactory, query);

        TaxonomyResourceProvider provider = new TestTaxonomyResourceProvider(typeSystem, termResourceProvider);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "testTaxonomy");
        Request userRequest = new InstanceRequest(requestProperties);

        // invoke method being tested
        provider.deleteResourceById(userRequest);

        Request getRequest = getRequestCapture.getValue();
        assertNull(getRequest.getQueryString());
        assertEquals(getRequest.getAdditionalSelectProperties().size(), 1);
        assertTrue(getRequest.getAdditionalSelectProperties().contains("id"));
        assertEquals(getRequest.getProperties().get("name"), "testTaxonomy");

        Request deleteRequest = deleteRequestCapture.getValue();
        assertNull(deleteRequest.getQueryString());
        assertEquals(deleteRequest.getAdditionalSelectProperties().size(), 1);
        assertTrue(deleteRequest.getAdditionalSelectProperties().contains("id"));
        assertEquals(deleteRequest.getProperties().get("name"), "testTaxonomy");

        ResourceDefinition resourceDefinition = resourceDefinitionCapture.getValue();
        assertTrue(resourceDefinition instanceof TaxonomyResourceDefinition);

        verify(termResourceProvider, typeSystem, queryFactory, query);
    }

    @Test(expectedExceptions = ResourceNotFoundException.class)
    public void testDeleteResourceById_404() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> getRequestCapture = newCapture();
        // mock expectations
        expect(queryFactory.createTaxonomyQuery(capture(getRequestCapture))).andReturn(query);
        expect(query.execute()).andThrow(new ResourceNotFoundException("test msg"));

        replay(typeSystem, queryFactory, query);

        TaxonomyResourceProvider provider = new TestTaxonomyResourceProvider(typeSystem, null);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("name", "badName");
        Request userRequest = new InstanceRequest(requestProperties);

        // invoke method being tested
        provider.deleteResourceById(userRequest);
    }


    private static class TestTaxonomyResourceProvider extends TaxonomyResourceProvider {
        private final TermResourceProvider termResourceProvider;
        public TestTaxonomyResourceProvider(AtlasTypeSystem typeSystem, TermResourceProvider termResourceProvider) {
            super(typeSystem);
            this.termResourceProvider = termResourceProvider;
        }

        @Override
        protected synchronized TermResourceProvider getTermResourceProvider() {
            return termResourceProvider;
        }
    }
}
