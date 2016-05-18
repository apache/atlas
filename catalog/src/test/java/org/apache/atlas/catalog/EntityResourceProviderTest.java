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
 * Unit Tests for EntityResourceProvider.
 */
public class EntityResourceProviderTest {
    @Test
    public void testGetResource() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);
        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> queryResultRow = new HashMap<>();
        queryResult.add(queryResultRow);
        queryResultRow.put("id", "1");
        queryResultRow.put("creation_time", "04/20/2016");

        // mock expectations
        expect(queryFactory.createEntityQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        EntityResourceProvider provider = new EntityResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("id", "1");
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
        expect(queryFactory.createEntityQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(emptyResponse);
        replay(typeSystem, queryFactory, query);

        EntityResourceProvider provider = new EntityResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("id", "1");
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
        queryResultRow1.put("mame", "entity1");
        queryResultRow1.put("description", "test entity description");
        queryResultRow1.put("creation_time", "04/20/2016");

        Map<String, Object> queryResultRow2 = new HashMap<>();
        queryResult.add(queryResultRow2);
        queryResultRow2.put("mame", "entity2");
        queryResultRow2.put("description", "test entity description 2");
        queryResultRow2.put("creation_time", "04/21/2016");

        // mock expectations
        expect(queryFactory.createEntityQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        EntityResourceProvider provider = new EntityResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Request userRequest = new CollectionRequest(Collections.<String, Object>emptyMap(), "name:entity*");
        Result result = provider.getResources(userRequest);

        assertEquals(2, result.getPropertyMaps().size());
        assertTrue(result.getPropertyMaps().contains(queryResultRow1));
        assertTrue(result.getPropertyMaps().contains(queryResultRow2));

        Request request = requestCapture.getValue();
        assertEquals("name:entity*", request.getQueryString());
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
        expect(queryFactory.createEntityQuery(capture(requestCapture))).andReturn(query);
        expect(query.execute()).andReturn(queryResult);
        replay(typeSystem, queryFactory, query);

        EntityResourceProvider provider = new EntityResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        Request userRequest = new CollectionRequest(Collections.<String, Object>emptyMap(), "name:entity*");
        Result result = provider.getResources(userRequest);

        assertEquals(0, result.getPropertyMaps().size());

        Request request = requestCapture.getValue();
        assertEquals("name:entity*", request.getQueryString());
        assertEquals(0, request.getAdditionalSelectProperties().size());
        assertEquals(0, request.getProperties().size());

        verify(typeSystem, queryFactory, query);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCreateResource() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);

        // mock expectations
        replay(typeSystem, queryFactory, query);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("id", "1");
        Request userRequest = new InstanceRequest(requestProperties);

        EntityResourceProvider provider = new EntityResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.createResource(userRequest);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCreateResources() throws Exception {
        AtlasTypeSystem typeSystem = createStrictMock(AtlasTypeSystem.class);
        QueryFactory queryFactory = createStrictMock(QueryFactory.class);
        AtlasQuery query = createStrictMock(AtlasQuery.class);

        // mock expectations
        replay(typeSystem, queryFactory, query);

        Map<String, Object> requestProperties = new HashMap<>();
        requestProperties.put("id", "1");
        Request userRequest = new InstanceRequest(requestProperties);

        EntityResourceProvider provider = new EntityResourceProvider(typeSystem);
        provider.setQueryFactory(queryFactory);

        provider.createResources(userRequest);
    }

}
