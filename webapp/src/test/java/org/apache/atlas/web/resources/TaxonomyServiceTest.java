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

package org.apache.atlas.web.resources;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.atlas.AtlasException;
import org.apache.atlas.catalog.AtlasTypeSystem;
import org.apache.atlas.catalog.JsonSerializer;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.ResourceProvider;
import org.apache.atlas.catalog.Result;
import org.apache.atlas.catalog.TaxonomyResourceProvider;
import org.apache.atlas.catalog.TermPath;
import org.apache.atlas.services.MetadataService;
import org.easymock.Capture;
import org.testng.annotations.Test;

/**
 * Unit tests for TaxonomyService.
 */
public class TaxonomyServiceTest {
    @Test
    public void testGetTaxonomy() throws Exception {
        String taxonomyName = "testTaxonomy";
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        JsonSerializer serializer = createStrictMock(JsonSerializer.class);
        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> resultPropertyMaps = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("name", "testTaxonomy");
        resultPropertyMaps.add(propertyMap);
        Result result = new Result(resultPropertyMaps);

        expect(taxonomyResourceProvider.getResourceById(capture(requestCapture))).andReturn(result);
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        expect(serializer.serialize(result, uriInfo)).andReturn("Taxonomy Get Response");
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer);

        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
        Response response = service.getTaxonomy(null, uriInfo, taxonomyName);

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        Map<String, Object> requestProperties = request.getQueryProperties();
        assertEquals(requestProperties.size(), 1);
        assertEquals(requestProperties.get("name"), taxonomyName);

        assertEquals(response.getStatus(), 200);
        assertEquals(response.getEntity(), "Taxonomy Get Response");

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
    }

    @Test
    public void testGetTaxonomies() throws Exception {
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies?name:testTaxonomy");
        JsonSerializer serializer = createStrictMock(JsonSerializer.class);
        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> resultPropertyMaps = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("name", "testTaxonomy");
        resultPropertyMaps.add(propertyMap);
        Result result = new Result(resultPropertyMaps);

        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        expect(taxonomyResourceProvider.getResources(capture(requestCapture))).andReturn(result);
        expect(serializer.serialize(result, uriInfo)).andReturn("Taxonomy Get Response");
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
        Response response = service.getTaxonomies(null, uriInfo);

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertTrue(request.getQueryProperties().isEmpty());
        assertEquals(request.getQueryString(), "name:testTaxonomy");

        assertEquals(response.getStatus(), 200);
        assertEquals(response.getEntity(), "Taxonomy Get Response");

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
    }

    @Test
    public void testCreateTaxonomy() throws Exception {
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy");
        Capture<Request> requestCapture = newCapture();

        String body = "{ \"description\" : \"test description\" } ";
        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        taxonomyResourceProvider.createResource(capture(requestCapture));
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, null);
        Response response = service.createTaxonomy(body, null, uriInfo, "testTaxonomy");

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryProperties().size(), 2);
        assertEquals(request.getQueryProperties().get("name"), "testTaxonomy");
        assertEquals(request.getQueryProperties().get("description"), "test description");
        assertNull(request.getQueryString());

        assertEquals(response.getStatus(), 201);
        BaseService.Results createResults = (BaseService.Results) response.getEntity();
        assertEquals(createResults.href, "http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy");
        assertEquals(createResults.status, 201);

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);
    }

    @Test
    public void testDeleteTaxonomy() throws Exception {
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy");
        Capture<Request> requestCapture = newCapture();

        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        taxonomyResourceProvider.deleteResourceById(capture(requestCapture));
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, null);
        Response response = service.deleteTaxonomy(null, uriInfo, "testTaxonomy");

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryProperties().size(), 1);
        assertEquals(request.getQueryProperties().get("name"), "testTaxonomy");
        assertNull(request.getQueryString());

        assertEquals(response.getStatus(), 200);
        BaseService.Results createResults = (BaseService.Results) response.getEntity();
        assertEquals(createResults.href, "http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy");
        assertEquals(createResults.status, 200);

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);
    }

    @Test
    public void testGetTaxonomyTerm() throws Exception {
        String taxonomyName = "testTaxonomy";
        String termName = "testTaxonomy.termName";
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        JsonSerializer serializer = createStrictMock(JsonSerializer.class);
        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> resultPropertyMaps = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("name", "testTaxonomy.termName");
        resultPropertyMaps.add(propertyMap);
        Result result = new Result(resultPropertyMaps);

        expect(termResourceProvider.getResourceById(capture(requestCapture))).andReturn(result);
        expect(serializer.serialize(result, uriInfo)).andReturn("Taxonomy Term Get Response");
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer);

        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
        Response response = service.getTaxonomyTerm(null, uriInfo, taxonomyName, termName);

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        Map<String, Object> requestProperties = request.getQueryProperties();
        assertEquals(requestProperties.size(), 1);
        TermPath termPath = (TermPath) request.getQueryProperties().get("termPath");
        assertEquals(termPath.getFullyQualifiedName(), "testTaxonomy.testTaxonomy.termName");

        assertEquals(response.getStatus(), 200);
        assertEquals(response.getEntity(), "Taxonomy Term Get Response");

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
    }

    @Test
    public void testGetTaxonomyTerms() throws Exception {
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms?name:testTaxonomy.testTerm");
        JsonSerializer serializer = createStrictMock(JsonSerializer.class);
        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> resultPropertyMaps = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("name", "testTaxonomy.testTerm");
        resultPropertyMaps.add(propertyMap);
        Result result = new Result(resultPropertyMaps);

        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        expect(termResourceProvider.getResources(capture(requestCapture))).andReturn(result);
        expect(serializer.serialize(result, uriInfo)).andReturn("Taxonomy Term Get Response");
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
        Response response = service.getTaxonomyTerms(null, uriInfo, "testTaxonomy");

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryProperties().size(), 1);
        TermPath termPath = (TermPath) request.getQueryProperties().get("termPath");
        assertEquals(termPath.getFullyQualifiedName(), "testTaxonomy");
        assertEquals(request.getQueryString(), "name:testTaxonomy.testTerm");

        assertEquals(response.getStatus(), 200);
        assertEquals(response.getEntity(), "Taxonomy Term Get Response");

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
    }

    @Test
    public void testGetSubTerms_instance() throws Exception {
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm/terms/testTerm2");
        JsonSerializer serializer = createStrictMock(JsonSerializer.class);
        PathSegment segment1 = createNiceMock(PathSegment.class);
        PathSegment segment2 = createNiceMock(PathSegment.class);
        PathSegment segment3 = createNiceMock(PathSegment.class);

        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> resultPropertyMaps = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("name", "testTaxonomy.testTerm.testTerm2");
        resultPropertyMaps.add(propertyMap);
        Result result = new Result(resultPropertyMaps);

        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        expect(uriInfo.getPathSegments()).andReturn(Arrays.asList(segment1, segment2, segment3));
        expect(segment3.getPath()).andReturn("testTerm2");
        expect(termResourceProvider.getResourceById(capture(requestCapture))).andReturn(result);
        expect(serializer.serialize(result, uriInfo)).andReturn("Taxonomy Term Get Response");
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer,
                segment1, segment2, segment3);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
        Response response = service.getSubTerms(null, uriInfo, "testTaxonomy", "testTerm", "/terms/testTerm2");

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryProperties().size(), 1);
        TermPath termPath = (TermPath) request.getQueryProperties().get("termPath");
        assertEquals(termPath.getFullyQualifiedName(), "testTaxonomy.testTerm.testTerm2");
        assertNull(request.getQueryString());

        assertEquals(response.getStatus(), 200);
        assertEquals(response.getEntity(), "Taxonomy Term Get Response");

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer,
                segment1, segment2, segment3);
    }

    @Test
    public void testGetSubTerms_collection() throws Exception {
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm/terms/testTerm2/terms?name:testTaxonomy.testTerm.testTerm2.testTerm3");
        JsonSerializer serializer = createStrictMock(JsonSerializer.class);
        // would actually be more segments but at this time only the last segment is used
        PathSegment segment1 = createNiceMock(PathSegment.class);
        PathSegment segment2 = createNiceMock(PathSegment.class);
        PathSegment segment3 = createNiceMock(PathSegment.class);

        Capture<Request> requestCapture = newCapture();

        Collection<Map<String, Object>> resultPropertyMaps = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("name", "testTaxonomy.testTerm.testTerm2.testTerm3");
        resultPropertyMaps.add(propertyMap);
        Result result = new Result(resultPropertyMaps);

        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        expect(uriInfo.getPathSegments()).andReturn(Arrays.asList(segment1, segment2, segment3));
        expect(segment3.getPath()).andReturn("terms");

        expect(termResourceProvider.getResources(capture(requestCapture))).andReturn(result);
        expect(serializer.serialize(result, uriInfo)).andReturn("Taxonomy Term Get Response");
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer,
                segment1, segment2, segment3);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, serializer);
        Response response = service.getSubTerms(null, uriInfo, "testTaxonomy", "testTerm", "/terms/testTerm2/terms");

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryProperties().size(), 1);
        TermPath termPath = (TermPath) request.getQueryProperties().get("termPath");
        assertEquals(termPath.getFullyQualifiedName(), "testTaxonomy.testTerm.testTerm2.");
        assertEquals(request.getQueryString(), "name:testTaxonomy.testTerm.testTerm2.testTerm3");

        assertEquals(response.getStatus(), 200);
        assertEquals(response.getEntity(), "Taxonomy Term Get Response");

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider, serializer,
                segment1, segment2, segment3);
    }

    @Test
    public void testCreateTerm() throws Exception {
        String taxonomyName = "testTaxonomy";
        String termName = "testTerm";
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm");
        Capture<Request> requestCapture = newCapture();

        String body = "{ \"description\" : \"test description\" } ";
        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        termResourceProvider.createResource(capture(requestCapture));
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, null);
        Response response = service.createTerm(body, null, uriInfo, taxonomyName, termName);

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryProperties().size(), 2);
        assertEquals(request.getQueryProperties().get("description"), "test description");
        TermPath termPath = (TermPath) request.getQueryProperties().get("termPath");
        assertEquals(termPath.getFullyQualifiedName(), "testTaxonomy.testTerm");
        assertNull(request.getQueryString());

        assertEquals(response.getStatus(), 201);
        BaseService.Results createResults = (BaseService.Results) response.getEntity();
        assertEquals(createResults.href, "http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm");
        assertEquals(createResults.status, 201);

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);
    }

    @Test
    public void testCreateSubTerm() throws Exception {
        String taxonomyName = "testTaxonomy";
        String termName = "testTerm";
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm/terms/testTerm2");
        Capture<Request> requestCapture = newCapture();

        String body = "{ \"description\" : \"test description\" } ";
        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        termResourceProvider.createResource(capture(requestCapture));
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, null);
        Response response = service.createSubTerm(body, null, uriInfo, taxonomyName, termName, "/terms/testTerm2");

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryProperties().size(), 2);
        assertEquals(request.getQueryProperties().get("description"), "test description");
        TermPath termPath = (TermPath) request.getQueryProperties().get("termPath");
        assertEquals(termPath.getFullyQualifiedName(), "testTaxonomy.testTerm.testTerm2");
        assertNull(request.getQueryString());

        assertEquals(response.getStatus(), 201);
        BaseService.Results createResults = (BaseService.Results) response.getEntity();
        assertEquals(createResults.href, "http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm/terms/testTerm2");
        assertEquals(createResults.status, 201);

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);
    }

    @Test
    public void testDeleteTerm() throws Exception {
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm");
        Capture<Request> requestCapture = newCapture();

        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        termResourceProvider.deleteResourceById(capture(requestCapture));
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, null);
        Response response = service.deleteTerm(null, uriInfo, "testTaxonomy", "testTerm");

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryProperties().size(), 1);
        TermPath termPath = (TermPath) request.getQueryProperties().get("termPath");
        assertEquals(termPath.getFullyQualifiedName(), "testTaxonomy.testTerm");
        assertNull(request.getQueryString());

        assertEquals(response.getStatus(), 200);
        BaseService.Results createResults = (BaseService.Results) response.getEntity();
        assertEquals(createResults.href, "http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm");
        assertEquals(createResults.status, 200);

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);
    }

    @Test
    public void testDeleteSubTerm() throws Exception {
        MetadataService metadataService = createStrictMock(MetadataService.class);
        ResourceProvider taxonomyResourceProvider = createStrictMock(ResourceProvider.class);
        ResourceProvider termResourceProvider = createStrictMock(ResourceProvider.class);
        UriInfo uriInfo = createNiceMock(UriInfo.class);
        URI uri = new URI("http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm/terms/testTerm2");
        Capture<Request> requestCapture = newCapture();

        // set mock expectations
        expect(uriInfo.getRequestUri()).andReturn(uri);
        termResourceProvider.deleteResourceById(capture(requestCapture));
        expect(metadataService.getTypeDefinition(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE)).andReturn(TaxonomyResourceProvider.TAXONOMY_TERM_TYPE + "-definition");
        replay(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);

        // instantiate service and invoke method being tested
        TestTaxonomyService service = new TestTaxonomyService(
                metadataService, taxonomyResourceProvider, termResourceProvider, null);
        Response response = service.deleteSubTerm(null, uriInfo, "testTaxonomy", "testTerm", "terms/testTerm2");

        assertTrue(service.wasTransactionInitialized());

        Request request = requestCapture.getValue();
        assertEquals(request.getQueryProperties().size(), 1);
        TermPath termPath = (TermPath) request.getQueryProperties().get("termPath");
        assertEquals(termPath.getFullyQualifiedName(), "testTaxonomy.testTerm.testTerm2");
        assertNull(request.getQueryString());

        assertEquals(response.getStatus(), 200);
        BaseService.Results createResults = (BaseService.Results) response.getEntity();
        assertEquals(createResults.href, "http://localhost:21000/api/atlas/v1/taxonomies/testTaxonomy/terms/testTerm/terms/testTerm2");
        assertEquals(createResults.status, 200);

        verify(uriInfo, metadataService, taxonomyResourceProvider, termResourceProvider);
    }

    private static class TestTaxonomyService extends TaxonomyService {
        private final ResourceProvider testTaxonomyResourceProvider;
        private final ResourceProvider testTermResourceProvider;
        private final JsonSerializer testSerializer;
        private boolean transactionInitialized = false;

        public TestTaxonomyService(MetadataService metadataService,
                                   ResourceProvider taxonomyProvider,
                                   ResourceProvider termResourceProvider,
                                   JsonSerializer serializer) throws AtlasException {

            testTaxonomyResourceProvider = taxonomyProvider;
            testTermResourceProvider = termResourceProvider;
            testSerializer = serializer;
            setMetadataService(metadataService);
        }

        @Override
        protected ResourceProvider createTaxonomyResourceProvider(AtlasTypeSystem typeSystem) {
            return testTaxonomyResourceProvider;
        }

        @Override
        protected ResourceProvider createTermResourceProvider(AtlasTypeSystem typeSystem) {
            return testTermResourceProvider;
        }

        @Override
        protected JsonSerializer getSerializer() {
            return testSerializer;
        }

        public boolean wasTransactionInitialized() {
            return transactionInitialized;
        }
    }
}
