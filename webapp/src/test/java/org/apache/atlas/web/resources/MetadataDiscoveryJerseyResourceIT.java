/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.resources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.web.util.Servlets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Search Integration Tests.
 */
public class MetadataDiscoveryJerseyResourceIT extends BaseResourceIT {

    private String tagName;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        createTypes();
        createInstance();
    }

    @Test
    public void testSearchByDSL() throws Exception {
        String dslQuery = "from dsl_test_type";
        WebResource resource = service.path("api/atlas/discovery/search/dsl").queryParam("query", dslQuery);

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        assertEquals(response.getString("query"), dslQuery);
        assertEquals(response.getString("queryType"), "dsl");

        JSONArray results = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertNotNull(results);
        assertEquals(results.length(), 1);

        int numRows = response.getInt(AtlasClient.COUNT);
        assertEquals(numRows, 1);
    }

    @Test
    public void testSearchDSLLimits() throws Exception {
        Referenceable entity = new Referenceable("dsl_test_type");
        entity.set("name", randomString());
        entity.set("description", randomString());
        createInstance(entity);

        //search without new parameters of limit and offset should work
        String dslQuery = "from dsl_test_type";
        WebResource resource = service.path("api/atlas/discovery/search/dsl").queryParam("query", dslQuery);

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                                                .method(HttpMethod.GET, ClientResponse.class);
        assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        //higher limit, all results returned
        JSONArray results = serviceClient.searchByDSL(dslQuery, 10, 0);
        assertEquals(results.length(), 2);

        //default limit and offset -1, all results returned
        results = serviceClient.searchByDSL(dslQuery, -1, -1);
        assertEquals(results.length(), 2);

        //uses the limit parameter passed
        results = serviceClient.searchByDSL(dslQuery, 1, 0);
        assertEquals(results.length(), 1);

        //uses the offset parameter passed
        results = serviceClient.searchByDSL(dslQuery, 10, 1);
        assertEquals(results.length(), 1);

        //limit > 0
        try {
            serviceClient.searchByDSL(dslQuery, 0, 10);
            fail("Expected BAD_REQUEST");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus(), ClientResponse.Status.BAD_REQUEST, "Got " + e.getStatus());
        }

        //limit > maxlimit
        try {
            serviceClient.searchByDSL(dslQuery, Integer.MAX_VALUE, 10);
            fail("Expected BAD_REQUEST");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus(), ClientResponse.Status.BAD_REQUEST, "Got " + e.getStatus());
        }

        //offset >= 0
        try {
            serviceClient.searchByDSL(dslQuery, 10, -2);
            fail("Expected BAD_REQUEST");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus(), ClientResponse.Status.BAD_REQUEST, "Got " + e.getStatus());
        }
    }

    @Test
    public void testSearchByDSLForUnknownType() throws Exception {
        String dslQuery = "from blah";
        WebResource resource = service.path("api/atlas/discovery/search/dsl").queryParam("query", dslQuery);

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        assertEquals(clientResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testSearchUsingGremlin() throws Exception {
        String query = "g.V.has('type', 'dsl_test_type').toList()";
        WebResource resource = service.path("api/atlas/discovery/search/gremlin").queryParam("query", query);

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        assertEquals(response.getString("query"), query);
        assertEquals(response.getString("queryType"), "gremlin");
    }

    @Test
    public void testSearchUsingDSL() throws Exception {
        String query = "from dsl_test_type";
        WebResource resource = service.path("api/atlas/discovery/search").queryParam("query", query);

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        assertEquals(response.getString("query"), query);
        assertEquals(response.getString("queryType"), "dsl");
    }

    @Test
    public void testSearchUsingFullText() throws Exception {
        JSONObject response = serviceClient.searchByFullText(tagName, 10, 0);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        assertEquals(response.getString("query"), tagName);
        assertEquals(response.getString("queryType"), "full-text");

        JSONArray results = response.getJSONArray(AtlasClient.RESULTS);
        assertEquals(results.length(), 1, "Results: " + results);

        JSONObject row = results.getJSONObject(0);
        Assert.assertNotNull(row.get("guid"));
        assertEquals(row.getString("typeName"), "dsl_test_type");
        Assert.assertNotNull(row.get("score"));

        int numRows = response.getInt(AtlasClient.COUNT);
        assertEquals(numRows, 1);
    }

    private void createTypes() throws Exception {
        HierarchicalTypeDefinition<ClassType> dslTestTypeDefinition = TypesUtil
                .createClassTypeDef("dsl_test_type", ImmutableSet.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<TraitType> classificationTraitDefinition = TypesUtil
                .createTraitTypeDef("Classification", ImmutableSet.<String>of(),
                        TypesUtil.createRequiredAttrDef("tag", DataTypes.STRING_TYPE));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                        ImmutableList.of(classificationTraitDefinition), ImmutableList.of(dslTestTypeDefinition));
        createType(typesDef);
    }

    private Id createInstance() throws Exception {
        Referenceable entityInstance = new Referenceable("dsl_test_type", "Classification");
        entityInstance.set("name", "foo name");
        entityInstance.set("description", "bar description");

        Struct traitInstance = (Struct) entityInstance.getTrait("Classification");
        tagName = randomString();
        traitInstance.set("tag", tagName);

        List<String> traits = entityInstance.getTraits();
        assertEquals(traits.size(), 1);

        return createInstance(entityInstance);
    }
}
