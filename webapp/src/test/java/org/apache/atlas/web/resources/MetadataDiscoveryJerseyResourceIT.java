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
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.atlas.AtlasClient;
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
import org.apache.atlas.typesystem.types.TypeUtils;
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

/**
 * Search Integration Tests.
 */
public class MetadataDiscoveryJerseyResourceIT extends BaseResourceIT {

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
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        Assert.assertEquals(response.getString("query"), dslQuery);
        Assert.assertEquals(response.getString("queryType"), "dsl");

        JSONObject results = response.getJSONObject(AtlasClient.RESULTS);
        Assert.assertNotNull(results);

        JSONArray rows = results.getJSONArray(AtlasClient.ROWS);
        Assert.assertEquals(rows.length(), 1);

        int numRows = response.getInt(AtlasClient.COUNT);
        Assert.assertEquals(numRows, 1);

    }

    @Test
    public void testSearchByDSLForUnknownType() throws Exception {
        String dslQuery = "from blah";
        WebResource resource = service.path("api/atlas/discovery/search/dsl").queryParam("query", dslQuery);

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testSearchUsingGremlin() throws Exception {
        String query = "g.V.has('type', 'dsl_test_type').toList()";
        WebResource resource = service.path("api/atlas/discovery/search").queryParam("query", query);

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        Assert.assertEquals(response.getString("query"), query);
        Assert.assertEquals(response.getString("queryType"), "gremlin");
    }

    @Test
    public void testSearchUsingDSL() throws Exception {
        String query = "from dsl_test_type";
        WebResource resource = service.path("api/atlas/discovery/search").queryParam("query", query);

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        Assert.assertEquals(response.getString("query"), query);
        Assert.assertEquals(response.getString("queryType"), "dsl");
    }

    @Test(enabled = false)
    public void testSearchUsingFullText() throws Exception {
        String query = "foundation_etl";
        JSONObject response = serviceClient.searchByFullText(query);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        Assert.assertEquals(response.getString("query"), query);
        Assert.assertEquals(response.getString("queryType"), "full-text");

        JSONArray results = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertEquals(results.length(), 1);

        JSONObject row = results.getJSONObject(0);
        Assert.assertNotNull(row.get("guid"));
        Assert.assertEquals(row.getString("typeName"), "dsl_test_type");
        Assert.assertNotNull(row.get("score"));

        int numRows = response.getInt(AtlasClient.COUNT);
        Assert.assertEquals(numRows, 1);
    }

    private void createTypes() throws Exception {
        HierarchicalTypeDefinition<ClassType> dslTestTypeDefinition = TypesUtil
                .createClassTypeDef("dsl_test_type", ImmutableList.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<TraitType> classificationTraitDefinition = TypesUtil
                .createTraitTypeDef("Classification", ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("tag", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef("PII_TYPE", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> phiTrait =
                TypesUtil.createTraitTypeDef("PHI", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> pciTrait =
                TypesUtil.createTraitTypeDef("PCI", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> soxTrait =
                TypesUtil.createTraitTypeDef("SOX", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> secTrait =
                TypesUtil.createTraitTypeDef("SEC", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> financeTrait =
                TypesUtil.createTraitTypeDef("Finance", ImmutableList.<String>of());

        TypesDef typesDef = TypeUtils
                .getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                        ImmutableList
                                .of(classificationTraitDefinition, piiTrait, phiTrait, pciTrait, soxTrait, secTrait,
                                        financeTrait), ImmutableList.of(dslTestTypeDefinition));
        createType(typesDef);
    }

    private Id createInstance() throws Exception {
        Referenceable entityInstance =
                new Referenceable("dsl_test_type", "Classification", "PII_TYPE", "PHI", "PCI", "SOX", "SEC", "Finance");
        entityInstance.set("name", "foo name");
        entityInstance.set("description", "bar description");

        Struct traitInstance = (Struct) entityInstance.getTrait("Classification");
        traitInstance.set("tag", "foundation_etl");

        List<String> traits = entityInstance.getTraits();
        Assert.assertEquals(traits.size(), 7);

        return createInstance(entityInstance);
    }
}
