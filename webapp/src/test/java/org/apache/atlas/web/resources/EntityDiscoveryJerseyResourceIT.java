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

package org.apache.atlas.web.resources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasFullTextResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Search V2 Integration Tests.
 */
public class EntityDiscoveryJerseyResourceIT extends BaseResourceIT {
    private String dbName;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        dbName = "db" + randomString();
        createTypes();
        createInstance(createHiveDBInstanceBuiltIn(dbName));
    }

    @Test
    public void testSearchByDSL() throws Exception {
        String dslQuery = "from "+ DATABASE_TYPE_BUILTIN + " " + QUALIFIED_NAME + "=\"" + dbName + "\"";

        AtlasSearchResult searchResult = atlasClientV2.dslSearch(dslQuery);
        assertNotNull(searchResult);
        assertEquals(searchResult.getQueryText(), dslQuery);
        assertEquals(searchResult.getQueryType(), AtlasQueryType.DSL);

        List<AtlasEntityHeader> entities = searchResult.getEntities();
        assertNotNull(entities);
        assertEquals(entities.size(), 1);

        AtlasEntityHeader dbEntity = entities.get(0);
        assertEquals(dbEntity.getTypeName(), DATABASE_TYPE_BUILTIN);
        assertEquals(dbEntity.getDisplayText(), dbName);
        assertEquals(dbEntity.getStatus(), Status.ACTIVE);
        assertNotNull(dbEntity.getGuid());
        assertNull(searchResult.getAttributes());
        assertNull(searchResult.getFullTextResult());
    }

    @Test
    public void testSearchDSLLimits() throws Exception {
        String dslQuery = "from "+ DATABASE_TYPE_BUILTIN + " " + QUALIFIED_NAME + "=\"" + dbName + "\"";
        AtlasSearchResult searchResult = atlasClientV2.dslSearch(dslQuery);
        assertNotNull(searchResult);

        //higher limit, all results returned
        searchResult = atlasClientV2.dslSearchWithParams(dslQuery, 10, 0);
        assertEquals(searchResult.getEntities().size(), 1);

        //default limit and offset -1, all results returned
        searchResult = atlasClientV2.dslSearchWithParams(dslQuery, -1, -1);
        assertEquals(searchResult.getEntities().size(), 1);

        //uses the limit parameter passed
        searchResult = atlasClientV2.dslSearchWithParams(dslQuery, 1, 0);
        assertEquals(searchResult.getEntities().size(), 1);

        //uses the offset parameter passed
        searchResult = atlasClientV2.dslSearchWithParams(dslQuery, 10, 1);
        assertNull(searchResult.getEntities());

        //limit > 0
        searchResult = atlasClientV2.dslSearchWithParams(dslQuery, 0, 10);
        assertNull(searchResult.getEntities());

        //limit > maxlimit
        searchResult = atlasClientV2.dslSearchWithParams(dslQuery, Integer.MAX_VALUE, 10);
        assertNull(searchResult.getEntities());

        //offset >= 0
        searchResult = atlasClientV2.dslSearchWithParams(dslQuery, 10, -2);
        assertEquals(searchResult.getEntities().size(), 1);
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void testSearchByDSLForUnknownType() throws Exception {
        String dslQuery = "from blah";
        atlasClientV2.dslSearch(dslQuery);
    }

    @Test
    public void testSearchUsingDSL() throws Exception {
        String query = "from "+ DATABASE_TYPE_BUILTIN + " " + QUALIFIED_NAME + "=\"" + dbName + "\"";
        AtlasSearchResult searchResult = atlasClientV2.dslSearch(query);
        assertNotNull(searchResult);

        assertEquals(searchResult.getQueryText(), query);
        assertEquals(searchResult.getQueryType(), AtlasQueryType.DSL);
        List<AtlasEntityHeader> entities = searchResult.getEntities();
        assertNotNull(entities);
        assertEquals(entities.size(), 1);

        AtlasEntityHeader dbEntity = entities.get(0);
        assertEquals(dbEntity.getTypeName(), DATABASE_TYPE_BUILTIN);
        assertEquals(dbEntity.getDisplayText(), dbName);
        assertEquals(dbEntity.getStatus(), Status.ACTIVE);

        assertNotNull(dbEntity.getGuid());
        assertNull(searchResult.getAttributes());
        assertNull(searchResult.getFullTextResult());
    }

    @Test
    public void testSearchFullTextOnDSLFailure() throws Exception {
        String query = "*";
        AtlasSearchResult searchResult = atlasClientV2.fullTextSearch(query);
        assertNotNull(searchResult);
        assertEquals(searchResult.getQueryText(), query);
        assertEquals(searchResult.getQueryType(), AtlasQueryType.FULL_TEXT);
    }

    @Test(dependsOnMethods = "testSearchDSLLimits")
    public void testSearchUsingFullText() throws Exception {
        AtlasSearchResult searchResult = atlasClientV2.fullTextSearchWithParams(dbName, 10, 0);
        assertNotNull(searchResult);

        assertEquals(searchResult.getQueryText(), dbName);
        assertEquals(searchResult.getQueryType(), AtlasQueryType.FULL_TEXT);

        List<AtlasFullTextResult> fullTextResults = searchResult.getFullTextResult();
        assertEquals(fullTextResults.size(), 1);

        AtlasFullTextResult result = fullTextResults.get(0);
        assertNotNull(result.getEntity());
        assertEquals(result.getEntity().getTypeName(), DATABASE_TYPE_BUILTIN);
        assertNotNull(result.getScore());

        //API works without limit and offset
        String query = dbName;
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("query", query);
        searchResult = atlasClientV2.fullTextSearch(query);
        assertNotNull(searchResult);
        assertEquals(searchResult.getFullTextResult().size(), 1);

        //verify passed in limits and offsets are used
        //higher limit and 0 offset returns all results
        searchResult = atlasClientV2.fullTextSearchWithParams(query, 10, 0);
        assertEquals(searchResult.getFullTextResult().size(), 1);

        //offset is used
        searchResult = atlasClientV2.fullTextSearchWithParams(query, 10, 1);
        assertEquals(searchResult.getFullTextResult().size(), 1);

        //limit is used
        searchResult = atlasClientV2.fullTextSearchWithParams(query, 1, 0);
        assertEquals(searchResult.getFullTextResult().size(), 1);

        //higher offset returns 0 results
        searchResult = atlasClientV2.fullTextSearchWithParams(query, 1, 2);
        assertEquals(searchResult.getFullTextResult().size(), 1);
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
}
