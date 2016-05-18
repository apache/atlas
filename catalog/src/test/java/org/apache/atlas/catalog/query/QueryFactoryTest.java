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

package org.apache.atlas.catalog.query;

import org.apache.atlas.catalog.CollectionRequest;
import org.apache.atlas.catalog.InstanceRequest;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.TermPath;
import org.apache.atlas.catalog.definition.EntityResourceDefinition;
import org.apache.atlas.catalog.definition.EntityTagResourceDefinition;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Unit tests for QueryFactory.
 */
public class QueryFactoryTest {
    @Test
    public void testCreateTaxonomyQuery() throws Exception {
        Map<String, Object> requestProps = new HashMap<>();
        requestProps.put("name", "test_taxonomy");
        Request request = new InstanceRequest(requestProps);

        QueryFactory factory = new QueryFactory();
        AtlasTaxonomyQuery query = (AtlasTaxonomyQuery) factory.createTaxonomyQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), TermQueryExpression.class);
        assertEquals(queryExpression.getField(), "name");
        assertEquals(queryExpression.getExpectedValue(), "test_taxonomy");
        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getTypeName(), "Taxonomy");
    }

    @Test
    public void testCreateTermQuery() throws Exception {
        Map<String, Object> requestProps = new HashMap<>();
        requestProps.put("name", "test_taxonomy.term1");
        requestProps.put("termPath", new TermPath("test_taxonomy.term1"));
        Request request = new InstanceRequest(requestProps);



        QueryFactory factory = new QueryFactory();
        AtlasTermQuery query = (AtlasTermQuery) factory.createTermQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), TermQueryExpression.class);
        assertEquals(queryExpression.getField(), "name");
        assertEquals(queryExpression.getExpectedValue(), "test_taxonomy.term1");
        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getTypeName(), "Term");
    }

    @Test
    public void testCreateEntityQuery() throws Exception {
        Map<String, Object> requestProps = new HashMap<>();
        requestProps.put("id", "foo");
        Request request = new InstanceRequest(requestProps);

        QueryFactory factory = new QueryFactory();
        AtlasEntityQuery query = (AtlasEntityQuery) factory.createEntityQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), TermQueryExpression.class);
        assertEquals(queryExpression.getField(), "id");
        assertEquals(queryExpression.getExpectedValue(), "foo");
        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getClass(), EntityResourceDefinition.class);
    }

    @Test
    public void testCreateEntityTagQuery() throws Exception {
        Map<String, Object> requestProps = new HashMap<>();
        requestProps.put("id", "entity_id");
        requestProps.put("name", "test_taxonomy.term1");
        Request request = new InstanceRequest(requestProps);

        QueryFactory factory = new QueryFactory();
        AtlasEntityTagQuery query = (AtlasEntityTagQuery) factory.createEntityTagQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), TermQueryExpression.class);
        assertEquals(queryExpression.getField(), "name");
        assertEquals(queryExpression.getExpectedValue(), "test_taxonomy.term1");
        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getClass(), EntityTagResourceDefinition.class);
    }

    @Test
    public void testCollectionQuery_TermQuery() throws Exception {
        String queryString = "name:test_taxonomy";
        Request request = new CollectionRequest(Collections.<String, Object>emptyMap(), queryString);

        QueryFactory factory = new QueryFactory();
        AtlasTaxonomyQuery query = (AtlasTaxonomyQuery) factory.createTaxonomyQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), TermQueryExpression.class);
        assertEquals(queryExpression.getField(), "name");
        assertEquals(queryExpression.getExpectedValue(), "test_taxonomy");
        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getTypeName(), "Taxonomy");
    }

    @Test
    public void testCollectionQuery_PrefixQuery() throws Exception {
        String queryString = "name:t*";
        Request request = new CollectionRequest(Collections.<String, Object>emptyMap(), queryString);

        QueryFactory factory = new QueryFactory();
        AtlasTaxonomyQuery query = (AtlasTaxonomyQuery) factory.createTaxonomyQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), PrefixQueryExpression.class);
        assertEquals(queryExpression.getField(), "name");
        assertEquals(queryExpression.getExpectedValue(), "t");
        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getTypeName(), "Taxonomy");
    }

    @Test
    public void testCollectionQuery_TermRangeQuery() throws Exception {
        String queryString = "creation_time:[2013-01-01:07:29:00 TO 2017-01-02]";
        Request request = new CollectionRequest(Collections.<String, Object>emptyMap(), queryString);

        QueryFactory factory = new QueryFactory();
        AtlasTaxonomyQuery query = (AtlasTaxonomyQuery) factory.createTaxonomyQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), TermRangeQueryExpression.class);
        assertEquals(queryExpression.getField(), "creation_time");
        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getTypeName(), "Taxonomy");
    }

    @Test
    public void testCollectionQuery_WildcardQuery() throws Exception {
        String queryString = "name:ta?onomy";
        Request request = new CollectionRequest(Collections.<String, Object>emptyMap(), queryString);

        QueryFactory factory = new QueryFactory();
        AtlasTaxonomyQuery query = (AtlasTaxonomyQuery) factory.createTaxonomyQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), WildcardQueryExpression.class);
        assertEquals(queryExpression.getField(), "name");
        assertEquals(queryExpression.getExpectedValue(), "ta?onomy");
        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getTypeName(), "Taxonomy");
    }

    @Test
    public void testCollectionQuery_BooleanQuery() throws Exception {
        String queryString = "name:foo OR name:bar";
        Request request = new CollectionRequest(Collections.<String, Object>emptyMap(), queryString);

        QueryFactory factory = new QueryFactory();
        AtlasTaxonomyQuery query = (AtlasTaxonomyQuery) factory.createTaxonomyQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), BooleanQueryExpression.class);

        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getTypeName(), "Taxonomy");
    }

    @Test
    public void testCollectionQuery_ProjectionQuery() throws Exception {
        String queryString = "relation/name:foo";
        Request request = new CollectionRequest(Collections.<String, Object>emptyMap(), queryString);

        QueryFactory factory = new QueryFactory();
        AtlasTaxonomyQuery query = (AtlasTaxonomyQuery) factory.createTaxonomyQuery(request);

        QueryExpression queryExpression = query.getQueryExpression();
        assertEquals(queryExpression.getClass(), ProjectionQueryExpression.class);

        ProjectionQueryExpression projectionExpression = (ProjectionQueryExpression) queryExpression;
        QueryExpression underlyingExpression = projectionExpression.getUnderlyingExpression();
        assertEquals(underlyingExpression.getClass(), TermQueryExpression.class);
        assertEquals(underlyingExpression.getField(), QueryFactory.escape("relation/name"));
        assertEquals(underlyingExpression.getExpectedValue(), "foo");

        assertEquals(query.getRequest(), request);
        assertEquals(query.getResourceDefinition().getTypeName(), "Taxonomy");
    }
}
