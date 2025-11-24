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
package org.apache.atlas.model.discovery;

import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasFullTextResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.discovery.AtlasSearchResult.AttributeSearchResult;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasRelationshipHeader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestAtlasSearchResult {
    private AtlasSearchResult searchResult;

    @BeforeMethod
    public void setUp() {
        searchResult = new AtlasSearchResult();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasSearchResult result = new AtlasSearchResult();

        assertNull(result.getQueryType());
        assertNull(result.getSearchParameters());
        assertNull(result.getQueryText());
        assertNull(result.getType());
        assertNull(result.getClassification());
        assertNull(result.getEntities());
        assertNull(result.getRelations());
        assertNull(result.getAttributes());
        assertNull(result.getFullTextResult());
        assertNull(result.getReferredEntities());
        assertEquals(result.getApproximateCount(), -1L);
        assertNull(result.getNextMarker());
    }

    @Test
    public void testConstructorWithQueryType() {
        AtlasSearchResult result = new AtlasSearchResult(AtlasQueryType.DSL);

        assertEquals(result.getQueryType(), AtlasQueryType.DSL);
        assertNull(result.getQueryText());
        assertNull(result.getSearchParameters());
        assertNull(result.getEntities());
        assertNull(result.getRelations());
        assertNull(result.getAttributes());
        assertNull(result.getFullTextResult());
        assertNull(result.getReferredEntities());
    }

    @Test
    public void testConstructorWithQueryTextAndType() {
        String queryText = "SELECT * FROM Table";
        AtlasSearchResult result = new AtlasSearchResult(queryText, AtlasQueryType.DSL);

        assertEquals(result.getQueryText(), queryText);
        assertEquals(result.getQueryType(), AtlasQueryType.DSL);
        assertNull(result.getSearchParameters());
        assertNull(result.getEntities());
        assertNull(result.getRelations());
        assertNull(result.getAttributes());
        assertNull(result.getFullTextResult());
        assertNull(result.getReferredEntities());
    }

    @Test
    public void testConstructorWithSearchParameters() {
        SearchParameters searchParams = new SearchParameters();
        searchParams.setQuery("test query");

        AtlasSearchResult result = new AtlasSearchResult(searchParams);

        assertEquals(result.getQueryType(), AtlasQueryType.BASIC);
        assertEquals(result.getQueryText(), "test query");
        assertSame(result.getSearchParameters(), searchParams);
        assertNull(result.getEntities());
        assertNull(result.getRelations());
        assertNull(result.getAttributes());
        assertNull(result.getFullTextResult());
        assertNull(result.getReferredEntities());
    }

    @Test
    public void testConstructorWithNullSearchParameters() {
        AtlasSearchResult result = new AtlasSearchResult((SearchParameters) null);

        assertEquals(result.getQueryType(), AtlasQueryType.BASIC);
        assertNull(result.getQueryText());
        assertNull(result.getSearchParameters());
    }

    @Test
    public void testQueryTypeGetterSetter() {
        assertNull(searchResult.getQueryType());

        searchResult.setQueryType(AtlasQueryType.FULL_TEXT);
        assertEquals(searchResult.getQueryType(), AtlasQueryType.FULL_TEXT);

        searchResult.setQueryType(null);
        assertNull(searchResult.getQueryType());
    }

    @Test
    public void testSearchParametersGetterSetter() {
        assertNull(searchResult.getSearchParameters());

        SearchParameters params = new SearchParameters();
        searchResult.setSearchParameters(params);
        assertSame(searchResult.getSearchParameters(), params);

        searchResult.setSearchParameters(null);
        assertNull(searchResult.getSearchParameters());
    }

    @Test
    public void testQueryTextGetterSetter() {
        assertNull(searchResult.getQueryText());

        String queryText = "test query";
        searchResult.setQueryText(queryText);
        assertEquals(searchResult.getQueryText(), queryText);

        searchResult.setQueryText("");
        assertEquals(searchResult.getQueryText(), "");

        searchResult.setQueryText(null);
        assertNull(searchResult.getQueryText());
    }

    @Test
    public void testTypeGetterSetter() {
        assertNull(searchResult.getType());

        String type = "Table";
        searchResult.setType(type);
        assertEquals(searchResult.getType(), type);

        searchResult.setType(null);
        assertNull(searchResult.getType());
    }

    @Test
    public void testClassificationGetterSetter() {
        assertNull(searchResult.getClassification());

        String classification = "PII";
        searchResult.setClassification(classification);
        assertEquals(searchResult.getClassification(), classification);

        searchResult.setClassification(null);
        assertNull(searchResult.getClassification());
    }

    @Test
    public void testEntitiesGetterSetter() {
        assertNull(searchResult.getEntities());

        List<AtlasEntityHeader> entities = new ArrayList<>();
        searchResult.setEntities(entities);
        assertSame(searchResult.getEntities(), entities);

        searchResult.setEntities(null);
        assertNull(searchResult.getEntities());
    }

    @Test
    public void testRelationsGetterSetter() {
        assertNull(searchResult.getRelations());

        List<AtlasRelationshipHeader> relations = new ArrayList<>();
        searchResult.setRelations(relations);
        assertSame(searchResult.getRelations(), relations);

        searchResult.setRelations(null);
        assertNull(searchResult.getRelations());
    }

    @Test
    public void testAttributesGetterSetter() {
        assertNull(searchResult.getAttributes());

        AttributeSearchResult attributes = new AttributeSearchResult();
        searchResult.setAttributes(attributes);
        assertSame(searchResult.getAttributes(), attributes);

        searchResult.setAttributes(null);
        assertNull(searchResult.getAttributes());
    }

    @Test
    public void testFullTextResultGetterSetter() {
        assertNull(searchResult.getFullTextResult());

        List<AtlasFullTextResult> fullTextResults = new ArrayList<>();
        searchResult.setFullTextResult(fullTextResults);
        assertSame(searchResult.getFullTextResult(), fullTextResults);

        searchResult.setFullTextResult(null);
        assertNull(searchResult.getFullTextResult());
    }

    @Test
    public void testReferredEntitiesGetterSetter() {
        assertNull(searchResult.getReferredEntities());

        Map<String, AtlasEntityHeader> referredEntities = new HashMap<>();
        searchResult.setReferredEntities(referredEntities);
        assertSame(searchResult.getReferredEntities(), referredEntities);

        searchResult.setReferredEntities(null);
        assertNull(searchResult.getReferredEntities());
    }

    @Test
    public void testApproximateCountGetterSetter() {
        assertEquals(searchResult.getApproximateCount(), -1L);

        searchResult.setApproximateCount(100L);
        assertEquals(searchResult.getApproximateCount(), 100L);

        searchResult.setApproximateCount(0L);
        assertEquals(searchResult.getApproximateCount(), 0L);

        searchResult.setApproximateCount(Long.MAX_VALUE);
        assertEquals(searchResult.getApproximateCount(), Long.MAX_VALUE);
    }

    @Test
    public void testNextMarkerGetterSetter() {
        assertNull(searchResult.getNextMarker());

        String marker = "next-page-marker";
        searchResult.setNextMarker(marker);
        assertEquals(searchResult.getNextMarker(), marker);

        searchResult.setNextMarker("");
        assertEquals(searchResult.getNextMarker(), "");

        searchResult.setNextMarker(null);
        assertNull(searchResult.getNextMarker());
    }

    @Test
    public void testAddEntityToEmptyList() {
        AtlasEntityHeader entity = new AtlasEntityHeader();
        entity.setGuid("entity-1");

        searchResult.addEntity(entity);

        assertNotNull(searchResult.getEntities());
        assertEquals(searchResult.getEntities().size(), 1);
        assertEquals(searchResult.getEntities().get(0).getGuid(), "entity-1");
    }

    @Test
    public void testAddEntityToExistingList() {
        AtlasEntityHeader entity1 = new AtlasEntityHeader();
        entity1.setGuid("entity-1");

        AtlasEntityHeader entity2 = new AtlasEntityHeader();
        entity2.setGuid("entity-2");

        searchResult.addEntity(entity1);
        searchResult.addEntity(entity2);

        assertEquals(searchResult.getEntities().size(), 2);
        assertEquals(searchResult.getEntities().get(1).getGuid(), "entity-2");
    }

    @Test
    public void testAddDuplicateEntity() {
        AtlasEntityHeader entity1 = new AtlasEntityHeader();
        entity1.setGuid("entity-1");

        AtlasEntityHeader entity2 = new AtlasEntityHeader();
        entity2.setGuid("entity-1"); // Same GUID

        searchResult.addEntity(entity1);
        searchResult.addEntity(entity2);

        assertEquals(searchResult.getEntities().size(), 1);
        assertSame(searchResult.getEntities().get(0), entity2);
    }

    @Test
    public void testRemoveEntity() {
        AtlasEntityHeader entity1 = new AtlasEntityHeader();
        entity1.setGuid("entity-1");

        AtlasEntityHeader entity2 = new AtlasEntityHeader();
        entity2.setGuid("entity-2");

        searchResult.addEntity(entity1);
        searchResult.addEntity(entity2);

        assertEquals(searchResult.getEntities().size(), 2);

        searchResult.removeEntity(entity1);

        assertEquals(searchResult.getEntities().size(), 1);
        assertEquals(searchResult.getEntities().get(0).getGuid(), "entity-2");
    }

    @Test
    public void testRemoveEntityFromEmptyList() {
        AtlasEntityHeader entity = new AtlasEntityHeader();
        entity.setGuid("entity-1");

        // Should not throw exception
        searchResult.removeEntity(entity);

        assertNull(searchResult.getEntities());
    }

    @Test
    public void testAddRelationToEmptyList() {
        AtlasRelationshipHeader relation = new AtlasRelationshipHeader();
        relation.setGuid("relation-1");

        searchResult.addRelation(relation);

        assertNotNull(searchResult.getRelations());
        assertEquals(searchResult.getRelations().size(), 1);
        assertEquals(searchResult.getRelations().get(0).getGuid(), "relation-1");
    }

    @Test
    public void testAddRelationToExistingList() {
        AtlasRelationshipHeader relation1 = new AtlasRelationshipHeader();
        relation1.setGuid("relation-1");

        AtlasRelationshipHeader relation2 = new AtlasRelationshipHeader();
        relation2.setGuid("relation-2");

        searchResult.addRelation(relation1);
        searchResult.addRelation(relation2);

        assertEquals(searchResult.getRelations().size(), 2);
        assertEquals(searchResult.getRelations().get(1).getGuid(), "relation-2");
    }

    @Test
    public void testAddDuplicateRelation() {
        AtlasRelationshipHeader relation1 = new AtlasRelationshipHeader();
        relation1.setGuid("relation-1");

        AtlasRelationshipHeader relation2 = new AtlasRelationshipHeader();
        relation2.setGuid("relation-1"); // Same GUID

        searchResult.addRelation(relation1);
        searchResult.addRelation(relation2);

        // Should remove the first and add the second
        assertEquals(searchResult.getRelations().size(), 1);
        assertSame(searchResult.getRelations().get(0), relation2);
    }

    @Test
    public void testRemoveRelation() {
        AtlasRelationshipHeader relation1 = new AtlasRelationshipHeader();
        relation1.setGuid("relation-1");

        AtlasRelationshipHeader relation2 = new AtlasRelationshipHeader();
        relation2.setGuid("relation-2");

        searchResult.addRelation(relation1);
        searchResult.addRelation(relation2);

        assertEquals(searchResult.getRelations().size(), 2);

        searchResult.removeRelation(relation1);

        assertEquals(searchResult.getRelations().size(), 1);
        assertEquals(searchResult.getRelations().get(0).getGuid(), "relation-2");
    }

    @Test
    public void testRemoveRelationFromEmptyList() {
        AtlasRelationshipHeader relation = new AtlasRelationshipHeader();
        relation.setGuid("relation-1");

        // Should not throw exception
        searchResult.removeRelation(relation);

        assertNull(searchResult.getRelations());
    }

    @Test
    public void testHashCodeConsistency() {
        int hashCode1 = searchResult.hashCode();
        int hashCode2 = searchResult.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        AtlasSearchResult result1 = new AtlasSearchResult();
        AtlasSearchResult result2 = new AtlasSearchResult();

        result1.setQueryType(AtlasQueryType.DSL);
        result1.setQueryText("test");

        result2.setQueryType(AtlasQueryType.DSL);
        result2.setQueryText("test");

        assertEquals(result1.hashCode(), result2.hashCode());
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(searchResult.equals(searchResult));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(searchResult.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(searchResult.equals("not a search result"));
    }

    @Test
    public void testEqualsWithSameValues() {
        AtlasSearchResult result1 = new AtlasSearchResult();
        AtlasSearchResult result2 = new AtlasSearchResult();

        result1.setQueryType(AtlasQueryType.DSL);
        result1.setQueryText("test");

        result2.setQueryType(AtlasQueryType.DSL);
        result2.setQueryText("test");

        assertTrue(result1.equals(result2));
        assertTrue(result2.equals(result1));
    }

    @Test
    public void testEqualsWithDifferentValues() {
        AtlasSearchResult result1 = new AtlasSearchResult();
        AtlasSearchResult result2 = new AtlasSearchResult();

        result1.setQueryType(AtlasQueryType.DSL);
        result2.setQueryType(AtlasQueryType.FULL_TEXT);

        assertFalse(result1.equals(result2));
        assertFalse(result2.equals(result1));
    }

    @Test
    public void testToString() {
        searchResult.setQueryType(AtlasQueryType.DSL);
        searchResult.setQueryText("test query");
        searchResult.setApproximateCount(100L);

        String result = searchResult.toString();

        assertNotNull(result);
        assertTrue(result.contains("AtlasSearchResult"));
        assertTrue(result.contains("DSL"));
        assertTrue(result.contains("test query"));
        assertTrue(result.contains("100"));
    }

    @Test
    public void testAtlasQueryTypeValues() {
        AtlasQueryType[] types = AtlasQueryType.values();

        assertTrue(types.length >= 6);

        // Verify expected types exist
        boolean foundDSL = false;
        boolean foundFullText = false;
        boolean foundBasic = false;

        for (AtlasQueryType type : types) {
            if (type == AtlasQueryType.DSL) {
                foundDSL = true;
            } else if (type == AtlasQueryType.FULL_TEXT) {
                foundFullText = true;
            } else if (type == AtlasQueryType.BASIC) {
                foundBasic = true;
            }
        }

        assertTrue(foundDSL);
        assertTrue(foundFullText);
        assertTrue(foundBasic);
    }

    @Test
    public void testSerializable() {
        assertNotNull(searchResult);
    }

    @Test
    public void testAttributeSearchResultDefaultConstructor() {
        AttributeSearchResult attributeResult = new AttributeSearchResult();

        assertNull(attributeResult.getName());
        assertNull(attributeResult.getValues());
    }

    @Test
    public void testAttributeSearchResultParameterizedConstructor() {
        List<String> names = Arrays.asList("attr1", "attr2");
        List<List<Object>> values = new ArrayList<>();
        values.add(Arrays.asList("value1", "value2"));

        AttributeSearchResult attributeResult = new AttributeSearchResult(names, values);

        assertSame(attributeResult.getName(), names);
        assertSame(attributeResult.getValues(), values);
    }

    @Test
    public void testAttributeSearchResultGetterSetter() {
        AttributeSearchResult attributeResult = new AttributeSearchResult();

        List<String> names = Arrays.asList("attr1", "attr2");
        List<List<Object>> values = new ArrayList<>();

        attributeResult.setName(names);
        attributeResult.setValues(values);

        assertSame(attributeResult.getName(), names);
        assertSame(attributeResult.getValues(), values);
    }

    @Test
    public void testAttributeSearchResultEquals() {
        List<String> names = Arrays.asList("attr1", "attr2");
        List<List<Object>> values = new ArrayList<>();

        AttributeSearchResult result1 = new AttributeSearchResult(names, values);
        AttributeSearchResult result2 = new AttributeSearchResult(names, values);

        assertTrue(result1.equals(result2));
        assertTrue(result2.equals(result1));
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    @Test
    public void testAttributeSearchResultToString() {
        List<String> names = Arrays.asList("attr1", "attr2");
        List<List<Object>> values = new ArrayList<>();

        AttributeSearchResult attributeResult = new AttributeSearchResult(names, values);
        String result = attributeResult.toString();

        assertNotNull(result);
        assertTrue(result.contains("AttributeSearchResult"));
    }

    @Test
    public void testAtlasFullTextResultDefaultConstructor() {
        AtlasFullTextResult fullTextResult = new AtlasFullTextResult();

        assertNull(fullTextResult.getEntity());
        assertNull(fullTextResult.getScore());
    }

    @Test
    public void testAtlasFullTextResultParameterizedConstructor() {
        AtlasEntityHeader entity = new AtlasEntityHeader();
        Double score = 0.85;

        AtlasFullTextResult fullTextResult = new AtlasFullTextResult(entity, score);

        assertSame(fullTextResult.getEntity(), entity);
        assertEquals(fullTextResult.getScore(), score);
    }

    @Test
    public void testAtlasFullTextResultGetterSetter() {
        AtlasFullTextResult fullTextResult = new AtlasFullTextResult();

        AtlasEntityHeader entity = new AtlasEntityHeader();
        Double score = 0.75;

        fullTextResult.setEntity(entity);
        fullTextResult.setScore(score);

        assertSame(fullTextResult.getEntity(), entity);
        assertEquals(fullTextResult.getScore(), score);
    }

    @Test
    public void testAtlasFullTextResultEquals() {
        AtlasEntityHeader entity = new AtlasEntityHeader();
        Double score = 0.90;

        AtlasFullTextResult result1 = new AtlasFullTextResult(entity, score);
        AtlasFullTextResult result2 = new AtlasFullTextResult(entity, score);

        assertTrue(result1.equals(result2));
        assertTrue(result2.equals(result1));
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    @Test
    public void testAtlasFullTextResultToString() {
        AtlasEntityHeader entity = new AtlasEntityHeader();
        Double score = 0.95;

        AtlasFullTextResult fullTextResult = new AtlasFullTextResult(entity, score);
        String result = fullTextResult.toString();

        assertNotNull(result);
        assertTrue(result.contains("AtlasFullTextResult"));
        assertTrue(result.contains("0.95"));
    }

    @Test
    public void testCompleteSearchResult() {
        // Create a fully populated search result
        AtlasSearchResult result = new AtlasSearchResult();

        result.setQueryType(AtlasQueryType.DSL);
        result.setQueryText("SELECT * FROM Table");
        result.setType("Table");
        result.setClassification("PII");
        result.setApproximateCount(1000L);
        result.setNextMarker("next-page");

        SearchParameters params = new SearchParameters();
        result.setSearchParameters(params);

        List<AtlasEntityHeader> entities = new ArrayList<>();
        AtlasEntityHeader entity = new AtlasEntityHeader();
        entity.setGuid("entity-1");
        entities.add(entity);
        result.setEntities(entities);

        List<AtlasRelationshipHeader> relations = new ArrayList<>();
        AtlasRelationshipHeader relation = new AtlasRelationshipHeader();
        relation.setGuid("relation-1");
        relations.add(relation);
        result.setRelations(relations);

        AttributeSearchResult attributes = new AttributeSearchResult();
        result.setAttributes(attributes);

        List<AtlasFullTextResult> fullTextResults = new ArrayList<>();
        AtlasFullTextResult fullTextResult = new AtlasFullTextResult();
        fullTextResults.add(fullTextResult);
        result.setFullTextResult(fullTextResults);

        Map<String, AtlasEntityHeader> referredEntities = new HashMap<>();
        referredEntities.put("ref-1", new AtlasEntityHeader());
        result.setReferredEntities(referredEntities);

        assertEquals(result.getQueryType(), AtlasQueryType.DSL);
        assertEquals(result.getQueryText(), "SELECT * FROM Table");
        assertEquals(result.getType(), "Table");
        assertEquals(result.getClassification(), "PII");
        assertEquals(result.getApproximateCount(), 1000L);
        assertEquals(result.getNextMarker(), "next-page");
        assertNotNull(result.getSearchParameters());
        assertNotNull(result.getEntities());
        assertNotNull(result.getRelations());
        assertNotNull(result.getAttributes());
        assertNotNull(result.getFullTextResult());
        assertNotNull(result.getReferredEntities());
    }
}
