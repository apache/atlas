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
package org.apache.atlas.model.typedef;

import org.apache.atlas.model.ModelTestUtil;
import org.apache.atlas.type.AtlasType;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasTypesDef {
    @Test
    public void testDefaultConstructor() {
        AtlasTypesDef typesDef = new AtlasTypesDef();

        assertNotNull(typesDef.getEnumDefs());
        assertNotNull(typesDef.getStructDefs());
        assertNotNull(typesDef.getClassificationDefs());
        assertNotNull(typesDef.getEntityDefs());
        assertNotNull(typesDef.getRelationshipDefs());
        assertNotNull(typesDef.getBusinessMetadataDefs());

        assertEquals(typesDef.getEnumDefs().size(), 0);
        assertEquals(typesDef.getStructDefs().size(), 0);
        assertEquals(typesDef.getClassificationDefs().size(), 0);
        assertEquals(typesDef.getEntityDefs().size(), 0);
        assertEquals(typesDef.getRelationshipDefs().size(), 0);
        assertEquals(typesDef.getBusinessMetadataDefs().size(), 0);

        assertTrue(typesDef.isEmpty());
    }

    @Test
    public void testConstructorWithoutRelationshipsAndBusinessMetadata() {
        List<AtlasEnumDef> enumDefs = Arrays.asList(ModelTestUtil.newEnumDef());
        List<AtlasStructDef> structDefs = Arrays.asList(ModelTestUtil.newStructDef());
        List<AtlasClassificationDef> classificationDefs = Arrays.asList(ModelTestUtil.newClassificationDef());
        List<AtlasEntityDef> entityDefs = Arrays.asList(ModelTestUtil.newEntityDef());

        AtlasTypesDef typesDef = new AtlasTypesDef(enumDefs, structDefs, classificationDefs, entityDefs);

        assertEquals(typesDef.getEnumDefs(), enumDefs);
        assertEquals(typesDef.getStructDefs(), structDefs);
        assertEquals(typesDef.getClassificationDefs(), classificationDefs);
        assertEquals(typesDef.getEntityDefs(), entityDefs);
        assertNotNull(typesDef.getRelationshipDefs());
        assertNotNull(typesDef.getBusinessMetadataDefs());
        assertEquals(typesDef.getRelationshipDefs().size(), 0);
        assertEquals(typesDef.getBusinessMetadataDefs().size(), 0);

        assertFalse(typesDef.isEmpty());
    }

    @Test
    public void testConstructorWithRelationships() {
        List<AtlasEnumDef> enumDefs = Arrays.asList(ModelTestUtil.newEnumDef());
        List<AtlasStructDef> structDefs = Arrays.asList(ModelTestUtil.newStructDef());
        List<AtlasClassificationDef> classificationDefs = Arrays.asList(ModelTestUtil.newClassificationDef());
        List<AtlasEntityDef> entityDefs = Arrays.asList(ModelTestUtil.newEntityDef());
        List<AtlasRelationshipDef> relationshipDefs = Arrays.asList(new AtlasRelationshipDef("testRel", "description", "1.0",
                AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.NONE,
                new AtlasRelationshipEndDef("type1", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                new AtlasRelationshipEndDef("type2", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE)));

        AtlasTypesDef typesDef = new AtlasTypesDef(enumDefs, structDefs, classificationDefs, entityDefs, relationshipDefs);

        assertEquals(typesDef.getEnumDefs(), enumDefs);
        assertEquals(typesDef.getStructDefs(), structDefs);
        assertEquals(typesDef.getClassificationDefs(), classificationDefs);
        assertEquals(typesDef.getEntityDefs(), entityDefs);
        assertEquals(typesDef.getRelationshipDefs(), relationshipDefs);
        assertNotNull(typesDef.getBusinessMetadataDefs());
        assertEquals(typesDef.getBusinessMetadataDefs().size(), 0);

        assertFalse(typesDef.isEmpty());
    }

    @Test
    public void testConstructorWithAllTypeDefs() {
        List<AtlasEnumDef> enumDefs = Arrays.asList(ModelTestUtil.newEnumDef());
        List<AtlasStructDef> structDefs = Arrays.asList(ModelTestUtil.newStructDef());
        List<AtlasClassificationDef> classificationDefs = Arrays.asList(ModelTestUtil.newClassificationDef());
        List<AtlasEntityDef> entityDefs = Arrays.asList(ModelTestUtil.newEntityDef());
        List<AtlasRelationshipDef> relationshipDefs = Arrays.asList(new AtlasRelationshipDef("testRel", "description", "1.0",
                AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.NONE,
                new AtlasRelationshipEndDef("type1", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                new AtlasRelationshipEndDef("type2", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE)));
        List<AtlasBusinessMetadataDef> businessMetadataDefs = Arrays.asList(new AtlasBusinessMetadataDef("testBizMeta", "description"));

        AtlasTypesDef typesDef = new AtlasTypesDef(enumDefs, structDefs, classificationDefs, entityDefs, relationshipDefs, businessMetadataDefs);

        assertEquals(typesDef.getEnumDefs(), enumDefs);
        assertEquals(typesDef.getStructDefs(), structDefs);
        assertEquals(typesDef.getClassificationDefs(), classificationDefs);
        assertEquals(typesDef.getEntityDefs(), entityDefs);
        assertEquals(typesDef.getRelationshipDefs(), relationshipDefs);
        assertEquals(typesDef.getBusinessMetadataDefs(), businessMetadataDefs);

        assertFalse(typesDef.isEmpty());
    }

    @Test
    public void testSettersAndGetters() {
        AtlasTypesDef typesDef = new AtlasTypesDef();

        List<AtlasEnumDef> enumDefs = Arrays.asList(ModelTestUtil.newEnumDef());
        typesDef.setEnumDefs(enumDefs);
        assertEquals(typesDef.getEnumDefs(), enumDefs);

        List<AtlasStructDef> structDefs = Arrays.asList(ModelTestUtil.newStructDef());
        typesDef.setStructDefs(structDefs);
        assertEquals(typesDef.getStructDefs(), structDefs);

        List<AtlasClassificationDef> classificationDefs = Arrays.asList(ModelTestUtil.newClassificationDef());
        typesDef.setClassificationDefs(classificationDefs);
        assertEquals(typesDef.getClassificationDefs(), classificationDefs);

        List<AtlasEntityDef> entityDefs = Arrays.asList(ModelTestUtil.newEntityDef());
        typesDef.setEntityDefs(entityDefs);
        assertEquals(typesDef.getEntityDefs(), entityDefs);

        List<AtlasRelationshipDef> relationshipDefs = Arrays.asList(new AtlasRelationshipDef("testRel", "description", "1.0",
                AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.NONE,
                new AtlasRelationshipEndDef("type1", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                new AtlasRelationshipEndDef("type2", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE)));
        typesDef.setRelationshipDefs(relationshipDefs);
        assertEquals(typesDef.getRelationshipDefs(), relationshipDefs);

        List<AtlasBusinessMetadataDef> businessMetadataDefs = Arrays.asList(new AtlasBusinessMetadataDef("testBizMeta", "description"));
        typesDef.setBusinessMetadataDefs(businessMetadataDefs);
        assertEquals(typesDef.getBusinessMetadataDefs(), businessMetadataDefs);

        assertFalse(typesDef.isEmpty());
    }

    @Test
    public void testHasTypeDefMethods() {
        AtlasEnumDef enumDef = ModelTestUtil.newEnumDef();
        AtlasStructDef structDef = ModelTestUtil.newStructDef();
        AtlasClassificationDef classificationDef = ModelTestUtil.newClassificationDef();
        AtlasEntityDef entityDef = ModelTestUtil.newEntityDef();
        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("testRel", "description", "1.0",
                AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.NONE,
                new AtlasRelationshipEndDef("type1", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                new AtlasRelationshipEndDef("type2", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE));
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef("testBizMeta", "description");

        AtlasTypesDef typesDef = new AtlasTypesDef(
                Arrays.asList(enumDef),
                Arrays.asList(structDef),
                Arrays.asList(classificationDef),
                Arrays.asList(entityDef),
                Arrays.asList(relationshipDef),
                Arrays.asList(businessMetadataDef));

        assertTrue(typesDef.hasEnumDef(enumDef.getName()));
        assertTrue(typesDef.hasStructDef(structDef.getName()));
        assertTrue(typesDef.hasClassificationDef(classificationDef.getName()));
        assertTrue(typesDef.hasEntityDef(entityDef.getName()));
        assertTrue(typesDef.hasRelationshipDef(relationshipDef.getName()));
        assertTrue(typesDef.hasBusinessMetadataDef(businessMetadataDef.getName()));

        assertFalse(typesDef.hasEnumDef("nonExistentEnum"));
        assertFalse(typesDef.hasStructDef("nonExistentStruct"));
        assertFalse(typesDef.hasClassificationDef("nonExistentClassification"));
        assertFalse(typesDef.hasEntityDef("nonExistentEntity"));
        assertFalse(typesDef.hasRelationshipDef("nonExistentRelationship"));
        assertFalse(typesDef.hasBusinessMetadataDef("nonExistentBusinessMetadata"));
    }

    @Test
    public void testHasTypeDefWithEmptyLists() {
        AtlasTypesDef typesDef = new AtlasTypesDef();

        assertFalse(typesDef.hasEnumDef("anyName"));
        assertFalse(typesDef.hasStructDef("anyName"));
        assertFalse(typesDef.hasClassificationDef("anyName"));
        assertFalse(typesDef.hasEntityDef("anyName"));
        assertFalse(typesDef.hasRelationshipDef("anyName"));
        assertFalse(typesDef.hasBusinessMetadataDef("anyName"));
    }

    @Test
    public void testIsEmpty() {
        AtlasTypesDef emptyTypesDef = new AtlasTypesDef();
        assertTrue(emptyTypesDef.isEmpty());

        // Test with each type of def
        AtlasTypesDef typesDefWithEnum = new AtlasTypesDef();
        typesDefWithEnum.setEnumDefs(Arrays.asList(ModelTestUtil.newEnumDef()));
        assertFalse(typesDefWithEnum.isEmpty());

        AtlasTypesDef typesDefWithStruct = new AtlasTypesDef();
        typesDefWithStruct.setStructDefs(Arrays.asList(ModelTestUtil.newStructDef()));
        assertFalse(typesDefWithStruct.isEmpty());

        AtlasTypesDef typesDefWithClassification = new AtlasTypesDef();
        typesDefWithClassification.setClassificationDefs(Arrays.asList(ModelTestUtil.newClassificationDef()));
        assertFalse(typesDefWithClassification.isEmpty());

        AtlasTypesDef typesDefWithEntity = new AtlasTypesDef();
        typesDefWithEntity.setEntityDefs(Arrays.asList(ModelTestUtil.newEntityDef()));
        assertFalse(typesDefWithEntity.isEmpty());

        AtlasTypesDef typesDefWithRelationship = new AtlasTypesDef();
        typesDefWithRelationship.setRelationshipDefs(Arrays.asList(new AtlasRelationshipDef("testRel", "description", "1.0",
                AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.NONE,
                new AtlasRelationshipEndDef("type1", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                new AtlasRelationshipEndDef("type2", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE))));
        assertFalse(typesDefWithRelationship.isEmpty());

        AtlasTypesDef typesDefWithBusinessMetadata = new AtlasTypesDef();
        typesDefWithBusinessMetadata.setBusinessMetadataDefs(Arrays.asList(new AtlasBusinessMetadataDef("testBizMeta", "description")));
        assertFalse(typesDefWithBusinessMetadata.isEmpty());
    }

    @Test
    public void testClearWithNullLists() {
        AtlasTypesDef typesDef = new AtlasTypesDef();

        // Set lists to null
        typesDef.setEnumDefs(null);
        typesDef.setStructDefs(null);
        typesDef.setClassificationDefs(null);
        typesDef.setEntityDefs(null);
        typesDef.setRelationshipDefs(null);
        typesDef.setBusinessMetadataDefs(null);

        // Clear should not throw exception
        typesDef.clear();
    }

    @Test
    public void testToString() {
        AtlasEnumDef enumDef = ModelTestUtil.newEnumDef();
        AtlasStructDef structDef = ModelTestUtil.newStructDef();
        AtlasClassificationDef classificationDef = ModelTestUtil.newClassificationDef();
        AtlasEntityDef entityDef = ModelTestUtil.newEntityDef();
        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("testRel", "description", "1.0",
                AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.NONE,
                new AtlasRelationshipEndDef("type1", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                new AtlasRelationshipEndDef("type2", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE));
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef("testBizMeta", "description");

        AtlasTypesDef typesDef = new AtlasTypesDef(
                Arrays.asList(enumDef),
                Arrays.asList(structDef),
                Arrays.asList(classificationDef),
                Arrays.asList(entityDef),
                Arrays.asList(relationshipDef),
                Arrays.asList(businessMetadataDef));

        String toStringResult = typesDef.toString();
        assertNotNull(toStringResult);
        assertTrue(toStringResult.contains("AtlasTypesDef"));
        assertTrue(toStringResult.contains("enumDefs"));
        assertTrue(toStringResult.contains("structDefs"));
        assertTrue(toStringResult.contains("classificationDefs"));
        assertTrue(toStringResult.contains("entityDefs"));
        assertTrue(toStringResult.contains("relationshipDefs"));
        assertTrue(toStringResult.contains("businessMetadataDefs"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        AtlasTypesDef typesDef = new AtlasTypesDef();

        StringBuilder sb = new StringBuilder();
        StringBuilder result = typesDef.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("AtlasTypesDef"));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        AtlasTypesDef typesDef = new AtlasTypesDef();

        StringBuilder result = typesDef.toString(null);
        assertNotNull(result);
        assertTrue(result.toString().contains("AtlasTypesDef"));
    }

    @Test
    public void testSerializationDeserialization() {
        AtlasEnumDef enumDef = ModelTestUtil.newEnumDef();
        AtlasStructDef structDef = ModelTestUtil.newStructDef();
        AtlasClassificationDef classificationDef = ModelTestUtil.newClassificationDef();
        AtlasEntityDef entityDef = ModelTestUtil.newEntityDef();
        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("testRel", "description", "1.0",
                AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.NONE,
                new AtlasRelationshipEndDef("type1", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                new AtlasRelationshipEndDef("type2", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE));
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef("testBizMeta", "description");

        AtlasTypesDef original = new AtlasTypesDef(
                Arrays.asList(enumDef),
                Arrays.asList(structDef),
                Arrays.asList(classificationDef),
                Arrays.asList(entityDef),
                Arrays.asList(relationshipDef),
                Arrays.asList(businessMetadataDef));

        String jsonString = AtlasType.toJson(original);
        AtlasTypesDef deserialized = AtlasType.fromJson(jsonString, AtlasTypesDef.class);

        assertNotNull(deserialized);
        assertEquals(deserialized.getEnumDefs().size(), original.getEnumDefs().size());
        assertEquals(deserialized.getStructDefs().size(), original.getStructDefs().size());
        assertEquals(deserialized.getClassificationDefs().size(), original.getClassificationDefs().size());
        assertEquals(deserialized.getEntityDefs().size(), original.getEntityDefs().size());
        assertEquals(deserialized.getRelationshipDefs().size(), original.getRelationshipDefs().size());
        assertEquals(deserialized.getBusinessMetadataDefs().size(), original.getBusinessMetadataDefs().size());

        assertTrue(deserialized.hasEnumDef(enumDef.getName()));
        assertTrue(deserialized.hasStructDef(structDef.getName()));
        assertTrue(deserialized.hasClassificationDef(classificationDef.getName()));
        assertTrue(deserialized.hasEntityDef(entityDef.getName()));
        assertTrue(deserialized.hasRelationshipDef(relationshipDef.getName()));
        assertTrue(deserialized.hasBusinessMetadataDef(businessMetadataDef.getName()));
    }

    @Test
    public void testHasTypeDefWithMultipleDefinitions() {
        AtlasEnumDef enumDef1 = ModelTestUtil.newEnumDef();
        AtlasEnumDef enumDef2 = ModelTestUtil.newEnumDef();

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEnumDefs(Arrays.asList(enumDef1, enumDef2));

        assertTrue(typesDef.hasEnumDef(enumDef1.getName()));
        assertTrue(typesDef.hasEnumDef(enumDef2.getName()));
        assertFalse(typesDef.hasEnumDef("nonExistent"));
    }

    @Test
    public void testConstructorOverloads() {
        // Test 4-parameter constructor
        AtlasTypesDef typesDef4 = new AtlasTypesDef(
                Arrays.asList(ModelTestUtil.newEnumDef()),
                Arrays.asList(ModelTestUtil.newStructDef()),
                Arrays.asList(ModelTestUtil.newClassificationDef()),
                Arrays.asList(ModelTestUtil.newEntityDef()));

        assertEquals(typesDef4.getEnumDefs().size(), 1);
        assertEquals(typesDef4.getStructDefs().size(), 1);
        assertEquals(typesDef4.getClassificationDefs().size(), 1);
        assertEquals(typesDef4.getEntityDefs().size(), 1);
        assertEquals(typesDef4.getRelationshipDefs().size(), 0);
        assertEquals(typesDef4.getBusinessMetadataDefs().size(), 0);

        // Test 5-parameter constructor
        AtlasTypesDef typesDef5 = new AtlasTypesDef(
                Arrays.asList(ModelTestUtil.newEnumDef()),
                Arrays.asList(ModelTestUtil.newStructDef()),
                Arrays.asList(ModelTestUtil.newClassificationDef()),
                Arrays.asList(ModelTestUtil.newEntityDef()),
                Arrays.asList(new AtlasRelationshipDef("testRel", "description", "1.0",
                        AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.NONE,
                        new AtlasRelationshipEndDef("type1", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("type2", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE))));

        assertEquals(typesDef5.getEnumDefs().size(), 1);
        assertEquals(typesDef5.getStructDefs().size(), 1);
        assertEquals(typesDef5.getClassificationDefs().size(), 1);
        assertEquals(typesDef5.getEntityDefs().size(), 1);
        assertEquals(typesDef5.getRelationshipDefs().size(), 1);
        assertEquals(typesDef5.getBusinessMetadataDefs().size(), 0);
    }

    @Test
    public void testIsEmptyWithNullLists() {
        AtlasTypesDef typesDef = new AtlasTypesDef();

        // Set some lists to null
        typesDef.setEnumDefs(null);
        typesDef.setStructDefs(null);

        // Should still be considered empty
        assertTrue(typesDef.isEmpty());
    }

    @Test
    public void testComplexScenario() {
        // Create a comprehensive AtlasTypesDef with all types
        AtlasEnumDef statusEnum = new AtlasEnumDef("Status", "Status enumeration", "1.0");
        statusEnum.addElement(new AtlasEnumDef.AtlasEnumElementDef("ACTIVE", "Active status", 1));
        statusEnum.addElement(new AtlasEnumDef.AtlasEnumElementDef("INACTIVE", "Inactive status", 2));

        AtlasStructDef addressStruct = new AtlasStructDef("Address", "Address structure", "1.0");
        addressStruct.addAttribute(new AtlasStructDef.AtlasAttributeDef("street", "string"));
        addressStruct.addAttribute(new AtlasStructDef.AtlasAttributeDef("city", "string"));

        AtlasClassificationDef piiClassification = new AtlasClassificationDef("PII", "Personally Identifiable Information", "1.0");
        piiClassification.addAttribute(new AtlasStructDef.AtlasAttributeDef("level", "string"));

        AtlasEntityDef personEntity = new AtlasEntityDef("Person", "Person entity", "1.0");
        personEntity.addAttribute(new AtlasStructDef.AtlasAttributeDef("name", "string"));
        personEntity.addAttribute(new AtlasStructDef.AtlasAttributeDef("address", "Address"));

        AtlasRelationshipDef friendshipRel = new AtlasRelationshipDef("Friendship", "Friendship relationship", "1.0",
                AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.NONE,
                new AtlasRelationshipEndDef("Person", "friends", AtlasStructDef.AtlasAttributeDef.Cardinality.SET),
                new AtlasRelationshipEndDef("Person", "friendOf", AtlasStructDef.AtlasAttributeDef.Cardinality.SET));

        AtlasBusinessMetadataDef qualityBizMeta = new AtlasBusinessMetadataDef("Quality", "Data quality metadata", "1.0");
        qualityBizMeta.addAttribute(new AtlasStructDef.AtlasAttributeDef("score", "int"));

        AtlasTypesDef typesDef = new AtlasTypesDef(Arrays.asList(statusEnum),
                Arrays.asList(addressStruct),
                Arrays.asList(piiClassification),
                Arrays.asList(personEntity),
                Arrays.asList(friendshipRel),
                Arrays.asList(qualityBizMeta));
        // Verify all types are present
        assertTrue(typesDef.hasEnumDef("Status"));
        assertTrue(typesDef.hasStructDef("Address"));
        assertTrue(typesDef.hasClassificationDef("PII"));
        assertTrue(typesDef.hasEntityDef("Person"));
        assertTrue(typesDef.hasRelationshipDef("Friendship"));
        assertTrue(typesDef.hasBusinessMetadataDef("Quality"));

        assertFalse(typesDef.isEmpty());

        // Test serialization and deserialization
        String json = AtlasType.toJson(typesDef);
        AtlasTypesDef deserializedTypesDef = AtlasType.fromJson(json, AtlasTypesDef.class);

        assertTrue(deserializedTypesDef.hasEnumDef("Status"));
        assertTrue(deserializedTypesDef.hasStructDef("Address"));
        assertTrue(deserializedTypesDef.hasClassificationDef("PII"));
        assertTrue(deserializedTypesDef.hasEntityDef("Person"));
        assertTrue(deserializedTypesDef.hasRelationshipDef("Friendship"));
        assertTrue(deserializedTypesDef.hasBusinessMetadataDef("Quality"));
    }
}
