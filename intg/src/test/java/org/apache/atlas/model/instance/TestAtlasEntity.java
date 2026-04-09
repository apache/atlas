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
package org.apache.atlas.model.instance;

import org.apache.atlas.model.ModelTestUtil;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.instance.AtlasEntity.Status;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasEntity {
    @Test
    public void testEntitySerDe() {
        AtlasEntityDef    entityDef    = ModelTestUtil.getEntityDef();
        AtlasTypeRegistry typeRegistry = ModelTestUtil.getTypesRegistry();
        AtlasEntityType   entityType   = typeRegistry.getEntityTypeByName(entityDef.getName());
        assertNotNull(entityType);

        AtlasEntity ent1 = entityType.createDefaultValue();

        String jsonString = AtlasType.toJson(ent1);

        AtlasEntity ent2 = AtlasType.fromJson(jsonString, AtlasEntity.class);

        entityType.normalizeAttributeValues(ent2);

        assertEquals(ent2, ent1, "Incorrect serialization/deserialization of AtlasEntity");
    }

    @Test
    public void testEntitySerDeWithSuperType() {
        AtlasEntityDef    entityDef    = ModelTestUtil.getEntityDefWithSuperType();
        AtlasTypeRegistry typeRegistry = ModelTestUtil.getTypesRegistry();
        AtlasEntityType   entityType   = typeRegistry.getEntityTypeByName(entityDef.getName());

        assertNotNull(entityType);

        AtlasEntity ent1 = entityType.createDefaultValue();

        String jsonString = AtlasType.toJson(ent1);

        AtlasEntity ent2 = AtlasType.fromJson(jsonString, AtlasEntity.class);

        entityType.normalizeAttributeValues(ent2);

        assertEquals(ent2, ent1, "Incorrect serialization/deserialization of AtlasEntity with superType");
    }

    @Test
    public void testEntitySerDeWithSuperTypes() {
        AtlasEntityDef    entityDef    = ModelTestUtil.getEntityDefWithSuperTypes();
        AtlasTypeRegistry typeRegistry = ModelTestUtil.getTypesRegistry();
        AtlasEntityType   entityType   = typeRegistry.getEntityTypeByName(entityDef.getName());

        assertNotNull(entityType);

        AtlasEntity ent1 = entityType.createDefaultValue();

        String jsonString = AtlasType.toJson(ent1);

        AtlasEntity ent2 = AtlasType.fromJson(jsonString, AtlasEntity.class);

        entityType.normalizeAttributeValues(ent2);

        assertEquals(ent2, ent1, "Incorrect serialization/deserialization of AtlasEntity with superTypes");
    }

    @Test
    public void testConstructors() {
        // Test default constructor
        AtlasEntity entity = new AtlasEntity();
        assertNotNull(entity);
        assertNull(entity.getTypeName());
        assertNotNull(entity.getGuid()); // init() method generates a guid

        // Test constructor with typeName
        AtlasEntity entityWithType = new AtlasEntity("testType");
        assertEquals("testType", entityWithType.getTypeName());
        assertNotNull(entityWithType.getGuid());

        // Test constructor with AtlasEntityDef
        AtlasEntityDef entityDef = ModelTestUtil.getEntityDef();
        AtlasEntity entityFromDef = new AtlasEntity(entityDef);
        assertEquals(entityDef.getName(), entityFromDef.getTypeName());

        // Test constructor with typeName, attrName, attrValue
        AtlasEntity entityWithSingleAttr = new AtlasEntity("testType", "attr1", "value1");
        assertEquals("testType", entityWithSingleAttr.getTypeName());
        assertEquals("value1", entityWithSingleAttr.getAttribute("attr1"));

        // Test constructor with typeName and attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr1", "value1");
        AtlasEntity entityWithAttrs = new AtlasEntity("testType", attributes);
        assertEquals("testType", entityWithAttrs.getTypeName());
        assertEquals(attributes, entityWithAttrs.getAttributes());

        // Test constructor with AtlasEntityHeader
        AtlasEntityHeader header = new AtlasEntityHeader("testType");
        header.setGuid("testGuid");
        header.setStatus(Status.ACTIVE);
        AtlasEntity entityFromHeader = new AtlasEntity(header);
        assertEquals("testType", entityFromHeader.getTypeName());
        assertEquals("testGuid", entityFromHeader.getGuid());
        assertEquals(Status.ACTIVE, entityFromHeader.getStatus());
    }

    @Test
    public void testMapConstructor() {
        Map<String, Object> map = new HashMap<>();
        map.put(AtlasEntity.KEY_GUID, "testGuid");
        map.put(AtlasEntity.KEY_HOME_ID, "homeId123");
        map.put(AtlasEntity.KEY_IS_PROXY, true);
        map.put(AtlasEntity.KEY_IS_INCOMPLETE, false);
        map.put(AtlasEntity.KEY_PROVENANCE_TYPE, 1);
        map.put(AtlasEntity.KEY_STATUS, "ACTIVE");
        map.put(AtlasEntity.KEY_CREATED_BY, "testUser");
        map.put(AtlasEntity.KEY_UPDATED_BY, "testUser2");
        map.put(AtlasEntity.KEY_CREATE_TIME, System.currentTimeMillis());
        map.put(AtlasEntity.KEY_UPDATE_TIME, System.currentTimeMillis());
        map.put(AtlasEntity.KEY_VERSION, 1L);

        AtlasEntity entity = new AtlasEntity(map);
        assertEquals("testGuid", entity.getGuid());
        assertEquals("homeId123", entity.getHomeId());
        assertTrue(entity.isProxy());
        assertFalse(entity.getIsIncomplete());
        assertEquals(Integer.valueOf(1), entity.getProvenanceType());
        assertEquals(Status.ACTIVE, entity.getStatus());
        assertEquals("testUser", entity.getCreatedBy());
        assertEquals("testUser2", entity.getUpdatedBy());
        assertNotNull(entity.getCreateTime());
        assertNotNull(entity.getUpdateTime());
        assertEquals(Long.valueOf(1), entity.getVersion());
    }

    @Test
    public void testCopyConstructor() {
        AtlasEntity original = new AtlasEntity("testType");
        original.setGuid("testGuid");
        original.setHomeId("homeId123");
        original.setIsProxy(true);
        original.setIsIncomplete(false);
        original.setProvenanceType(1);
        original.setStatus(Status.ACTIVE);
        original.setCreatedBy("testUser");
        original.setUpdatedBy("testUser2");
        original.setCreateTime(new Date());
        original.setUpdateTime(new Date());
        original.setVersion(1L);

        // Set attributes to avoid NPE in copy constructor
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr1", "value1");
        original.setAttributes(attributes);

        Map<String, Object> relationshipAttrs = new HashMap<>();
        relationshipAttrs.put("rel1", "value1");
        original.setRelationshipAttributes(relationshipAttrs);

        Map<String, String> customAttrs = new HashMap<>();
        customAttrs.put("custom1", "customValue1");
        original.setCustomAttributes(customAttrs);

        Set<String> labels = new HashSet<>();
        labels.add("label1");
        original.setLabels(labels);

        AtlasEntity copy = new AtlasEntity(original);
        assertEquals(original.getGuid(), copy.getGuid());
        assertEquals(original.getHomeId(), copy.getHomeId());
        assertEquals(original.isProxy(), copy.isProxy());
        assertEquals(original.getIsIncomplete(), copy.getIsIncomplete());
        assertEquals(original.getProvenanceType(), copy.getProvenanceType());
        assertEquals(original.getStatus(), copy.getStatus());
        assertEquals(original.getCreatedBy(), copy.getCreatedBy());
        assertEquals(original.getUpdatedBy(), copy.getUpdatedBy());
        assertEquals(original.getCreateTime(), copy.getCreateTime());
        assertEquals(original.getUpdateTime(), copy.getUpdateTime());
        assertEquals(original.getVersion(), copy.getVersion());
        assertEquals(original.getRelationshipAttributes(), copy.getRelationshipAttributes());
        assertEquals(original.getCustomAttributes(), copy.getCustomAttributes());
        assertEquals(original.getLabels(), copy.getLabels());
    }

    @Test
    public void testGettersAndSetters() {
        AtlasEntity entity = new AtlasEntity();

        // Test guid
        entity.setGuid("testGuid");
        assertEquals("testGuid", entity.getGuid());

        // Test homeId
        entity.setHomeId("homeId123");
        assertEquals("homeId123", entity.getHomeId());

        // Test isProxy
        entity.setIsProxy(true);
        assertTrue(entity.isProxy());

        // Test isIncomplete
        entity.setIsIncomplete(true);
        assertTrue(entity.getIsIncomplete());

        // Test provenanceType
        entity.setProvenanceType(2);
        assertEquals(Integer.valueOf(2), entity.getProvenanceType());

        // Test status
        entity.setStatus(Status.DELETED);
        assertEquals(Status.DELETED, entity.getStatus());

        // Test createdBy and updatedBy
        entity.setCreatedBy("user1");
        entity.setUpdatedBy("user2");
        assertEquals("user1", entity.getCreatedBy());
        assertEquals("user2", entity.getUpdatedBy());

        // Test dates
        Date createTime = new Date();
        Date updateTime = new Date();
        entity.setCreateTime(createTime);
        entity.setUpdateTime(updateTime);
        assertEquals(createTime, entity.getCreateTime());
        assertEquals(updateTime, entity.getUpdateTime());

        // Test version
        entity.setVersion(5L);
        assertEquals(Long.valueOf(5), entity.getVersion());
    }

    @Test
    public void testRelationshipAttributes() {
        AtlasEntity entity = new AtlasEntity();

        // Test setting relationship attributes
        Map<String, Object> relationshipAttrs = new HashMap<>();
        relationshipAttrs.put("rel1", "value1");
        entity.setRelationshipAttributes(relationshipAttrs);
        assertEquals(relationshipAttrs, entity.getRelationshipAttributes());

        // Test setting individual relationship attribute
        entity.setRelationshipAttribute("rel2", "value2");
        assertEquals("value2", entity.getRelationshipAttribute("rel2"));
        assertTrue(entity.hasRelationshipAttribute("rel2"));
        assertFalse(entity.hasRelationshipAttribute("nonexistent"));

        // Test setting relationship attribute when map is null
        AtlasEntity entity2 = new AtlasEntity();
        entity2.setRelationshipAttribute("rel1", "value1");
        assertEquals("value1", entity2.getRelationshipAttribute("rel1"));
        assertTrue(entity2.hasRelationshipAttribute("rel1"));
    }

    @Test
    public void testCustomAttributes() {
        AtlasEntity entity = new AtlasEntity();

        Map<String, String> customAttrs = new HashMap<>();
        customAttrs.put("custom1", "value1");
        entity.setCustomAttributes(customAttrs);
        assertEquals(customAttrs, entity.getCustomAttributes());
    }

    @Test
    public void testBusinessAttributes() {
        AtlasEntity entity = new AtlasEntity();

        // Test setting business attributes
        Map<String, Map<String, Object>> businessAttrs = new HashMap<>();
        Map<String, Object> nsAttrs = new HashMap<>();
        nsAttrs.put("attr1", "value1");
        businessAttrs.put("namespace1", nsAttrs);
        entity.setBusinessAttributes(businessAttrs);
        assertEquals(businessAttrs, entity.getBusinessAttributes());

        // Test setting individual business attribute
        entity.setBusinessAttribute("namespace2", "attr2", "value2");
        assertEquals("value2", entity.getBusinessAttribute("namespace2", "attr2"));

        // Test setting business attribute when map is null
        AtlasEntity entity2 = new AtlasEntity();
        entity2.setBusinessAttribute("namespace1", "attr1", "value1");
        assertEquals("value1", entity2.getBusinessAttribute("namespace1", "attr1"));
    }

    @Test
    public void testLabels() {
        AtlasEntity entity = new AtlasEntity();

        Set<String> labels = new HashSet<>();
        labels.add("label1");
        labels.add("label2");
        entity.setLabels(labels);
        assertEquals(labels, entity.getLabels());
    }

    @Test
    public void testPendingTasks() {
        AtlasEntity entity = new AtlasEntity();

        Set<String> pendingTasks = new HashSet<>();
        pendingTasks.add("task1");
        pendingTasks.add("task2");
        entity.setPendingTasks(pendingTasks);
        assertEquals(pendingTasks, entity.getPendingTasks());
    }

    @Test
    public void testClassifications() {
        AtlasEntity entity = new AtlasEntity();

        // Test setting classifications
        List<AtlasClassification> classifications = new ArrayList<>();
        classifications.add(new AtlasClassification("classification1"));
        classifications.add(new AtlasClassification("classification2"));
        entity.setClassifications(classifications);
        assertEquals(classifications, entity.getClassifications());

        // Test adding classifications
        List<AtlasClassification> additionalClassifications = new ArrayList<>();
        additionalClassifications.add(new AtlasClassification("classification3"));
        entity.addClassifications(additionalClassifications);
        assertEquals(3, entity.getClassifications().size());

        // Test adding classifications when original list is null
        AtlasEntity entity2 = new AtlasEntity();
        entity2.addClassifications(additionalClassifications);
        assertEquals(1, entity2.getClassifications().size());
    }

    @Test
    public void testMeanings() {
        AtlasEntity entity = new AtlasEntity();

        // Test setting meanings
        List<AtlasTermAssignmentHeader> meanings = new ArrayList<>();
        AtlasTermAssignmentHeader meaning1 = new AtlasTermAssignmentHeader();
        meaning1.setTermGuid("term1");
        meanings.add(meaning1);
        entity.setMeanings(meanings);
        assertEquals(meanings, entity.getMeanings());

        // Test adding meaning
        AtlasTermAssignmentHeader meaning2 = new AtlasTermAssignmentHeader();
        meaning2.setTermGuid("term2");
        entity.addMeaning(meaning2);
        assertEquals(2, entity.getMeanings().size());

        // Test adding meaning when list is null
        AtlasEntity entity2 = new AtlasEntity();
        entity2.addMeaning(meaning2);
        assertEquals(1, entity2.getMeanings().size());
    }

    @Test
    public void testToString() {
        AtlasEntity entity = new AtlasEntity("testType");
        entity.setGuid("testGuid");
        entity.setStatus(Status.ACTIVE);

        String toString = entity.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasEntity"));
        assertTrue(toString.contains("testGuid"));
        assertTrue(toString.contains("ACTIVE"));
    }

    @Test
    public void testEquals() {
        AtlasEntity entity1 = new AtlasEntity("testType");
        entity1.setGuid("guid1");
        entity1.setStatus(Status.ACTIVE);

        AtlasEntity entity2 = new AtlasEntity("testType");
        entity2.setGuid("guid1");
        entity2.setStatus(Status.ACTIVE);

        // Test equals
        assertEquals(entity1, entity2);
        assertEquals(entity1.hashCode(), entity2.hashCode());

        // Test self equality
        assertEquals(entity1, entity1);

        // Test null equality
        assertNotEquals(entity1, null);

        // Test different class equality
        assertNotEquals(entity1, "string");

        // Test different guid
        entity2.setGuid("guid2");
        assertNotEquals(entity1, entity2);
    }

    @Test
    public void testPrivateNextInternalId() throws Exception {
        // Test private method nextInternalId using reflection
        Method nextInternalIdMethod = AtlasEntity.class.getDeclaredMethod("nextInternalId");
        nextInternalIdMethod.setAccessible(true);

        String id1 = (String) nextInternalIdMethod.invoke(null);
        String id2 = (String) nextInternalIdMethod.invoke(null);

        assertNotNull(id1);
        assertNotNull(id2);
        assertNotEquals(id1, id2);
        assertTrue(id1.startsWith("-"));
        assertTrue(id2.startsWith("-"));
    }

    @Test
    public void testAtlasEntityExtInfo() {
        // Test default constructor
        AtlasEntity.AtlasEntityExtInfo extInfo = new AtlasEntity.AtlasEntityExtInfo();
        assertNotNull(extInfo);
        assertNull(extInfo.getReferredEntities());

        // Test constructor with referred entity
        AtlasEntity referredEntity = new AtlasEntity("testType");
        referredEntity.setGuid("referredGuid");
        AtlasEntity.AtlasEntityExtInfo extInfoWithEntity = new AtlasEntity.AtlasEntityExtInfo(referredEntity);
        assertNotNull(extInfoWithEntity.getReferredEntities());
        assertEquals(referredEntity, extInfoWithEntity.getReferredEntity("referredGuid"));

        // Test constructor with map
        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        referredEntities.put("guid1", referredEntity);
        AtlasEntity.AtlasEntityExtInfo extInfoWithMap = new AtlasEntity.AtlasEntityExtInfo(referredEntities);
        assertEquals(referredEntities, extInfoWithMap.getReferredEntities());

        // Test copy constructor
        AtlasEntity.AtlasEntityExtInfo extInfoCopy = new AtlasEntity.AtlasEntityExtInfo(extInfoWithMap);
        assertEquals(referredEntities, extInfoCopy.getReferredEntities());

        // Test adding referred entity
        AtlasEntity newEntity = new AtlasEntity("newType");
        newEntity.setGuid("newGuid");
        extInfo.addReferredEntity(newEntity);
        assertEquals(newEntity, extInfo.getReferredEntity("newGuid"));

        // Test adding referred entity with specific guid
        AtlasEntity anotherEntity = new AtlasEntity("anotherType");
        extInfo.addReferredEntity("specificGuid", anotherEntity);
        assertEquals(anotherEntity, extInfo.getReferredEntity("specificGuid"));

        // Test removing referred entity
        AtlasEntity removed = extInfo.removeReferredEntity("specificGuid");
        assertEquals(anotherEntity, removed);
        assertNull(extInfo.getReferredEntity("specificGuid"));

        // Test updating entity guid
        extInfo.addReferredEntity("oldGuid", newEntity);
        extInfo.updateEntityGuid("oldGuid", "newGuid2");
        assertEquals("newGuid2", extInfo.getEntity("newGuid2").getGuid());

        // Test hasEntity
        assertTrue(extInfo.hasEntity("newGuid2"));
        assertFalse(extInfo.hasEntity("nonexistent"));

        // Test toString
        String toString = extInfo.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasEntityExtInfo"));

        // Test equals and hashCode
        AtlasEntity.AtlasEntityExtInfo extInfo2 = new AtlasEntity.AtlasEntityExtInfo();
        extInfo2.setReferredEntities(extInfo.getReferredEntities());
        assertEquals(extInfo, extInfo2);
        assertEquals(extInfo.hashCode(), extInfo2.hashCode());
    }

    @Test
    public void testAtlasEntityWithExtInfo() {
        AtlasEntity entity = new AtlasEntity("testType");
        entity.setGuid("entityGuid");

        // Test constructors
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        assertNotNull(entityWithExtInfo);

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo2 = new AtlasEntity.AtlasEntityWithExtInfo(entity);
        assertEquals(entity, entityWithExtInfo2.getEntity());

        AtlasEntity.AtlasEntityExtInfo extInfo = new AtlasEntity.AtlasEntityExtInfo();
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo3 = new AtlasEntity.AtlasEntityWithExtInfo(entity, extInfo);
        assertEquals(entity, entityWithExtInfo3.getEntity());

        // Test getEntity override
        assertEquals(entity, entityWithExtInfo2.getEntity("entityGuid"));

        // Test compact method
        AtlasEntity referredEntity = new AtlasEntity("referredType");
        referredEntity.setGuid("referredGuid");
        entityWithExtInfo2.addReferredEntity(referredEntity);
        entityWithExtInfo2.addReferredEntity(entity); // This should be removed by compact
        entityWithExtInfo2.compact();
        assertNull(entityWithExtInfo2.getReferredEntity("entityGuid"));
        assertNotNull(entityWithExtInfo2.getReferredEntity("referredGuid"));

        // Test toString
        String toString = entityWithExtInfo2.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasEntityWithExtInfo"));

        // Test equals and hashCode
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo4 = new AtlasEntity.AtlasEntityWithExtInfo(entity, extInfo);
        assertEquals(entityWithExtInfo3, entityWithExtInfo4);
        assertEquals(entityWithExtInfo3.hashCode(), entityWithExtInfo4.hashCode());
    }

    @Test
    public void testAtlasEntitiesWithExtInfo() {
        List<AtlasEntity> entities = new ArrayList<>();
        AtlasEntity entity1 = new AtlasEntity("type1");
        entity1.setGuid("guid1");
        AtlasEntity entity2 = new AtlasEntity("type2");
        entity2.setGuid("guid2");
        entities.add(entity1);
        entities.add(entity2);

        // Test constructors
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        assertNotNull(entitiesWithExtInfo);

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo2 = new AtlasEntity.AtlasEntitiesWithExtInfo(entity1);
        assertEquals(1, entitiesWithExtInfo2.getEntities().size());

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo3 = new AtlasEntity.AtlasEntitiesWithExtInfo(entities);
        assertEquals(entities, entitiesWithExtInfo3.getEntities());

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(entity1);
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo4 = new AtlasEntity.AtlasEntitiesWithExtInfo(entityWithExtInfo);
        assertEquals(1, entitiesWithExtInfo4.getEntities().size());

        AtlasEntity.AtlasEntityExtInfo extInfo = new AtlasEntity.AtlasEntityExtInfo();
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo5 = new AtlasEntity.AtlasEntitiesWithExtInfo(entities, extInfo);
        assertEquals(entities, entitiesWithExtInfo5.getEntities());

        // Test getEntity override
        assertEquals(entity1, entitiesWithExtInfo3.getEntity("guid1"));
        assertEquals(entity2, entitiesWithExtInfo3.getEntity("guid2"));

        // Test addEntity and removeEntity
        AtlasEntity entity3 = new AtlasEntity("type3");
        entitiesWithExtInfo.addEntity(entity3);
        assertEquals(1, entitiesWithExtInfo.getEntities().size());

        entitiesWithExtInfo.removeEntity(entity3);
        assertEquals(0, entitiesWithExtInfo.getEntities().size());

        // Test compact method
        AtlasEntity referredEntity = new AtlasEntity("referredType");
        referredEntity.setGuid("referredGuid");
        entitiesWithExtInfo3.addReferredEntity(referredEntity);
        entitiesWithExtInfo3.addReferredEntity(entity1); // This should be removed by compact
        entitiesWithExtInfo3.compact();
        assertNull(entitiesWithExtInfo3.getReferredEntity("guid1"));
        assertNotNull(entitiesWithExtInfo3.getReferredEntity("referredGuid"));

        // Test toString
        String toString = entitiesWithExtInfo3.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasEntitiesWithExtInfo"));

        // Test equals and hashCode
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo6 = new AtlasEntity.AtlasEntitiesWithExtInfo(entities, extInfo);
        assertEquals(entitiesWithExtInfo5, entitiesWithExtInfo6);
        assertEquals(entitiesWithExtInfo5.hashCode(), entitiesWithExtInfo6.hashCode());
    }

    @Test
    public void testAtlasEntitiesNestedClass() {
        // Test default constructor
        AtlasEntity.AtlasEntities entities = new AtlasEntity.AtlasEntities();
        assertNotNull(entities);

        // Test constructor with list
        List<AtlasEntity> entityList = new ArrayList<>();
        entityList.add(new AtlasEntity("type1"));
        entityList.add(new AtlasEntity("type2"));

        AtlasEntity.AtlasEntities entitiesWithList = new AtlasEntity.AtlasEntities(entityList);
        assertNotNull(entitiesWithList);
        assertEquals(entityList, entitiesWithList.getList());

        // Test constructor with pagination parameters
        AtlasEntity.AtlasEntities entitiesWithPagination = new AtlasEntity.AtlasEntities(entityList, 0, 10, 2, null, "name");
        assertNotNull(entitiesWithPagination);
        assertEquals(entityList, entitiesWithPagination.getList());
        assertEquals(0, entitiesWithPagination.getStartIndex());
        assertEquals(10, entitiesWithPagination.getPageSize());
        assertEquals(2, entitiesWithPagination.getTotalCount());
    }

    @Test
    public void testStatusEnum() {
        // Test all enum values
        assertEquals(3, Status.values().length);
        assertTrue(Arrays.asList(Status.values()).contains(Status.ACTIVE));
        assertTrue(Arrays.asList(Status.values()).contains(Status.DELETED));
        assertTrue(Arrays.asList(Status.values()).contains(Status.PURGED));
    }

    @Test
    public void testNullValues() {
        AtlasEntity entity = new AtlasEntity();

        // Test setting null values
        entity.setGuid(null);
        assertNull(entity.getGuid());

        entity.setRelationshipAttributes(null);
        assertNull(entity.getRelationshipAttributes());

        entity.setClassifications(null);
        assertNull(entity.getClassifications());

        entity.setMeanings(null);
        assertNull(entity.getMeanings());

        entity.setCustomAttributes(null);
        assertNull(entity.getCustomAttributes());

        entity.setBusinessAttributes(null);
        assertNull(entity.getBusinessAttributes());

        entity.setLabels(null);
        assertNull(entity.getLabels());

        entity.setPendingTasks(null);
        assertNull(entity.getPendingTasks());
    }

    @Test
    public void testCopyConstructorWithNull() {
        AtlasEntity entityFromNull = new AtlasEntity((AtlasEntity) null);
        assertNotNull(entityFromNull);
        assertNull(entityFromNull.getTypeName());
    }
}
