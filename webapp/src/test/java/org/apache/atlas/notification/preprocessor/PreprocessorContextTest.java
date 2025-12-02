/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.atlas.notification.preprocessor;

import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.EntityCorrelationManager;
import org.apache.atlas.notification.preprocessor.PreprocessorContext.PreprocessAction;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

public class PreprocessorContextTest {
    @Mock
    private AtlasKafkaMessage<HookNotification> kafkaMessage;

    @Mock
    private HookNotification.EntityCreateRequestV2 createRequest;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private AtlasEntitiesWithExtInfo entitiesWithExtInfo;

    @Mock
    private EntityCorrelationManager correlationManager;

    @Mock
    private AtlasEntity entity1;

    @Mock
    private AtlasEntity entity2;

    @Mock
    private AtlasObjectId objectId1;

    @Mock
    private AtlasObjectId objectId2;

    private List<Pattern> hiveTablesToIgnore;
    private List<Pattern> hiveTablesToPrune;
    private Map<String, PreprocessAction> hiveTablesCache;
    private List<String> hiveDummyDatabasesToIgnore;
    private List<String> hiveDummyTablesToIgnore;
    private List<String> hiveTablePrefixesToIgnore;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        hiveTablesToIgnore = new ArrayList<>();
        hiveTablesToPrune = new ArrayList<>();
        hiveTablesCache = new HashMap<>();
        hiveDummyDatabasesToIgnore = new ArrayList<>();
        hiveDummyTablesToIgnore = new ArrayList<>();
        hiveTablePrefixesToIgnore = new ArrayList<>();

        // Setup default mocks
        when(kafkaMessage.getMessage()).thenReturn(createRequest);
        when(kafkaMessage.getOffset()).thenReturn(12345L);
        when(kafkaMessage.getPartition()).thenReturn(1);
        when(kafkaMessage.getMsgCreated()).thenReturn(System.currentTimeMillis());
        when(kafkaMessage.getSpooled()).thenReturn(false);
        when(createRequest.getType()).thenReturn(HookNotification.HookNotificationType.ENTITY_DELETE_V2);
    }

    private PreprocessorContext createContext() {
        return new PreprocessorContext(
                kafkaMessage, typeRegistry, hiveTablesToIgnore, hiveTablesToPrune,
                hiveTablesCache, hiveDummyDatabasesToIgnore, hiveDummyTablesToIgnore,
                hiveTablePrefixesToIgnore, false, false, false, false, correlationManager);
    }

    private PreprocessorContext createContextWithCreateRequest() {
        when(createRequest.getType()).thenReturn(HookNotification.HookNotificationType.ENTITY_CREATE_V2);
        when(createRequest.getEntities()).thenReturn(entitiesWithExtInfo);
        return createContext();
    }

    @Test
    public void testConstructorWithEntityCreateRequest() {
        when(createRequest.getType()).thenReturn(HookNotification.HookNotificationType.ENTITY_CREATE_V2);
        when(createRequest.getEntities()).thenReturn(entitiesWithExtInfo);

        PreprocessorContext context = new PreprocessorContext(
                kafkaMessage, typeRegistry, hiveTablesToIgnore, hiveTablesToPrune,
                hiveTablesCache, hiveDummyDatabasesToIgnore, hiveDummyTablesToIgnore,
                hiveTablePrefixesToIgnore, true, true, true, true, correlationManager);

        assertNotNull(context);
        assertEquals(kafkaMessage, context.getKafkaMessage());
        assertEquals(12345L, context.getKafkaMessageOffset());
        assertEquals(1, context.getKafkaPartition());
        assertTrue(context.updateHiveProcessNameWithQualifiedName());
        assertTrue(context.getHiveTypesRemoveOwnedRefAttrs());
        assertTrue(context.getRdbmsTypesRemoveOwnedRefAttrs());
        assertTrue(context.getS3V2DirectoryPruneObjectPrefix());
    }

    @Test
    public void testConstructorWithEntityDeleteRequest() {
        when(createRequest.getType()).thenReturn(HookNotification.HookNotificationType.ENTITY_DELETE_V2);

        PreprocessorContext context = createContext();

        assertNotNull(context);
        assertNull(context.getEntities());
        assertNull(context.getReferredEntities());
    }

    @Test
    public void testIsHivePreprocessEnabledWithConfigurations() {
        hiveTablesToIgnore.add(Pattern.compile("test.*"));

        PreprocessorContext context = createContext();

        assertTrue(context.isHivePreprocessEnabled());
    }

    @Test
    public void testIsHivePreprocessEnabledWithoutConfigurations() {
        PreprocessorContext context = createContext();

        assertFalse(context.isHivePreprocessEnabled());
    }

    @Test
    public void testGetEntitiesAndReferredEntities() {
        List<AtlasEntity> entities = Arrays.asList(entity1, entity2);
        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        referredEntities.put("guid1", entity1);

        when(entitiesWithExtInfo.getEntities()).thenReturn(entities);
        when(entitiesWithExtInfo.getReferredEntities()).thenReturn(referredEntities);

        PreprocessorContext context = createContextWithCreateRequest();

        assertEquals(entities, context.getEntities());
        assertEquals(referredEntities, context.getReferredEntities());
    }

    @Test
    public void testGetEntity() {
        when(entitiesWithExtInfo.getEntity("guid1")).thenReturn(entity1);

        PreprocessorContext context = createContextWithCreateRequest();

        assertEquals(entity1, context.getEntity("guid1"));
        assertNull(context.getEntity(null));
    }

    @Test
    public void testRemoveReferredEntity() {
        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        referredEntities.put("guid1", entity1);
        when(entitiesWithExtInfo.getReferredEntities()).thenReturn(referredEntities);

        PreprocessorContext context = createContextWithCreateRequest();

        AtlasEntity removedEntity = context.removeReferredEntity("guid1");
        assertEquals(entity1, removedEntity);
        assertNull(context.removeReferredEntity("non_existent"));
    }

    @Test
    public void testGetPreprocessActionForHiveDb() {
        hiveDummyDatabasesToIgnore.add("test_db");
        hiveDummyDatabasesToIgnore.add("dummy_db");

        PreprocessorContext context = createContext();

        assertEquals(PreprocessAction.IGNORE, context.getPreprocessActionForHiveDb("test_db"));
        assertEquals(PreprocessAction.IGNORE, context.getPreprocessActionForHiveDb("TEST_DB"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveDb("normal_db"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveDb(null));
    }

    @Test
    public void testGetPreprocessActionForHiveTableWithIgnorePatterns() {
        hiveTablesToIgnore.add(Pattern.compile("test_.*"));
        hiveTablesToIgnore.add(Pattern.compile(".*_temp"));

        PreprocessorContext context = createContext();

        assertEquals(PreprocessAction.IGNORE, context.getPreprocessActionForHiveTable("test_table@cluster"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable("my_table_temp@cluster"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable("normal_table@cluster"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable(null));
    }

    @Test
    public void testGetPreprocessActionForHiveTableWithPrunePatterns() {
        hiveTablesToPrune.add(Pattern.compile("prune_.*"));

        PreprocessorContext context = createContext();

        assertEquals(PreprocessAction.PRUNE, context.getPreprocessActionForHiveTable("prune_table@cluster"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable("normal_table@cluster"));
    }

    @Test
    public void testGetPreprocessActionForHiveTableWithCache() {
        hiveTablesCache.put("cached_table@cluster", PreprocessAction.IGNORE);

        PreprocessorContext context = createContext();

        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable("cached_table@cluster"));
    }

    @Test
    public void testGetPreprocessActionForHiveTableWithDummyTables() {
        hiveDummyTablesToIgnore.add("temp_table");
        hiveDummyTablesToIgnore.add("dummy_table");

        PreprocessorContext context = createContext();

        assertEquals(PreprocessAction.IGNORE, context.getPreprocessActionForHiveTable("db.temp_table@cluster"));
        assertEquals(PreprocessAction.IGNORE, context.getPreprocessActionForHiveTable("db.DUMMY_TABLE@cluster"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable("db.normal_table@cluster"));
    }

    @Test
    public void testGetPreprocessActionForHiveTableWithTablePrefixes() {
        hiveTablePrefixesToIgnore.add("temp_");
        hiveTablePrefixesToIgnore.add("staging_");

        PreprocessorContext context = createContext();

        assertEquals(PreprocessAction.IGNORE, context.getPreprocessActionForHiveTable("db.temp_table@cluster"));
        assertEquals(PreprocessAction.IGNORE, context.getPreprocessActionForHiveTable("db.STAGING_table@cluster"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable("db.normal_table@cluster"));
    }

    @Test
    public void testGetPreprocessActionForHiveTableWithDummyDatabases() {
        hiveDummyDatabasesToIgnore.add("temp_db");

        PreprocessorContext context = createContext();

        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable("temp_db.table@cluster"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable("TEMP_DB.table@cluster"));
        assertEquals(PreprocessAction.NONE, context.getPreprocessActionForHiveTable("normal_db.table@cluster"));
    }

    @Test
    public void testIgnoredAndPrunedEntitiesManagement() {
        PreprocessorContext context = createContext();

        when(entity1.getGuid()).thenReturn("guid1");
        when(entity1.getTypeName()).thenReturn("test_type");
        when(entity1.getAttribute("qualifiedName")).thenReturn("test.qualified.name");

        // Test ignored entities
        assertFalse(context.isIgnoredEntity("guid1"));
        context.addToIgnoredEntities(entity1);
        assertTrue(context.isIgnoredEntity("guid1"));
        assertTrue(context.getIgnoredEntities().contains("guid1"));

        // Test pruned entities
        assertFalse(context.isPrunedEntity("guid2"));
        context.addToPrunedEntities("guid2");
        assertTrue(context.isPrunedEntity("guid2"));
        assertTrue(context.getPrunedEntities().contains("guid2"));

        // Test referred entities to move
        context.addToReferredEntitiesToMove("guid3");
        assertTrue(context.getReferredEntitiesToMove().contains("guid3"));

        context.addToReferredEntitiesToMove(Arrays.asList("guid4", "guid5"));
        assertTrue(context.getReferredEntitiesToMove().contains("guid4"));
        assertTrue(context.getReferredEntitiesToMove().contains("guid5"));
    }

    @Test
    public void testAddToIgnoredEntitiesWithObjects() {
        PreprocessorContext context = createContext();

        when(objectId1.getGuid()).thenReturn("guid1");
        when(objectId2.getGuid()).thenReturn("guid2");

        List<AtlasObjectId> objectList = Arrays.asList(objectId1, objectId2);
        context.addToIgnoredEntities(objectList);

        assertTrue(context.isIgnoredEntity("guid1"));
        assertTrue(context.isIgnoredEntity("guid2"));
    }

    @Test
    public void testHiveTableNameFromQualifiedName() {
        PreprocessorContext context = createContext();

        assertEquals(context.getHiveTableNameFromQualifiedName("db.table@cluster"), "table");
        assertNull(context.getHiveTableNameFromQualifiedName("db@cluster"));
        assertNull(context.getHiveTableNameFromQualifiedName("table"));
    }

    @Test
    public void testHiveDbNameFromQualifiedName() {
        PreprocessorContext context = createContext();

        assertEquals("db", context.getHiveDbNameFromQualifiedName("db.table@cluster"));
        assertEquals("db", context.getHiveDbNameFromQualifiedName("db.table.column@cluster"));
        assertEquals("db", context.getHiveDbNameFromQualifiedName("db@cluster"));
        assertNull(context.getHiveDbNameFromQualifiedName("table"));
    }

    @Test
    public void testGetTypeNameFromObjects() {
        PreprocessorContext context = createContext();

        when(objectId1.getTypeName()).thenReturn("hive_table");
        assertEquals("hive_table", context.getTypeName(objectId1));

        when(entity1.getTypeName()).thenReturn("hive_column");
        assertEquals("hive_column", context.getTypeName(entity1));

        Map<String, Object> map = new HashMap<>();
        map.put("typeName", "hive_db");
        assertEquals("hive_db", context.getTypeName(map));

        assertNull(context.getTypeName("invalid_object"));
    }

    @Test
    public void testGetGuidFromObjects() {
        PreprocessorContext context = createContext();

        when(objectId1.getGuid()).thenReturn("guid1");
        assertEquals("guid1", context.getGuid(objectId1));

        when(entity1.getGuid()).thenReturn("guid2");
        assertEquals("guid2", context.getGuid(entity1));

        Map<String, Object> map = new HashMap<>();
        map.put("guid", "guid3");
        assertEquals("guid3", context.getGuid(map));

        assertNull(context.getGuid("invalid_object"));
    }

    @Test
    public void testGetMsgCreatedAndSpooledMessage() {
        long currentTime = System.currentTimeMillis();
        when(kafkaMessage.getMsgCreated()).thenReturn(currentTime);
        when(kafkaMessage.getSpooled()).thenReturn(true);

        PreprocessorContext context = createContext();

        assertEquals(currentTime, context.getMsgCreated());
        assertTrue(context.isSpooledMessage());
    }

    @Test
    public void testGetGuidForDeletedEntity() {
        when(correlationManager.getGuidForDeletedEntityToBeCorrelated("test.qualified.name", 12345L))
                .thenReturn("deleted_guid");

        when(kafkaMessage.getMsgCreated()).thenReturn(12345L);

        PreprocessorContext context = createContext();

        assertEquals("deleted_guid", context.getGuidForDeletedEntity("test.qualified.name"));
    }

    @Test
    public void testCollectGuids() {
        PreprocessorContext context = createContext();

        when(objectId1.getGuid()).thenReturn("guid1");
        when(objectId2.getGuid()).thenReturn("guid2");
        when(entity1.getGuid()).thenReturn("guid3");

        List<Object> objects = Arrays.asList(objectId1, objectId2, entity1);
        Set<String> guids = context.getIgnoredEntities(); // Use existing set for testing
        guids.clear(); // Clear any existing entries

        context.collectGuids(objects, guids);

        assertTrue(guids.contains("guid1"));
        assertTrue(guids.contains("guid2"));
        assertTrue(guids.contains("guid3"));
        assertEquals(3, guids.size());
    }

    @Test
    public void testMoveRegisteredReferredEntities() {
        List<AtlasEntity> entities = new ArrayList<>();
        entities.add(entity1);
        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        referredEntities.put("guid2", entity2);

        when(entitiesWithExtInfo.getEntities()).thenReturn(entities);
        when(entitiesWithExtInfo.getReferredEntities()).thenReturn(referredEntities);

        PreprocessorContext context = createContextWithCreateRequest();

        context.addToReferredEntitiesToMove("guid2");
        context.moveRegisteredReferredEntities();

        assertEquals(2, entities.size());
        assertTrue(entities.contains(entity2));
        assertFalse(referredEntities.containsKey("guid2"));
        assertTrue(context.getReferredEntitiesToMove().isEmpty());
    }

    @Test
    public void testIsMatchPrivateMethod() throws Exception {
        PreprocessorContext context = createContext();

        Method isMatchMethod = PreprocessorContext.class.getDeclaredMethod("isMatch", String.class, List.class);
        isMatchMethod.setAccessible(true);

        List<Pattern> patterns = Arrays.asList(
                Pattern.compile("test_.*"),
                Pattern.compile(".*_temp"));

        boolean result1 = (boolean) isMatchMethod.invoke(context, "test_table", patterns);
        assertTrue(result1);

        boolean result2 = (boolean) isMatchMethod.invoke(context, "my_table_temp", patterns);
        assertTrue(result2);

        boolean result3 = (boolean) isMatchMethod.invoke(context, "normal_table", patterns);
        assertFalse(result3);
    }

    @Test
    public void testSetRelationshipTypePrivateMethod() throws Exception {
        PreprocessorContext context = createContext();

        Method setRelationshipTypeMethod = PreprocessorContext.class.getDeclaredMethod("setRelationshipType", Object.class, String.class);
        setRelationshipTypeMethod.setAccessible(true);

        AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId();
        AtlasRelatedObjectId result1 = (AtlasRelatedObjectId) setRelationshipTypeMethod.invoke(context, relatedObjectId, "test_relationship");
        assertNotNull(result1);
        assertEquals("test_relationship", result1.getRelationshipType());

        AtlasObjectId objectId = new AtlasObjectId();
        AtlasRelatedObjectId result2 = (AtlasRelatedObjectId) setRelationshipTypeMethod.invoke(context, objectId, "test_relationship2");
        assertNotNull(result2);
        assertEquals("test_relationship2", result2.getRelationshipType());

        Map<String, Object> map = new HashMap<>();
        map.put("typeName", "test_type");
        AtlasRelatedObjectId result3 = (AtlasRelatedObjectId) setRelationshipTypeMethod.invoke(context, map, "test_relationship3");
        assertNotNull(result3);
        assertEquals("test_relationship3", result3.getRelationshipType());

        assertNull(setRelationshipTypeMethod.invoke(context, "invalid_object", "relationship"));
    }

    @Test
    public void testGetAssignedGuidPrivateMethod() throws Exception {
        PreprocessorContext context = createContext();

        Method getAssignedGuidMethod = PreprocessorContext.class.getDeclaredMethod("getAssignedGuid", String.class);
        getAssignedGuidMethod.setAccessible(true);

        // Add guid assignment
        context.getGuidAssignments().put("guid1", "assigned_guid1");

        String result1 = (String) getAssignedGuidMethod.invoke(context, "guid1");
        assertEquals("assigned_guid1", result1);

        String result2 = (String) getAssignedGuidMethod.invoke(context, "guid2");
        assertEquals("guid2", result2); // Returns original guid if no assignment
    }

    @Test
    public void testPrepareForPostUpdate() {
        PreprocessorContext context = createContextWithCreateRequest();

        // Test with null postUpdateEntities
        context.prepareForPostUpdate();

        // Test with empty postUpdateEntities
        assertNull(context.getPostUpdateEntities());

        // Add a created entity and test removal
        context.getCreatedEntities().add("created_guid");

        // Invoke prepareForPostUpdate - should handle null postUpdateEntities gracefully
        context.prepareForPostUpdate();

        // Verify it doesn't crash and handles null case
        assertNull(context.getPostUpdateEntities());
    }

    @Test
    public void testEdgeCasesForNullHandling() {
        PreprocessorContext context = createContext();

        // Test null handling in various methods
        context.addToIgnoredEntities((String) null);
        context.addToPrunedEntities((String) null);
        context.addToReferredEntitiesToMove((String) null);
        context.addToReferredEntitiesToMove((List<String>) null);

        assertFalse(context.isIgnoredEntity(null));
        assertFalse(context.isPrunedEntity(null));

        assertNull(context.getTypeName(null));
        assertNull(context.getGuid(null));

        // Test collectGuids with null
        Set<String> guids = context.getIgnoredEntities();
        int initialSize = guids.size();
        context.collectGuids(null, guids);
        assertEquals(initialSize, guids.size());
    }

    @Test
    public void testAddToPrunedEntitiesWithEntity() {
        PreprocessorContext context = createContext();

        when(entity1.getGuid()).thenReturn("guid1");
        when(entity1.getTypeName()).thenReturn("hive_table");
        when(entity1.getAttribute("qualifiedName")).thenReturn("test.table@cluster");

        // Test adding entity to pruned entities
        assertFalse(context.isPrunedEntity("guid1"));
        context.addToPrunedEntities(entity1);
        assertTrue(context.isPrunedEntity("guid1"));
        assertTrue(context.getPrunedEntities().contains("guid1"));

        // Test adding same entity again (should not duplicate)
        int sizeBeforeDuplicate = context.getPrunedEntities().size();
        context.addToPrunedEntities(entity1);
        assertEquals(sizeBeforeDuplicate, context.getPrunedEntities().size());
    }

    @Test
    public void testAddToPrunedEntitiesWithObject() {
        PreprocessorContext context = createContext();

        when(objectId1.getGuid()).thenReturn("guid1");
        when(objectId2.getGuid()).thenReturn("guid2");

        List<AtlasObjectId> objectList = Arrays.asList(objectId1, objectId2);

        // Test adding objects to pruned entities
        context.addToPrunedEntities(objectList);

        assertTrue(context.isPrunedEntity("guid1"));
        assertTrue(context.isPrunedEntity("guid2"));
    }

    @Test
    public void testRemoveRefAttributeAndRegisterToMove() throws Exception {
        PreprocessorContext context = createContextWithCreateRequest();

        when(entitiesWithExtInfo.getEntity("ref_guid")).thenReturn(entity2);
        when(entity1.removeAttribute("columns")).thenReturn(Collections.singletonList(objectId1));
        when(objectId1.getGuid()).thenReturn("ref_guid");

        // Test case 1: entity has relationship attribute
        when(entity2.hasRelationshipAttribute("table")).thenReturn(true);
        when(entity2.getRelationshipAttribute("table")).thenReturn(objectId2);

        context.removeRefAttributeAndRegisterToMove(entity1, "columns", "hive_table_columns", "table");

        assertTrue(context.getReferredEntitiesToMove().contains("ref_guid"));
        verify(entity2).setRelationshipAttribute(eq("table"), any());

        // Test case 2: entity has regular attribute
        when(entity2.hasRelationshipAttribute("table")).thenReturn(false);
        when(entity2.hasAttribute("table")).thenReturn(true);
        when(entity2.getAttribute("table")).thenReturn(objectId2);

        context.removeRefAttributeAndRegisterToMove(entity1, "columns", "hive_table_columns", "table");

        assertTrue(context.getReferredEntitiesToMove().contains("ref_guid"));

        // Reset for next test
        context.getReferredEntitiesToMove().clear();

        // Test case 3: entity has neither relationship nor regular attribute
        when(entity2.hasRelationshipAttribute("table")).thenReturn(false);
        when(entity2.hasAttribute("table")).thenReturn(false);

        context.removeRefAttributeAndRegisterToMove(entity1, "columns", "hive_table_columns", "table");

        assertTrue(context.getReferredEntitiesToMove().contains("ref_guid"));
    }

    @Test
    public void testRemoveRefAttributeAndRegisterToMoveWithNullAttribute() {
        PreprocessorContext context = createContextWithCreateRequest();

        when(entity1.removeAttribute("columns")).thenReturn(null);

        // Should not throw exception when attribute is null
        context.removeRefAttributeAndRegisterToMove(entity1, "columns", "hive_table_columns", "table");

        assertTrue(context.getReferredEntitiesToMove().isEmpty());
    }

    @Test
    public void testPrepareForPostUpdateWithEntities() throws Exception {
        PreprocessorContext context = createContextWithCreateRequest();

        // Use reflection to access private method addToPostUpdate
        Method addToPostUpdateMethod = PreprocessorContext.class.getDeclaredMethod("addToPostUpdate", AtlasEntity.class, String.class, Object.class);
        addToPostUpdateMethod.setAccessible(true);

        // Create test entity
        when(entity1.getGuid()).thenReturn("guid1");
        when(entity1.getTypeName()).thenReturn("hive_table");

        // Add entity to post-update list
        addToPostUpdateMethod.invoke(context, entity1, "testAttr", "testValue");

        // Set up guid assignments
        context.getGuidAssignments().put("guid1", "assigned_guid1");

        // Mock entity attributes and relationship attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr1", objectId1);
        Map<String, Object> relationshipAttributes = new HashMap<>();
        relationshipAttributes.put("relAttr1", objectId2);

        when(entity1.getAttributes()).thenReturn(attributes);
        when(entity1.getRelationshipAttributes()).thenReturn(relationshipAttributes);
        when(objectId1.getGuid()).thenReturn("guid2");
        when(objectId2.getGuid()).thenReturn("guid3");

        context.prepareForPostUpdate();

//        verify(entity1).setGuid("assigned_guid1");
        assertNotNull(context.getPostUpdateEntities());
        assertEquals(1, context.getPostUpdateEntities().size());
    }

    @Test
    public void testPrepareForPostUpdateWithCreatedEntities() throws Exception {
        PreprocessorContext context = createContextWithCreateRequest();

        // Use reflection to access private method addToPostUpdate
        Method addToPostUpdateMethod = PreprocessorContext.class.getDeclaredMethod("addToPostUpdate", AtlasEntity.class, String.class, Object.class);
        addToPostUpdateMethod.setAccessible(true);

        when(entity1.getGuid()).thenReturn("guid1");
        when(entity1.getTypeName()).thenReturn("hive_table");

        // Add entity to post-update list
        addToPostUpdateMethod.invoke(context, entity1, "testAttr", "testValue");

        // Add guid to created entities (should be removed from post-update)
        context.getGuidAssignments().put("guid1", "assigned_guid1");
        context.getCreatedEntities().add("assigned_guid1");

        context.prepareForPostUpdate();

        assertNotNull(context.getPostUpdateEntities());
        assertEquals(0, context.getPostUpdateEntities().size()); // Should be removed
    }

    @Test
    public void testPrepareForPostUpdateWithDeletedEntities() throws Exception {
        PreprocessorContext context = createContextWithCreateRequest();

        Method addToPostUpdateMethod = PreprocessorContext.class.getDeclaredMethod("addToPostUpdate", AtlasEntity.class, String.class, Object.class);
        addToPostUpdateMethod.setAccessible(true);

        when(entity1.getGuid()).thenReturn("guid1");
        when(entity1.getTypeName()).thenReturn("hive_table");

        // Add entity to post-update list
        addToPostUpdateMethod.invoke(context, entity1, "testAttr", "testValue");

        // Add guid to deleted entities (should be removed from post-update)
        context.getGuidAssignments().put("guid1", "assigned_guid1");
        context.getDeletedEntities().add("assigned_guid1");

        context.prepareForPostUpdate();

        assertNotNull(context.getPostUpdateEntities());
        assertEquals(0, context.getPostUpdateEntities().size()); // Should be removed
    }

    @Test
    public void testSetAssignedGuidsPrivateMethod() throws Exception {
        PreprocessorContext context = createContext();

        Method setAssignedGuidsMethod = PreprocessorContext.class.getDeclaredMethod("setAssignedGuids", Object.class);
        setAssignedGuidsMethod.setAccessible(true);

        // Test with AtlasRelatedObjectId
        AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId();
        relatedObjectId.setGuid("guid1");
        context.getGuidAssignments().put("guid1", "assigned_guid1");

        setAssignedGuidsMethod.invoke(context, relatedObjectId);
        assertEquals("assigned_guid1", relatedObjectId.getGuid());

        // Test with AtlasObjectId
        AtlasObjectId objectId = new AtlasObjectId();
        objectId.setGuid("guid2");
        context.getGuidAssignments().put("guid2", "assigned_guid2");

        setAssignedGuidsMethod.invoke(context, objectId);
        assertEquals("assigned_guid2", objectId.getGuid());

        // Test with Map
        Map<String, Object> map = new HashMap<>();
        map.put("guid", "guid3");
        context.getGuidAssignments().put("guid3", "assigned_guid3");

        setAssignedGuidsMethod.invoke(context, map);
        assertEquals("assigned_guid3", map.get("guid"));

        // Test with Collection
        List<Object> collection = Arrays.asList(relatedObjectId, objectId);
        setAssignedGuidsMethod.invoke(context, collection);
        // Should process each element in collection
    }

    @Test
    public void testSetAssignedGuidPrivateMethod() throws Exception {
        PreprocessorContext context = createContext();

        Method setAssignedGuidMethod = PreprocessorContext.class.getDeclaredMethod("setAssignedGuid", Object.class);
        setAssignedGuidMethod.setAccessible(true);

        // Test with AtlasRelatedObjectId
        AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId();
        relatedObjectId.setGuid("guid1");
        context.getGuidAssignments().put("guid1", "assigned_guid1");

        setAssignedGuidMethod.invoke(context, relatedObjectId);
        assertEquals("assigned_guid1", relatedObjectId.getGuid());

        // Test with AtlasObjectId
        AtlasObjectId objectId = new AtlasObjectId();
        objectId.setGuid("guid2");
        context.getGuidAssignments().put("guid2", "assigned_guid2");

        setAssignedGuidMethod.invoke(context, objectId);
        assertEquals("assigned_guid2", objectId.getGuid());

        // Test with Map containing guid
        Map<String, Object> mapWithGuid = new HashMap<>();
        mapWithGuid.put("guid", "guid3");
        context.getGuidAssignments().put("guid3", "assigned_guid3");

        setAssignedGuidMethod.invoke(context, mapWithGuid);
        assertEquals("assigned_guid3", mapWithGuid.get("guid"));

        // Test with Map without guid
        Map<String, Object> mapWithoutGuid = new HashMap<>();
        mapWithoutGuid.put("other", "value");

        setAssignedGuidMethod.invoke(context, mapWithoutGuid);
        assertEquals("value", mapWithoutGuid.get("other")); // Should remain unchanged

        // Test with other object types (should not modify)
        String otherObject = "test";
        setAssignedGuidMethod.invoke(context, otherObject);
        assertEquals("test", otherObject);
    }

    @Test
    public void testAddToPostUpdatePrivateMethod() throws Exception {
        PreprocessorContext context = createContext();

        Method addToPostUpdateMethod = PreprocessorContext.class.getDeclaredMethod("addToPostUpdate", AtlasEntity.class, String.class, Object.class);
        addToPostUpdateMethod.setAccessible(true);

        when(entity1.getGuid()).thenReturn("guid1");
        when(entity1.getTypeName()).thenReturn("hive_table");

        // Test adding first entity
        addToPostUpdateMethod.invoke(context, entity1, "attr1", "value1");

        assertNotNull(context.getPostUpdateEntities());
        assertEquals(1, context.getPostUpdateEntities().size());

        // Test adding same entity with different attribute (should update existing)
        addToPostUpdateMethod.invoke(context, entity1, "attr2", "value2");

        assertEquals(1, context.getPostUpdateEntities().size()); // Still only one entity

        // Test adding different entity
        when(entity2.getGuid()).thenReturn("guid2");
        when(entity2.getTypeName()).thenReturn("hive_column");

        addToPostUpdateMethod.invoke(context, entity2, "attr3", "value3");

        assertEquals(2, context.getPostUpdateEntities().size()); // Now two entities
    }
}
