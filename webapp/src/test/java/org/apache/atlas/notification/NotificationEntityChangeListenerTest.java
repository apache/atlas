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

package org.apache.atlas.notification;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.atlas.v1.model.notification.EntityNotificationV1;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

public class NotificationEntityChangeListenerTest {
    private static final String ENTITY_TYPE = "DataSet";
    private static final String INTERNAL_ENTITY_TYPE = "__AtlasAuditEntry";
    private static final String CLASSIFICATION_TYPE = "PII";
    private static final String SUPER_CLASSIFICATION_TYPE = "Sensitive";
    private static final String ENTITY_ID = "entity-id-123";
    @Mock
    private AtlasTypeRegistry typeRegistry;
    @Mock
    private NotificationInterface notificationInterface;
    @Mock
    private Configuration configuration;
    @Mock
    private RequestContext requestContext;
    @Mock
    private MetricRecorder metricRecorder;
    @Mock
    private AtlasClassificationType classificationType;
    @Mock
    private AtlasClassificationType superClassificationType;
    @Mock
    private AtlasGlossaryTerm glossaryTerm;
    @SuppressWarnings("unchecked")
    @Mock
    private EntityNotificationSender<EntityNotificationV1> notificationSender;
    private NotificationEntityChangeListener listener;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Set up RequestContext in ThreadLocal
        setRequestContext();

        // Create listener instance
        listener = new NotificationEntityChangeListener(notificationInterface, typeRegistry, configuration);

        // Inject mocked notificationSender using reflection
        setNotificationSender();

        // Setup common mocks
        setupCommonMocks();
    }

    @AfterMethod
    public void tearDown() {
        RequestContext.clear();
    }

    /**
     * Helper method to set RequestContext in ThreadLocal using reflection
     */
    private void setRequestContext() throws Exception {
        Field currentContextField = RequestContext.class.getDeclaredField("CURRENT_CONTEXT");
        currentContextField.setAccessible(true);
        ThreadLocal<RequestContext> threadLocal = (ThreadLocal<RequestContext>) currentContextField.get(null);
        threadLocal.set(requestContext);
    }

    /**
     * Helper method to inject mocked notificationSender using reflection
     */
    private void setNotificationSender() throws Exception {
        Field notificationSenderField = NotificationEntityChangeListener.class.getDeclaredField("notificationSender");
        notificationSenderField.setAccessible(true);
        notificationSenderField.set(listener, notificationSender);
    }

    /**
     * Setup common mocks used across multiple tests
     */
    private void setupCommonMocks() {
        // RequestContext mocks
        when(requestContext.startMetricRecord(anyString())).thenReturn(metricRecorder);
        doNothing().when(requestContext).endMetricRecord(any(MetricRecorder.class));

        // Type registry mocks
        when(typeRegistry.getClassificationTypeByName(CLASSIFICATION_TYPE)).thenReturn(classificationType);
        when(typeRegistry.getClassificationTypeByName(SUPER_CLASSIFICATION_TYPE)).thenReturn(superClassificationType);

        // Classification type mocks
        when(classificationType.getAllSuperTypes()).thenReturn(Collections.singleton(SUPER_CLASSIFICATION_TYPE));
        when(classificationType.getAllAttributes()).thenReturn(new HashMap<>());
        when(superClassificationType.getAllSuperTypes()).thenReturn(Collections.emptySet());
        when(superClassificationType.getAllAttributes()).thenReturn(new HashMap<>());
    }

    // Constructor Tests

    @Test
    public void testConstructorWithValidParameters() {
        NotificationEntityChangeListener testListener = new NotificationEntityChangeListener(
                notificationInterface, typeRegistry, configuration);

        assertNotNull(testListener);

        // Verify fields are set correctly using reflection
        try {
            Field typeRegistryField = NotificationEntityChangeListener.class.getDeclaredField("typeRegistry");
            typeRegistryField.setAccessible(true);
            assertEquals(typeRegistry, typeRegistryField.get(testListener));

            Field configurationField = NotificationEntityChangeListener.class.getDeclaredField("configuration");
            configurationField.setAccessible(true);
            assertEquals(configuration, configurationField.get(testListener));

            Field notificationSenderField = NotificationEntityChangeListener.class.getDeclaredField("notificationSender");
            notificationSenderField.setAccessible(true);
            assertNotNull(notificationSenderField.get(testListener));
        } catch (Exception e) {
            fail("Failed to verify constructor initialization: " + e.getMessage());
        }
    }

    @Test
    public void testConstructorWithNullConfiguration() {
        NotificationEntityChangeListener testListener = new NotificationEntityChangeListener(
                notificationInterface, typeRegistry, null);

        assertNotNull(testListener);
    }

    // EntityChangeListener Interface Tests

    @Test
    public void testOnEntitiesAdded() throws Exception {
        Collection<Referenceable> entities = Collections.singletonList(createTestEntity(ENTITY_ID, ENTITY_TYPE));

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);

            listener.onEntitiesAdded(entities, false);

            verify(notificationSender).send(anyList());
            verify(requestContext).startMetricRecord("entityNotification");
            verify(requestContext).endMetricRecord(metricRecorder);
        }
    }

    @Test
    public void testOnEntitiesAddedWithImport() throws Exception {
        Collection<Referenceable> entities = Collections.singletonList(createTestEntity(ENTITY_ID, ENTITY_TYPE));

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);

            listener.onEntitiesAdded(entities, true);

            verify(notificationSender).send(anyList());
        }
    }

    @Test
    public void testOnEntitiesUpdated() throws Exception {
        Collection<Referenceable> entities = Collections.singletonList(createTestEntity(ENTITY_ID, ENTITY_TYPE));

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);

            listener.onEntitiesUpdated(entities, false);

            verify(notificationSender).send(anyList());
            verify(requestContext).startMetricRecord("entityNotification");
            verify(requestContext).endMetricRecord(metricRecorder);
        }
    }

    @Test
    public void testOnEntitiesDeleted() throws Exception {
        Collection<Referenceable> entities = Collections.singletonList(createTestEntity(ENTITY_ID, ENTITY_TYPE));

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);

            listener.onEntitiesDeleted(entities, false);

            verify(notificationSender).send(anyList());
        }
    }

    @Test
    public void testOnTraitsAdded() throws Exception {
        Referenceable entity = createTestEntity(ENTITY_ID, ENTITY_TYPE);
        Collection<Struct> traits = Collections.singletonList(new Struct(CLASSIFICATION_TYPE));

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);

            listener.onTraitsAdded(entity, traits);

            verify(notificationSender).send(anyList());
        }
    }

    @Test
    public void testOnTraitsUpdated() throws Exception {
        Referenceable entity = createTestEntity(ENTITY_ID, ENTITY_TYPE);
        Collection<Struct> traits = Collections.singletonList(new Struct(CLASSIFICATION_TYPE));

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);

            listener.onTraitsUpdated(entity, traits);

            verify(notificationSender).send(anyList());
        }
    }

    @Test
    public void testOnTraitsDeleted() throws Exception {
        Referenceable entity = createTestEntity(ENTITY_ID, ENTITY_TYPE);
        Collection<Struct> traits = Collections.singletonList(new Struct(CLASSIFICATION_TYPE));

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);

            listener.onTraitsDeleted(entity, traits);

            verify(notificationSender).send(anyList());
        }
    }

    @Test
    public void testOnTermAdded() {
        Collection<Referenceable> entities = Collections.singletonList(createTestEntity(ENTITY_ID, ENTITY_TYPE));

        // This method does nothing, so we just verify it doesn't throw
        assertDoesNotThrow(() -> listener.onTermAdded(entities, glossaryTerm));

        // Verify no notifications are sent
        verifyNoInteractions(notificationSender);
    }

    @Test
    public void testOnTermDeleted() {
        Collection<Referenceable> entities = Collections.singletonList(createTestEntity(ENTITY_ID, ENTITY_TYPE));

        // This method does nothing, so we just verify it doesn't throw
        assertDoesNotThrow(() -> listener.onTermDeleted(entities, glossaryTerm));

        // Verify no notifications are sent
        verifyNoInteractions(notificationSender);
    }

    // Static Method Tests

    @Test
    public void testGetAllTraitsSuperTraits() throws Exception {
        AtlasTypeRegistry typeSystem = mock(AtlasTypeRegistry.class);

        String traitName = "MyTrait";
        Struct myTrait = new Struct(traitName);

        String superTraitName = "MySuperTrait";

        AtlasClassificationType traitDef = mock(AtlasClassificationType.class);
        Set<String> superTypeNames = Collections.singleton(superTraitName);

        AtlasClassificationType superTraitDef = mock(AtlasClassificationType.class);
        Set<String> superSuperTypeNames = Collections.emptySet();

        Referenceable entity = getEntity("id", myTrait);

        when(typeSystem.getClassificationTypeByName(traitName)).thenReturn(traitDef);
        when(typeSystem.getClassificationTypeByName(superTraitName)).thenReturn(superTraitDef);

        when(traitDef.getAllSuperTypes()).thenReturn(superTypeNames);
        when(superTraitDef.getAllSuperTypes()).thenReturn(superSuperTypeNames);

        List<Struct> allTraits = NotificationEntityChangeListener.getAllTraits(entity, typeSystem);

        assertEquals(2, allTraits.size());

        for (Struct trait : allTraits) {
            String typeName = trait.getTypeName();
            assertTrue(typeName.equals(traitName) || typeName.equals(superTraitName));
        }
    }

    @Test
    public void testGetAllTraitsWithAttributes() throws Exception {
        AtlasTypeRegistry typeSystem = mock(AtlasTypeRegistry.class);

        String traitName = "MyTrait";
        Struct myTrait = new Struct(traitName);
        Map<String, Object> traitAttributes = new HashMap<>();
        traitAttributes.put("attr1", "value1");
        traitAttributes.put("attr2", "value2");
        myTrait.setValues(traitAttributes);

        String superTraitName = "MySuperTrait";

        AtlasClassificationType traitDef = mock(AtlasClassificationType.class);
        Set<String> superTypeNames = Collections.singleton(superTraitName);

        AtlasClassificationType superTraitDef = mock(AtlasClassificationType.class);
        Set<String> superSuperTypeNames = Collections.emptySet();

        AtlasStructType.AtlasAttribute mockAttr = mock(AtlasStructType.AtlasAttribute.class);

        Map<String, AtlasStructType.AtlasAttribute> superTraitAttributes = new HashMap<>();
        superTraitAttributes.put("attr1", mockAttr); // only attr1 should be inherited
        when(superTraitDef.getAllAttributes()).thenReturn(superTraitAttributes);

        Referenceable entity = getEntity("id", myTrait);

        when(typeSystem.getClassificationTypeByName(traitName)).thenReturn(traitDef);
        when(typeSystem.getClassificationTypeByName(superTraitName)).thenReturn(superTraitDef);

        when(traitDef.getAllSuperTypes()).thenReturn(superTypeNames);
        when(superTraitDef.getAllSuperTypes()).thenReturn(superSuperTypeNames);

        List<Struct> allTraits = NotificationEntityChangeListener.getAllTraits(entity, typeSystem);

        assertEquals(2, allTraits.size());

        Struct superTrait = allTraits.stream()
                .filter(t -> t.getTypeName().equals(superTraitName))
                .findFirst()
                .orElse(null);

        assertNotNull(superTrait);
        assertNotNull(superTrait.getValues());
        assertEquals("value1", superTrait.getValues().get("attr1"));
        assertNull(superTrait.getValues().get("attr2")); // should not inherit attr2
    }

    @Test
    public void testGetAllTraitsNoSuperTypes() throws Exception {
        AtlasTypeRegistry typeSystem = mock(AtlasTypeRegistry.class);

        String traitName = "MyTrait";
        Struct myTrait = new Struct(traitName);

        AtlasClassificationType traitDef = mock(AtlasClassificationType.class);
        Set<String> superTypeNames = Collections.emptySet();

        Referenceable entity = getEntity("id", myTrait);

        when(typeSystem.getClassificationTypeByName(traitName)).thenReturn(traitDef);
        when(traitDef.getAllSuperTypes()).thenReturn(superTypeNames);

        List<Struct> allTraits = NotificationEntityChangeListener.getAllTraits(entity, typeSystem);

        assertEquals(1, allTraits.size());
        assertEquals(traitName, allTraits.get(0).getTypeName());
    }

    @Test
    public void testGetAllTraitsNullTypeRegistry() throws Exception {
        String traitName = "MyTrait";
        Struct myTrait = new Struct(traitName);
        Referenceable entity = getEntity("id", myTrait);

        // When type registry returns null for classification type
        when(typeRegistry.getClassificationTypeByName(traitName)).thenReturn(null);

        List<Struct> allTraits = NotificationEntityChangeListener.getAllTraits(entity, typeRegistry);

        assertEquals(1, allTraits.size());
        assertEquals(traitName, allTraits.get(0).getTypeName());
    }

    // Edge Case Tests

    @Test
    public void testOnEntitiesAddedWithEmptyCollection() throws Exception {
        Collection<Referenceable> entities = Collections.emptyList();

        listener.onEntitiesAdded(entities, false);

        // Should not send notification for empty collection
        verify(notificationSender, never()).send(anyList());
    }

    @Test
    public void testOnEntitiesAddedWithInternalType() throws Exception {
        Collection<Referenceable> entities = Collections.singletonList(createTestEntity(ENTITY_ID, INTERNAL_ENTITY_TYPE));

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(INTERNAL_ENTITY_TYPE)).thenReturn(true);

            listener.onEntitiesAdded(entities, false);

            // Should not send notification for internal type
            verify(notificationSender, never()).send(anyList());
        }
    }

    @Test
    public void testOnEntitiesAddedWithMixedTypes() throws Exception {
        Collection<Referenceable> entities = Arrays.asList(
                createTestEntity(ENTITY_ID, ENTITY_TYPE),
                createTestEntity("internal-id", INTERNAL_ENTITY_TYPE));

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);
            graphHelperMock.when(() -> GraphHelper.isInternalType(INTERNAL_ENTITY_TYPE)).thenReturn(true);

            listener.onEntitiesAdded(entities, false);

            // Should send notification only for non-internal type
            verify(notificationSender).send(argThat(notifications ->
                    notifications.size() == 1 &&
                            notifications.get(0).getEntity().getTypeName().equals(ENTITY_TYPE)));
        }
    }

    // Private Method Tests using Reflection

    @Test
    public void testGetNotificationAttributesWithConfiguration() throws Exception {
        String entityType = "TestEntity";
        String[] expectedAttributes = {"attr1", "attr2", "attr3"};

        when(configuration.getStringArray(
                "atlas.notification.entity.TestEntity.attributes.include"))
                .thenReturn(expectedAttributes);

        Method getNotificationAttributesMethod = NotificationEntityChangeListener.class
                .getDeclaredMethod("getNotificationAttributes", String.class);
        getNotificationAttributesMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) getNotificationAttributesMethod.invoke(listener, entityType);

        assertNotNull(result);
        assertEquals(Arrays.asList(expectedAttributes), result);

        verify(configuration).getStringArray("atlas.notification.entity.TestEntity.attributes.include");
    }

    @Test
    public void testGetNotificationAttributesWithCache() throws Exception {
        String entityType = "TestEntity";
        String[] expectedAttributes = {"attr1", "attr2"};

        when(configuration.getStringArray(
                "atlas.notification.entity.TestEntity.attributes.include"))
                .thenReturn(expectedAttributes);

        Method getNotificationAttributesMethod = NotificationEntityChangeListener.class
                .getDeclaredMethod("getNotificationAttributes", String.class);
        getNotificationAttributesMethod.setAccessible(true);

        // First call - should read from configuration
        @SuppressWarnings("unchecked")
        List<String> result1 = (List<String>) getNotificationAttributesMethod.invoke(listener, entityType);

        // Second call - should read from cache
        @SuppressWarnings("unchecked")
        List<String> result2 = (List<String>) getNotificationAttributesMethod.invoke(listener, entityType);

        assertEquals(result1, result2);

        // Configuration should be called only once
        verify(configuration, times(1)).getStringArray(anyString());
    }

    @Test
    public void testGetNotificationAttributesNullConfiguration() throws Exception {
        // Create listener with null configuration
        NotificationEntityChangeListener nullConfigListener =
                new NotificationEntityChangeListener(notificationInterface, typeRegistry, null);

        Method getNotificationAttributesMethod = NotificationEntityChangeListener.class
                .getDeclaredMethod("getNotificationAttributes", String.class);
        getNotificationAttributesMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) getNotificationAttributesMethod.invoke(nullConfigListener, "TestEntity");

        assertNull(result);
    }

    @Test
    public void testGetNotificationAttributesNullReturn() throws Exception {
        String entityType = "TestEntity";

        when(configuration.getStringArray(anyString())).thenReturn(null);

        Method getNotificationAttributesMethod = NotificationEntityChangeListener.class
                .getDeclaredMethod("getNotificationAttributes", String.class);
        getNotificationAttributesMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) getNotificationAttributesMethod.invoke(listener, entityType);

        assertNull(result);
    }

    @Test
    public void testNotifyOfEntityEventWithAttributeFiltering() throws Exception {
        Referenceable entity = createTestEntity(ENTITY_ID, ENTITY_TYPE);
        entity.getValuesMap().put("allowedAttr", "value1");
        entity.getValuesMap().put("filteredAttr", "value2");

        String[] allowedAttributes = {"allowedAttr"};
        when(configuration.getStringArray(anyString())).thenReturn(allowedAttributes);

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);

            listener.onEntitiesAdded(Collections.singletonList(entity), false);

            verify(notificationSender).send(argThat(notifications -> {
                if (notifications.size() != 1) {
                    return false;
                }
                EntityNotificationV1 notification = notifications.get(0);
                Referenceable notifiedEntity = notification.getEntity();
                return notifiedEntity.getValuesMap().containsKey("allowedAttr") &&
                        !notifiedEntity.getValuesMap().containsKey("filteredAttr");
            }));
        }
    }

    // Exception Handling Tests

    @Test
    public void testOnEntitiesAddedWithException() throws Exception {
        Collection<Referenceable> entities = Collections.singletonList(createTestEntity(ENTITY_ID, ENTITY_TYPE));

        doThrow(new NotificationException(new RuntimeException("Test exception")))
                .when(notificationSender).send(anyList());

        try (MockedStatic<GraphHelper> graphHelperMock = mockStatic(GraphHelper.class)) {
            graphHelperMock.when(() -> GraphHelper.isInternalType(ENTITY_TYPE)).thenReturn(false);

            assertThrows(AtlasException.class, () -> listener.onEntitiesAdded(entities, false));
        }
    }

    // Utility Methods

    private Referenceable createTestEntity(String id, String typeName) {
        Map<String, Object> values = new HashMap<>();
        values.put("name", "test-entity-name");
        values.put("qualifiedName", "test-entity@cluster");

        return new Referenceable(id, typeName, values);
    }

    private Referenceable getEntity(String id, Struct... traits) {
        String typeName = "typeName";
        Map<String, Object> values = new HashMap<>();

        List<String> traitNames = new LinkedList<>();
        Map<String, Struct> traitMap = new HashMap<>();

        for (Struct trait : traits) {
            String traitName = trait.getTypeName();
            traitNames.add(traitName);
            traitMap.put(traitName, trait);
        }
        return new Referenceable(id, typeName, values, traitNames, traitMap);
    }

    /**
     * Custom assertion for lambda expressions in TestNG
     */
    private void assertDoesNotThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            fail("Expected no exception to be thrown, but got: " + e.getClass().getSimpleName() +
                    " with message: " + e.getMessage());
        }
    }
}
