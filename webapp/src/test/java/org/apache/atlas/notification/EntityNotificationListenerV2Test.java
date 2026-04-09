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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasRelationshipHeader;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType.ENTITY_CREATE;
import static org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType.RELATIONSHIP_CREATE;
import static org.apache.atlas.repository.graph.GraphHelper.isInternalType;
import static org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever.CREATE_TIME;
import static org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever.DESCRIPTION;
import static org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever.NAME;
import static org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever.OWNER;
import static org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever.QUALIFIED_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EntityNotificationListenerV2Test {
    private static final String ENTITY_GUID = "entity-guid-123";
    private static final String ENTITY_TYPE = "DataSet";
    private static final String RELATIONSHIP_GUID = "relationship-guid-123";
    private static final String RELATIONSHIP_TYPE = "ProcessExecution";
    private static final String CLASSIFICATION_TYPE = "PII";
    private static final Long REQUEST_TIME = 12345L;
    @Mock
    private AtlasTypeRegistry typeRegistry;
    @Mock
    private NotificationInterface notificationInterface;
    @Mock
    private Configuration configuration;
    @Mock
    private EntityNotificationSender<EntityNotificationV2> notificationSender;
    @Mock
    private RequestContext requestContext;
    @Mock
    private MetricRecorder metricRecorder;
    @Mock
    private AtlasEntity entity;
    @Mock
    private AtlasRelationship relationship;
    @Mock
    private AtlasClassification classification;
    @Mock
    private AtlasEntityType entityType;
    @Mock
    private AtlasClassificationType classificationType;
    @Mock
    private AtlasClassificationType superClassificationType;
    @Mock
    private AtlasAttribute attribute;
    @Mock
    private AtlasAttributeDef attributeDef;
    @Mock
    private AtlasGlossaryTerm glossaryTerm;
    @Mock
    private AtlasRelatedObjectId relatedObjectId;
    private EntityNotificationListenerV2 listener;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Set up RequestContext in ThreadLocal
        setRequestContext();

        // Create listener instance with mocked notificationSender
        listener = new EntityNotificationListenerV2(typeRegistry, notificationInterface, configuration);
        setNotificationSender();

        // Setup common mocks
        setupCommonMocks();
    }

    @After
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
        Field notificationSenderField = EntityNotificationListenerV2.class.getDeclaredField("notificationSender");
        notificationSenderField.setAccessible(true);
        notificationSenderField.set(listener, notificationSender);
    }

    /**
     * Setup common mocks used across multiple tests
     */
    private void setupCommonMocks() {
        // RequestContext mocks
        when(requestContext.startMetricRecord(anyString())).thenReturn(metricRecorder);
        when(requestContext.getRequestTime()).thenReturn(REQUEST_TIME);
        when(requestContext.getAddedPropagations()).thenReturn(new HashMap<>());
        when(requestContext.getRemovedPropagations()).thenReturn(new HashMap<>());

        // Entity mocks
        when(entity.getGuid()).thenReturn(ENTITY_GUID);
        when(entity.getTypeName()).thenReturn(ENTITY_TYPE);
        when(entity.getStatus()).thenReturn(AtlasEntity.Status.ACTIVE);
        when(entity.getIsIncomplete()).thenReturn(false);
        when(entity.getAttribute(NAME)).thenReturn("test-entity");
        when(entity.getAttribute(QUALIFIED_NAME)).thenReturn("test-entity@cluster");
        when(entity.getAttribute(DESCRIPTION)).thenReturn("Test entity description");
        when(entity.getAttribute(OWNER)).thenReturn("test-owner");
        when(entity.getAttribute(CREATE_TIME)).thenReturn(System.currentTimeMillis());
        when(entity.getClassifications()).thenReturn(Collections.singletonList(classification));

        // Relationship mocks
        when(relationship.getGuid()).thenReturn(RELATIONSHIP_GUID);
        when(relationship.getTypeName()).thenReturn(RELATIONSHIP_TYPE);
        when(relationship.getStatus()).thenReturn(AtlasRelationship.Status.ACTIVE);
        when(relationship.getEnd1()).thenReturn(new AtlasObjectId(ENTITY_TYPE, ENTITY_GUID));
        when(relationship.getEnd2()).thenReturn(new AtlasObjectId(ENTITY_TYPE, "entity-guid-456"));

        // Classification mocks
        when(classification.getTypeName()).thenReturn(CLASSIFICATION_TYPE);
        when(classification.getEntityGuid()).thenReturn(ENTITY_GUID);
        when(classification.isPropagate()).thenReturn(true);
        when(classification.getAttributes()).thenReturn(new HashMap<>());

        // Type registry mocks
        when(typeRegistry.getEntityTypeByName(ENTITY_TYPE)).thenReturn(entityType);
        when(typeRegistry.getClassificationTypeByName(CLASSIFICATION_TYPE)).thenReturn(classificationType);
        when(typeRegistry.getClassificationTypeByName("super_classification")).thenReturn(superClassificationType);

        // Entity type mocks
        Map<String, AtlasAttribute> attributes = new HashMap<>();
        attributes.put("unique_attr", attribute);
        when(entityType.getAllAttributes()).thenReturn(attributes);

        // Attribute mocks
        when(attribute.getName()).thenReturn("unique_attr");
        when(attribute.getAttributeDef()).thenReturn(attributeDef);
        when(attributeDef.getIsUnique()).thenReturn(true);
        when(entity.getAttribute("unique_attr")).thenReturn("unique_value");

        // Classification type mocks
        when(classificationType.getAllSuperTypes()).thenReturn(Collections.singleton("super_classification"));
        Map<String, AtlasAttribute> classificationAttrs = new HashMap<>();
        when(superClassificationType.getAllAttributes()).thenReturn(new HashMap<>());
    }

    // Constructor Tests
    @Test
    public void testConstructor() throws Exception {
        EntityNotificationListenerV2 newListener = new EntityNotificationListenerV2(typeRegistry, notificationInterface, configuration);

        // Verify typeRegistry field
        Field typeRegistryField = EntityNotificationListenerV2.class.getDeclaredField("typeRegistry");
        typeRegistryField.setAccessible(true);
        assertEquals(typeRegistry, typeRegistryField.get(newListener));

        // Verify notificationSender field is created
        Field notificationSenderField = EntityNotificationListenerV2.class.getDeclaredField("notificationSender");
        notificationSenderField.setAccessible(true);
        assertNotNull(notificationSenderField.get(newListener));
    }

    // Entity Events Tests
    @Test
    public void testOnEntitiesAdded() throws AtlasBaseException, NotificationException {
        List<AtlasEntity> entities = Collections.singletonList(entity);

        listener.onEntitiesAdded(entities, false);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnEntitiesAddedWithInternalType() throws AtlasBaseException, NotificationException {
        when(entity.getTypeName()).thenReturn("__internal_type");
        List<AtlasEntity> entities = Collections.singletonList(entity);

        listener.onEntitiesAdded(entities, false);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        // Should not send notification for internal types
        verify(notificationSender, never()).send(anyList());
    }

    @Test
    public void testOnEntitiesUpdated() throws AtlasBaseException, NotificationException {
        List<AtlasEntity> entities = Collections.singletonList(entity);

        listener.onEntitiesUpdated(entities, false);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnEntitiesDeleted() throws AtlasBaseException, NotificationException {
        List<AtlasEntity> entities = Collections.singletonList(entity);

        listener.onEntitiesDeleted(entities, false);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnEntitiesPurged() {
        List<AtlasEntity> entities = Collections.singletonList(entity);

        listener.onEntitiesPurged(entities);

        // Should do nothing - no interactions expected
        verifyNoInteractions(notificationSender);
        verifyNoInteractions(requestContext);
    }

    // Classification Events Tests
    @Test
    public void testOnClassificationsAddedSingleEntity() throws AtlasBaseException, NotificationException {
        List<AtlasClassification> classifications = Collections.singletonList(classification);

        listener.onClassificationsAdded(entity, classifications);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnClassificationsAddedMultipleEntities() throws AtlasBaseException, NotificationException {
        List<AtlasEntity> entities = Collections.singletonList(entity);
        List<AtlasClassification> classifications = Collections.singletonList(classification);

        listener.onClassificationsAdded(entities, classifications);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnClassificationsUpdatedWithAddedPropagations() throws AtlasBaseException, NotificationException {
        Map<String, List<AtlasClassification>> addedPropagations = new HashMap<>();
        addedPropagations.put(ENTITY_GUID, Collections.singletonList(classification));
        when(requestContext.getAddedPropagations()).thenReturn(addedPropagations);

        List<AtlasClassification> classifications = Collections.singletonList(classification);

        listener.onClassificationsUpdated(entity, classifications);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnClassificationsUpdatedWithRemovedPropagations() throws AtlasBaseException, NotificationException {
        Map<String, List<AtlasClassification>> removedPropagations = new HashMap<>();
        removedPropagations.put(ENTITY_GUID, Collections.singletonList(classification));
        when(requestContext.getRemovedPropagations()).thenReturn(removedPropagations);

        List<AtlasClassification> classifications = Collections.singletonList(classification);

        listener.onClassificationsUpdated(entity, classifications);

        verify(requestContext, times(1)).getAddedPropagations();
        verify(requestContext, times(1)).getRemovedPropagations();
        // Should not send notification when entity has removed propagations but no added propagations
        verify(notificationSender, never()).send(anyList());
    }

    @Test
    public void testOnClassificationsUpdatedRegular() throws AtlasBaseException, NotificationException {
        List<AtlasClassification> classifications = Collections.singletonList(classification);

        listener.onClassificationsUpdated(entity, classifications);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnClassificationsDeletedSingleEntity() throws AtlasBaseException, NotificationException {
        List<AtlasClassification> classifications = Collections.singletonList(classification);

        listener.onClassificationsDeleted(entity, classifications);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnClassificationsDeletedMultipleEntities() throws AtlasBaseException, NotificationException {
        List<AtlasEntity> entities = Collections.singletonList(entity);
        List<AtlasClassification> classifications = Collections.singletonList(classification);

        listener.onClassificationsDeleted(entities, classifications);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    // Term Events Tests (Should do nothing)
    @Test
    public void testOnTermAdded() {
        List<AtlasRelatedObjectId> entities = Collections.singletonList(relatedObjectId);

        listener.onTermAdded(glossaryTerm, entities);

        verifyNoInteractions(notificationSender);
        verifyNoInteractions(requestContext);
    }

    @Test
    public void testOnTermDeleted() {
        List<AtlasRelatedObjectId> entities = Collections.singletonList(relatedObjectId);

        listener.onTermDeleted(glossaryTerm, entities);

        verifyNoInteractions(notificationSender);
        verifyNoInteractions(requestContext);
    }

    // Relationship Events Tests
    @Test
    public void testOnRelationshipsAdded() throws AtlasBaseException, NotificationException {
        List<AtlasRelationship> relationships = Collections.singletonList(relationship);

        listener.onRelationshipsAdded(relationships, false);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnRelationshipsAddedWithInternalType() throws AtlasBaseException, NotificationException {
        when(relationship.getTypeName()).thenReturn("__internal_relationship");
        List<AtlasRelationship> relationships = Collections.singletonList(relationship);

        listener.onRelationshipsAdded(relationships, false);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        // Should not send notification for internal types
        verify(notificationSender, never()).send(anyList());
    }

    @Test
    public void testOnRelationshipsUpdated() throws AtlasBaseException, NotificationException {
        List<AtlasRelationship> relationships = Collections.singletonList(relationship);

        listener.onRelationshipsUpdated(relationships, false);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnRelationshipsDeleted() throws AtlasBaseException, NotificationException {
        List<AtlasRelationship> relationships = Collections.singletonList(relationship);

        listener.onRelationshipsDeleted(relationships, false);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testOnRelationshipsPurged() {
        List<AtlasRelationship> relationships = Collections.singletonList(relationship);

        listener.onRelationshipsPurged(relationships);

        // Should do nothing - no interactions expected
        verifyNoInteractions(notificationSender);
        verifyNoInteractions(requestContext);
    }

    // Label and Business Attribute Events Tests (Should do nothing)
    @Test
    public void testOnLabelsAdded() {
        Set<String> labels = Collections.singleton("test-label");

        listener.onLabelsAdded(entity, labels);

        verifyNoInteractions(notificationSender);
        verifyNoInteractions(requestContext);
    }

    @Test
    public void testOnLabelsDeleted() {
        Set<String> labels = Collections.singleton("test-label");

        listener.onLabelsDeleted(entity, labels);

        verifyNoInteractions(notificationSender);
        verifyNoInteractions(requestContext);
    }

    @Test
    public void testOnBusinessAttributesUpdated() {
        Map<String, Map<String, Object>> attributes = new HashMap<>();

        listener.onBusinessAttributesUpdated(entity, attributes);

        verifyNoInteractions(notificationSender);
        verifyNoInteractions(requestContext);
    }

    // Private Method Tests using Reflection

    @Test
    public void testNotifyEntityEventsPrivateMethod() throws Exception {
        Method notifyEntityEvents = EntityNotificationListenerV2.class.getDeclaredMethod("notifyEntityEvents", List.class, OperationType.class);
        notifyEntityEvents.setAccessible(true);

        List<AtlasEntity> entities = Collections.singletonList(entity);

        notifyEntityEvents.invoke(listener, entities, ENTITY_CREATE);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testNotifyEntityEventsWithEmptyList() throws Exception {
        Method notifyEntityEvents = EntityNotificationListenerV2.class.getDeclaredMethod("notifyEntityEvents", List.class, OperationType.class);
        notifyEntityEvents.setAccessible(true);

        List<AtlasEntity> entities = Collections.emptyList();

        notifyEntityEvents.invoke(listener, entities, ENTITY_CREATE);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        // Should not send notification for empty list
        verify(notificationSender, never()).send(anyList());
    }

    @Test
    public void testNotifyRelationshipEventsPrivateMethod() throws Exception {
        Method notifyRelationshipEvents = EntityNotificationListenerV2.class.getDeclaredMethod("notifyRelationshipEvents", List.class, OperationType.class);
        notifyRelationshipEvents.setAccessible(true);

        List<AtlasRelationship> relationships = Collections.singletonList(relationship);

        notifyRelationshipEvents.invoke(listener, relationships, RELATIONSHIP_CREATE);

        verify(requestContext).startMetricRecord("entityNotification");
        verify(requestContext).endMetricRecord(metricRecorder);
        verify(notificationSender).send(anyList());
    }

    @Test
    public void testSendNotificationsPrivateMethod() throws Exception {
        Method sendNotifications = EntityNotificationListenerV2.class.getDeclaredMethod("sendNotifications", OperationType.class, List.class);
        sendNotifications.setAccessible(true);

        List<EntityNotificationV2> messages = Collections.singletonList(
                new EntityNotificationV2(new AtlasEntityHeader(ENTITY_TYPE, ENTITY_GUID, new HashMap<>()), ENTITY_CREATE, REQUEST_TIME));

        sendNotifications.invoke(listener, ENTITY_CREATE, messages);

        verify(notificationSender).send(messages);
    }

    @Test
    public void testSendNotificationsWithEmptyList() throws Exception {
        Method sendNotifications = EntityNotificationListenerV2.class.getDeclaredMethod("sendNotifications", OperationType.class, List.class);
        sendNotifications.setAccessible(true);

        List<EntityNotificationV2> messages = Collections.emptyList();

        sendNotifications.invoke(listener, ENTITY_CREATE, messages);

        // Should not call notificationSender for empty list
        verify(notificationSender, never()).send(anyList());
    }

    @Test
    public void testSendNotificationsWithException() throws Exception {
        Method sendNotifications = EntityNotificationListenerV2.class.getDeclaredMethod("sendNotifications", OperationType.class, List.class);
        sendNotifications.setAccessible(true);

        List<EntityNotificationV2> messages = Collections.singletonList(
                new EntityNotificationV2(new AtlasEntityHeader(ENTITY_TYPE, ENTITY_GUID, new HashMap<>()), ENTITY_CREATE, REQUEST_TIME));

        doThrow(new NotificationException(new RuntimeException("Test exception"))).when(notificationSender).send(anyList());

        try {
            sendNotifications.invoke(listener, ENTITY_CREATE, messages);
            fail("Expected AtlasBaseException");
        } catch (Exception e) {
            assertTrue("Should throw AtlasBaseException", e.getCause() instanceof AtlasBaseException);
            AtlasBaseException atlasException = (AtlasBaseException) e.getCause();
            assertEquals(AtlasErrorCode.ENTITY_NOTIFICATION_FAILED, atlasException.getAtlasErrorCode());
        }
    }

    @Test
    public void testToNotificationHeaderEntityPrivateMethod() throws Exception {
        Method toNotificationHeaderEntity = EntityNotificationListenerV2.class.getDeclaredMethod("toNotificationHeader", AtlasEntity.class);
        toNotificationHeaderEntity.setAccessible(true);

        AtlasEntityHeader result = (AtlasEntityHeader) toNotificationHeaderEntity.invoke(listener, entity);

        assertNotNull(result);
        assertEquals(ENTITY_TYPE, result.getTypeName());
        assertEquals(ENTITY_GUID, result.getGuid());
        assertEquals(AtlasEntity.Status.ACTIVE, result.getStatus());
        assertEquals(false, result.getIsIncomplete());
        assertEquals("test-entity", result.getDisplayText());
        assertEquals("unique_value", result.getAttribute("unique_attr"));
        assertNotNull(result.getClassifications());
        assertEquals(2, result.getClassifications().size()); // Original + super type
    }

    @Test
    public void testToNotificationHeaderEntityWithNullDisplayText() throws Exception {
        when(entity.getAttribute(NAME)).thenReturn(null);
        when(entity.getAttribute(QUALIFIED_NAME)).thenReturn(null);

        Method toNotificationHeaderEntity = EntityNotificationListenerV2.class.getDeclaredMethod("toNotificationHeader", AtlasEntity.class);
        toNotificationHeaderEntity.setAccessible(true);

        AtlasEntityHeader result = (AtlasEntityHeader) toNotificationHeaderEntity.invoke(listener, entity);

        assertNotNull(result);
        assertNull(result.getDisplayText());
    }

    @Test
    public void testToNotificationHeaderEntityWithNullEntityType() throws Exception {
        when(typeRegistry.getEntityTypeByName(ENTITY_TYPE)).thenReturn(null);

        Method toNotificationHeaderEntity = EntityNotificationListenerV2.class.getDeclaredMethod("toNotificationHeader", AtlasEntity.class);
        toNotificationHeaderEntity.setAccessible(true);

        AtlasEntityHeader result = (AtlasEntityHeader) toNotificationHeaderEntity.invoke(listener, entity);

        assertNotNull(result);
        assertEquals(ENTITY_TYPE, result.getTypeName());
        // Should not set attributes when entityType is null
        assertNull(result.getAttribute("unique_attr"));
    }

    @Test
    public void testToNotificationHeaderEntityWithEmptyClassifications() throws Exception {
        when(entity.getClassifications()).thenReturn(Collections.emptyList());

        Method toNotificationHeaderEntity = EntityNotificationListenerV2.class.getDeclaredMethod("toNotificationHeader", AtlasEntity.class);
        toNotificationHeaderEntity.setAccessible(true);

        AtlasEntityHeader result = (AtlasEntityHeader) toNotificationHeaderEntity.invoke(listener, entity);

        assertNotNull(result);
        assertNull(result.getClassifications());
    }

    @Test
    public void testToNotificationHeaderRelationshipPrivateMethod() throws Exception {
        Method toNotificationHeaderRelationship = EntityNotificationListenerV2.class.getDeclaredMethod("toNotificationHeader", AtlasRelationship.class);
        toNotificationHeaderRelationship.setAccessible(true);

        AtlasRelationshipHeader result = (AtlasRelationshipHeader) toNotificationHeaderRelationship.invoke(listener, relationship);

        assertNotNull(result);
        assertEquals(RELATIONSHIP_GUID, result.getGuid());
        assertEquals(RELATIONSHIP_TYPE, result.getTypeName());
    }

    @Test
    public void testSetAttributePrivateMethod() throws Exception {
        Method setAttribute = EntityNotificationListenerV2.class.getDeclaredMethod("setAttribute", AtlasEntityHeader.class, String.class, Object.class);
        setAttribute.setAccessible(true);

        AtlasEntityHeader header = new AtlasEntityHeader(ENTITY_TYPE, ENTITY_GUID, new HashMap<>());
        String attrName = "test_attr";
        String attrValue = "test_value";

        setAttribute.invoke(listener, header, attrName, attrValue);

        assertEquals(attrValue, header.getAttribute(attrName));
    }

    @Test
    public void testSetAttributeWithNullValue() throws Exception {
        Method setAttribute = EntityNotificationListenerV2.class.getDeclaredMethod("setAttribute", AtlasEntityHeader.class, String.class, Object.class);
        setAttribute.setAccessible(true);

        AtlasEntityHeader header = new AtlasEntityHeader(ENTITY_TYPE, ENTITY_GUID, new HashMap<>());
        String attrName = "test_attr";

        setAttribute.invoke(listener, header, attrName, null);

        // Should not set attribute when value is null
        assertNull(header.getAttribute(attrName));
    }

    @Test
    public void testGetAllClassificationsPrivateMethod() throws Exception {
        Method getAllClassifications = EntityNotificationListenerV2.class.getDeclaredMethod("getAllClassifications", List.class);
        getAllClassifications.setAccessible(true);

        List<AtlasClassification> input = Collections.singletonList(classification);

        @SuppressWarnings("unchecked")
        List<AtlasClassification> result = (List<AtlasClassification>) getAllClassifications.invoke(listener, input);

        assertNotNull(result);
        assertEquals(2, result.size()); // Original + super type
        assertEquals(CLASSIFICATION_TYPE, result.get(0).getTypeName());
        assertEquals("super_classification", result.get(1).getTypeName());
        assertEquals(ENTITY_GUID, result.get(1).getEntityGuid());
        assertEquals(true, result.get(1).isPropagate());
    }

    @Test
    public void testGetAllClassificationsWithEmptyList() throws Exception {
        Method getAllClassifications = EntityNotificationListenerV2.class.getDeclaredMethod("getAllClassifications", List.class);
        getAllClassifications.setAccessible(true);

        List<AtlasClassification> input = Collections.emptyList();

        @SuppressWarnings("unchecked")
        List<AtlasClassification> result = (List<AtlasClassification>) getAllClassifications.invoke(listener, input);

        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testGetAllClassificationsWithNullSuperTypes() throws Exception {
        when(classificationType.getAllSuperTypes()).thenReturn(null);

        Method getAllClassifications = EntityNotificationListenerV2.class.getDeclaredMethod("getAllClassifications", List.class);
        getAllClassifications.setAccessible(true);

        List<AtlasClassification> input = Collections.singletonList(classification);

        @SuppressWarnings("unchecked")
        List<AtlasClassification> result = (List<AtlasClassification>) getAllClassifications.invoke(listener, input);

        assertNotNull(result);
        assertEquals(1, result.size()); // Only original, no super types
        assertEquals(CLASSIFICATION_TYPE, result.get(0).getTypeName());
    }

    @Test
    public void testGetAllClassificationsWithClassificationAttributes() throws Exception {
        Map<String, Object> classificationAttrs = new HashMap<>();
        classificationAttrs.put("attr1", "value1");
        classificationAttrs.put("attr2", "value2");
        when(classification.getAttributes()).thenReturn(classificationAttrs);

        Map<String, AtlasAttribute> superTypeAttrs = new HashMap<>();
        superTypeAttrs.put("attr1", attribute);
        when(superClassificationType.getAllAttributes()).thenReturn(superTypeAttrs);

        Method getAllClassifications = EntityNotificationListenerV2.class.getDeclaredMethod("getAllClassifications", List.class);
        getAllClassifications.setAccessible(true);

        List<AtlasClassification> input = Collections.singletonList(classification);

        @SuppressWarnings("unchecked")
        List<AtlasClassification> result = (List<AtlasClassification>) getAllClassifications.invoke(listener, input);

        assertNotNull(result);
        assertEquals(2, result.size());

        AtlasClassification superTypeClassification = result.get(1);
        assertNotNull(superTypeClassification.getAttributes());
        assertEquals("value1", superTypeClassification.getAttributes().get("attr1"));
        // attr2 should not be present as it's not in super type attributes
        assertNull(superTypeClassification.getAttributes().get("attr2"));
    }

    // Edge Case Tests
    @Test
    public void testIsInternalTypeStatic() {
        // Test the static method behavior
        assertTrue("Internal type should return true", isInternalType("__AtlasAuditEntry"));
        assertTrue("Internal type should return true", isInternalType("__internal"));
        assertFalse("Regular type should return false", isInternalType("DataSet"));
        assertFalse("Regular type should return false", isInternalType("Table"));
    }

    @Test
    public void testAttributeDefWithUniqueAndIncludeInNotification() throws Exception {
        // Test with different combinations of isUnique and includeInNotification
        AtlasAttribute uniqueAttr = mock(AtlasAttribute.class);
        AtlasAttributeDef uniqueAttrDef = mock(AtlasAttributeDef.class);
        when(uniqueAttr.getName()).thenReturn("unique_only_attr");
        when(uniqueAttr.getAttributeDef()).thenReturn(uniqueAttrDef);
        when(uniqueAttrDef.getIsUnique()).thenReturn(true);
        when(entity.getAttribute("unique_only_attr")).thenReturn("unique_only_value");

        AtlasAttribute includeAttr = mock(AtlasAttribute.class);
        AtlasAttributeDef includeAttrDef = mock(AtlasAttributeDef.class);
        when(includeAttr.getName()).thenReturn("include_only_attr");
        when(includeAttr.getAttributeDef()).thenReturn(includeAttrDef);
        when(includeAttrDef.getIsUnique()).thenReturn(false);
        when(includeAttrDef.getIncludeInNotification()).thenReturn(true);
        when(entity.getAttribute("include_only_attr")).thenReturn("include_only_value");

        Map<String, AtlasAttribute> attributes = new HashMap<>();
        attributes.put("unique_attr", attribute);
        attributes.put("unique_only_attr", uniqueAttr);
        attributes.put("include_only_attr", includeAttr);
        when(entityType.getAllAttributes()).thenReturn(attributes);

        Method toNotificationHeaderEntity = EntityNotificationListenerV2.class.getDeclaredMethod("toNotificationHeader", AtlasEntity.class);
        toNotificationHeaderEntity.setAccessible(true);

        AtlasEntityHeader result = (AtlasEntityHeader) toNotificationHeaderEntity.invoke(listener, entity);

        assertNotNull(result);
        assertEquals("unique_value", result.getAttribute("unique_attr"));
        assertEquals("unique_only_value", result.getAttribute("unique_only_attr"));
        assertEquals("include_only_value", result.getAttribute("include_only_attr"));
    }

    @Test
    public void testAttributeWithNullValue() throws Exception {
        when(entity.getAttribute("unique_attr")).thenReturn(null);

        Method toNotificationHeaderEntity = EntityNotificationListenerV2.class.getDeclaredMethod("toNotificationHeader", AtlasEntity.class);
        toNotificationHeaderEntity.setAccessible(true);

        AtlasEntityHeader result = (AtlasEntityHeader) toNotificationHeaderEntity.invoke(listener, entity);

        assertNotNull(result);
        // Should not set attribute when value is null
        assertNull(result.getAttribute("unique_attr"));
    }
}
