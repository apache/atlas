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
package org.apache.atlas.repository.converters;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.instance.AtlasSystemAttributes;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasEntityFormatConverterTest {
    @Mock
    private AtlasFormatConverters mockConverterRegistry;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasEntityType mockEntityType;

    @Mock
    private AtlasClassificationType mockClassificationType;

    @Mock
    private AtlasFormatConverter mockClassificationConverter;

    @Mock
    private AtlasFormatConverter.ConverterContext mockContext;

    private AtlasEntityFormatConverter converter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new AtlasEntityFormatConverter(mockConverterRegistry, mockTypeRegistry);
        // Set up basic mocks
        when(mockEntityType.getTypeName()).thenReturn("TestType");
        when(mockClassificationType.getTypeName()).thenReturn("TestClassification");
        when(mockClassificationType.getTypeCategory()).thenReturn(TypeCategory.CLASSIFICATION);
    }

    @Test
    public void testGetTypeCategory() {
        assertEquals(converter.getTypeCategory(), TypeCategory.ENTITY);
    }

    @Test
    public void testIsValidValueV1WithNull() {
        assertTrue(converter.isValidValueV1(null, mockEntityType));
    }

    @Test
    public void testIsValidValueV1WithId() {
        Id id = new Id("test-guid", 1, "TestType");
        assertTrue(converter.isValidValueV1(id, mockEntityType));
    }

    @Test
    public void testIsValidValueV1WithReferenceable() {
        Referenceable referenceable = new Referenceable("TestType");
        assertTrue(converter.isValidValueV1(referenceable, mockEntityType));
    }

    @Test
    public void testIsValidValueV1WithInvalidType() {
        String invalidObject = "notAnIdOrReferenceable";
        assertFalse(converter.isValidValueV1(invalidObject, mockEntityType));
    }

    @Test
    public void testFromV1ToV2WithNull() throws AtlasBaseException {
        AtlasEntity result = converter.fromV1ToV2(null, mockEntityType, mockContext);
        assertNull(result);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV1ToV2WithInvalidType() throws AtlasBaseException {
        String invalidObject = "notAReferenceable";
        converter.fromV1ToV2(invalidObject, mockEntityType, mockContext);
    }

    @Test
    public void testFromV1ToV2WithReferenceableBasic() throws AtlasBaseException {
        // Given
        Id id = new Id("testGuid", 1, "TestType");
        id.setState(Id.EntityState.ACTIVE);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "testEntity");
        Referenceable referenceable = new Referenceable("TestType", attributes);
        referenceable.setId(id);
        AtlasSystemAttributes systemAttrs = new AtlasSystemAttributes();
        systemAttrs.setCreatedBy("user1");
        systemAttrs.setModifiedBy("user2");
        systemAttrs.setCreatedTime(new Date(1000L));
        systemAttrs.setModifiedTime(new Date(2000L));
        referenceable.setSystemAttributes(systemAttrs);
        // When
        when(mockContext.entityExists("testGuid")).thenReturn(false);
        when(mockEntityType.getTypeName()).thenReturn("TestType");

        AtlasEntity result = converter.fromV1ToV2(referenceable, mockEntityType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.getGuid(), "testGuid");
        assertEquals(result.getTypeName(), "TestType");
        assertEquals(result.getStatus(), AtlasEntity.Status.ACTIVE);
        assertEquals(result.getCreatedBy(), "user1");
        assertEquals(result.getUpdatedBy(), "user2");
        assertEquals(result.getCreateTime(), new Date(1000L));
        assertEquals(result.getUpdateTime(), new Date(2000L));
        assertEquals(result.getVersion(), Long.valueOf(1));
    }

    @Test
    public void testFromV1ToV2WithExistingEntity() throws AtlasBaseException {
        // Given
        Id id = new Id("existingGuid", 1, "TestType");
        Referenceable referenceable = new Referenceable("TestType", new HashMap<>());
        referenceable.setId(id);
        // When
        when(mockContext.entityExists("existingGuid")).thenReturn(true);
        AtlasEntity result = converter.fromV1ToV2(referenceable, mockEntityType, mockContext);
        // Then
        assertNull(result);
    }

    @Test
    public void testFromV1ToV2WithDeletedState() throws AtlasBaseException {
        // Given
        Id id = new Id("testGuid", 1, "TestType");
        id.setState(Id.EntityState.DELETED);
        Referenceable referenceable = new Referenceable("TestType", new HashMap<>());
        referenceable.setId(id);
        // When
        when(mockContext.entityExists("testGuid")).thenReturn(false);
        AtlasEntity result = converter.fromV1ToV2(referenceable, mockEntityType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.getStatus(), AtlasEntity.Status.DELETED);
    }

    @Test
    public void testFromV2ToV1WithNull() throws AtlasBaseException {
        Object result = converter.fromV2ToV1(null, mockEntityType, mockContext);
        assertNull(result);
    }

    @Test
    public void testFromV2ToV1WithMapWithoutAttributes() throws AtlasBaseException {
        // Given
        Map<String, Object> entityMap = new HashMap<>();
        entityMap.put(AtlasObjectId.KEY_GUID, "testGuid");
        // When
        Object result = converter.fromV2ToV1(entityMap, mockEntityType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof Id);
        Id id = (Id) result;
        assertEquals(id.getId(), "testGuid");
        assertEquals(id.getTypeName(), "TestType");
    }

    @Test
    public void testFromV2ToV1WithMapWithAttributes() throws AtlasBaseException {
        // Given
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "testEntity");
        Map<String, Object> entityMap = new HashMap<>();
        entityMap.put(AtlasObjectId.KEY_GUID, "testGuid");
        entityMap.put("attributes", attributes);
        // When
        Object result = converter.fromV2ToV1(entityMap, mockEntityType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof Referenceable);
        Referenceable ref = (Referenceable) result;
        assertEquals(ref.getId().getId(), "testGuid");
        assertEquals(ref.getTypeName(), "TestType");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV2ToV1WithMapWithoutGuid() throws AtlasBaseException {
        // Given
        Map<String, Object> entityMap = new HashMap<>();
        entityMap.put("attributes", new HashMap<>());
        // When/Then
        converter.fromV2ToV1(entityMap, mockEntityType, mockContext);
    }

    @Test
    public void testFromV2ToV1WithAtlasEntity() throws AtlasBaseException {
        // Given
        AtlasEntity entity = new AtlasEntity("TestType");
        entity.setGuid("testGuid");
        entity.setStatus(AtlasEntity.Status.ACTIVE);
        entity.setCreatedBy("user1");
        entity.setUpdatedBy("user2");
        entity.setCreateTime(new Date(1000L));
        entity.setUpdateTime(new Date(2000L));
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "testEntity");
        entity.setAttributes(attributes);
        // When
        Object result = converter.fromV2ToV1(entity, mockEntityType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof Referenceable);
        Referenceable ref = (Referenceable) result;
        assertEquals(ref.getId().getId(), "testGuid");
        assertEquals(ref.getTypeName(), "TestType");
        assertEquals(ref.getId().getState().name(), "ACTIVE");
        assertNotNull(ref.getSystemAttributes());
        assertEquals(ref.getSystemAttributes().getCreatedBy(), "user1");
        assertEquals(ref.getSystemAttributes().getModifiedBy(), "user2");
    }

    @Test
    public void testFromV2ToV1WithAtlasEntityWithNullStatus() throws AtlasBaseException {
        // Given
        AtlasEntity entity = new AtlasEntity("TestType");
        entity.setGuid("testGuid");
        entity.setStatus(null); // null status should default to ACTIVE
        // When
        Object result = converter.fromV2ToV1(entity, mockEntityType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof Referenceable);
        Referenceable ref = (Referenceable) result;
        assertEquals(ref.getId().getState().name(), "ACTIVE");
    }

    @Test
    public void testFromV2ToV1WithAtlasObjectId() throws AtlasBaseException {
        // Given
        AtlasObjectId objectId = new AtlasObjectId("testGuid", "TestType");
        AtlasEntity entity = new AtlasEntity("TestType");
        entity.setGuid("testGuid");
        AtlasType entityType = mockEntityType;
        // When
        when(mockContext.getById("testGuid")).thenReturn(entity);
        when(mockTypeRegistry.getType("TestType")).thenReturn(entityType);
        Object result = converter.fromV2ToV1(objectId, mockEntityType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof Referenceable);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV2ToV1WithAtlasObjectIdNotFound() throws AtlasBaseException {
        // Given
        AtlasObjectId objectId = new AtlasObjectId("nonExistentGuid", "TestType");
        // When
        when(mockContext.getById("nonExistentGuid")).thenReturn(null);
        // Then
        converter.fromV2ToV1(objectId, mockEntityType, mockContext);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV2ToV1WithInvalidType() throws AtlasBaseException {
        String invalidObject = "notAnEntityOrMap";
        converter.fromV2ToV1(invalidObject, mockEntityType, mockContext);
    }

    @Test
    public void testConvertStatePrivateMethod() throws Exception {
        // Test the private convertState method using reflection
        Method convertState = AtlasEntityFormatConverter.class.getDeclaredMethod("convertState", Id.EntityState.class);
        convertState.setAccessible(true);
        // Test with DELETED state
        AtlasEntity.Status deletedStatus = (AtlasEntity.Status) convertState.invoke(converter, Id.EntityState.DELETED);
        assertEquals(deletedStatus, AtlasEntity.Status.DELETED);
        // Test with ACTIVE state
        AtlasEntity.Status activeStatus = (AtlasEntity.Status) convertState.invoke(converter, Id.EntityState.ACTIVE);
        assertEquals(activeStatus, AtlasEntity.Status.ACTIVE);
        // Test with null state
        AtlasEntity.Status nullStatus = (AtlasEntity.Status) convertState.invoke(converter, (Object) null);
        assertEquals(nullStatus, AtlasEntity.Status.ACTIVE);
    }

    @Test
    public void testConstructorInheritance() {
        // Verify the constructor properly calls parent with correct parameters
        AtlasEntityFormatConverter testConverter = new AtlasEntityFormatConverter(mockConverterRegistry, mockTypeRegistry);
        assertNotNull(testConverter);
        assertEquals(testConverter.getTypeCategory(), TypeCategory.ENTITY);
        assertTrue(testConverter instanceof AtlasStructFormatConverter);
    }

    @Test
    public void testDebugLogging() {
        // Test that debug logging works correctly (testing code path coverage)
        String debugValue = "debugTestValue";
        boolean result = converter.isValidValueV1(debugValue, mockEntityType);
        assertFalse(result); // Should be false for String type
    }
}
