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
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasObjectIdConverterTest {
    @Mock
    private AtlasFormatConverters mockConverterRegistry;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasEntityType mockEntityType;

    @Mock
    private AtlasFormatConverter.ConverterContext mockContext;

    private AtlasObjectIdConverter converter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new AtlasObjectIdConverter(mockConverterRegistry, mockTypeRegistry);
        // Set up common mock behavior
        when(mockEntityType.getTypeCategory()).thenReturn(TypeCategory.ENTITY);
    }

    @Test
    public void testGetTypeCategory() {
        assertEquals(converter.getTypeCategory(), TypeCategory.OBJECT_ID_TYPE);
    }

    @Test
    public void testIsValidValueV1WithNull() {
        // When
        boolean result = converter.isValidValueV1(null, mockEntityType);

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithId() {
        // Given
        Id id = new Id("test-guid", 1, "TestType");

        // When
        boolean result = converter.isValidValueV1(id, mockEntityType);

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithReferenceable() {
        // Given
        Referenceable referenceable = new Referenceable("TestType");

        // When
        boolean result = converter.isValidValueV1(referenceable, mockEntityType);

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithInvalidType() {
        // Given
        String invalidObject = "notAnIdOrReferenceable";

        // When
        boolean result = converter.isValidValueV1(invalidObject, mockEntityType);

        // Then
        assertFalse(result);
    }

    @Test
    public void testFromV1ToV2WithNull() throws AtlasBaseException {
        // When
        Object result = converter.fromV1ToV2(null, mockEntityType, mockContext);

        // Then
        assertNull(result);
    }

    @Test
    public void testFromV1ToV2WithId() throws AtlasBaseException {
        // Given
        Id id = new Id("test-guid", 1, "TestType");

        // When
        Object result = converter.fromV1ToV2(id, mockEntityType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result instanceof AtlasObjectId);
        AtlasObjectId objectId = (AtlasObjectId) result;
        assertEquals(objectId.getGuid(), "test-guid");
        assertEquals(objectId.getTypeName(), "TestType");
    }

    @Test
    public void testFromV2ToV1WithNull() throws AtlasBaseException {
        // When
        Object result = converter.fromV2ToV1(null, mockEntityType, mockContext);

        // Then
        assertNull(result);
    }

    @Test
    public void testFromV2ToV1WithMap() throws AtlasBaseException {
        // Given
        Map<String, Object> v2Map = new HashMap<>();
        v2Map.put(AtlasObjectId.KEY_GUID, "test-guid");
        v2Map.put(AtlasObjectId.KEY_TYPENAME, "TestType");

        // When
        Object result = converter.fromV2ToV1(v2Map, mockEntityType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result instanceof Id);
        Id id = (Id) result;
        assertEquals(id.getId(), "test-guid");
        assertEquals(id.getTypeName(), "TestType");
        assertEquals(id.getVersion(), 0);
    }

    @Test
    public void testFromV2ToV1WithAtlasObjectId() throws AtlasBaseException {
        // Given
        AtlasObjectId objectId = new AtlasObjectId("test-guid", "TestType");

        // When
        Object result = converter.fromV2ToV1(objectId, mockEntityType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result instanceof Id);
        Id id = (Id) result;
        assertEquals(id.getId(), "test-guid");
        assertEquals(id.getTypeName(), "TestType");
        assertEquals(id.getVersion(), 0);
    }

    @Test
    public void testFromV2ToV1WithAtlasEntity() throws AtlasBaseException {
        // Given
        AtlasEntity entity = new AtlasEntity("TestType");
        entity.setGuid("test-guid");
        entity.setVersion(5L);

        // When
        Object result = converter.fromV2ToV1(entity, mockEntityType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result instanceof Id);
        Id id = (Id) result;
        assertEquals(id.getId(), "test-guid");
        assertEquals(id.getTypeName(), "TestType");
        assertEquals(id.getVersion(), 5);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV2ToV1WithInvalidType() throws AtlasBaseException {
        // Given
        String invalidObject = "notAnObjectId";

        // When/Then
        converter.fromV2ToV1(invalidObject, mockEntityType, mockContext);
    }
}
