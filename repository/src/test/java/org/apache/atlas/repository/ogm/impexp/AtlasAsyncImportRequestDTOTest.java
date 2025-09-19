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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.atlas.repository.ogm.impexp;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.ASYNC_IMPORT_TYPE_NAME;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.COMPLETED_TIME;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.IMPORT_DETAILS_PROPERTY;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.IMPORT_ID_PROPERTY;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.IMPORT_RESULT_PROPERTY;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.PROCESSING_START_TIME;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.RECEIVED_TIME_PROPERTY;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.REQUEST_ID_PROPERTY;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.STAGED_TIME_PROPERTY;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.START_ENTITY_POSITION_PROPERTY;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.STATUS_PROPERTY;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasAsyncImportRequestDTOTest {
    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasEntityType mockEntityType;

    private AtlasAsyncImportRequestDTO atlasAsyncImportRequestDTO;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mockTypeRegistry.getEntityTypeByName(ASYNC_IMPORT_TYPE_NAME)).thenReturn(mockEntityType);
        when(mockEntityType.createDefaultValue()).thenReturn(new AtlasEntity());
        atlasAsyncImportRequestDTO = new AtlasAsyncImportRequestDTO(mockTypeRegistry);
    }

    @Test
    public void testConstructor() {
        assertNotNull(atlasAsyncImportRequestDTO);
        assertEquals(atlasAsyncImportRequestDTO.getObjectType(), AtlasAsyncImportRequest.class);
        assertEquals(atlasAsyncImportRequestDTO.getEntityType(), mockEntityType);
    }

    @Test
    public void testFromEntityWithValidDataStandardJson() {
        // Given - using real JSON that AtlasType.fromJson can actually parse
        AtlasEntity entity = createTestAtlasEntity();
        // Create valid JSON strings that can be parsed by AtlasType.fromJson
        String jsonImportResult = "{\"userName\":\"testUser\",\"operationType\":\"EXPORT\"}";
        String jsonImportDetails = "{\"fileName\":\"test.zip\",\"transformers\":[]}";
        entity.setAttribute(IMPORT_RESULT_PROPERTY, jsonImportResult);
        entity.setAttribute(REQUEST_ID_PROPERTY, "test-request-id");
        entity.setAttribute(IMPORT_ID_PROPERTY, "test-import-id");
        entity.setAttribute(STATUS_PROPERTY, "STAGING");
        entity.setAttribute(START_ENTITY_POSITION_PROPERTY, "100");
        entity.setAttribute(IMPORT_DETAILS_PROPERTY, jsonImportDetails);
        entity.setAttribute(RECEIVED_TIME_PROPERTY, "1234567890");
        entity.setAttribute(STAGED_TIME_PROPERTY, "1234567900");
        entity.setAttribute(PROCESSING_START_TIME, "1234567910");
        entity.setAttribute(COMPLETED_TIME, "1234567920");

        // When - this will use the actual AtlasType.fromJson method
        AtlasAsyncImportRequest result = atlasAsyncImportRequestDTO.from(entity);

        // Then
        assertNotNull(result);
        assertEquals(result.getGuid(), "test-guid");
        assertTrue(result.getImportTrackingInfo().getRequestId().contains("test-import-id"));
        assertEquals(result.getImportId(), "test-import-id");
        assertEquals(result.getStatus(), AtlasAsyncImportRequest.ImportStatus.STAGING);
        assertEquals(result.getImportTrackingInfo().getStartEntityPosition(), 100);
        assertNotNull(result.getImportDetails());
        assertEquals(result.getReceivedTime(), 1234567890L);
        assertEquals(result.getStagedTime(), 1234567900L);
        assertEquals(result.getProcessingStartTime(), 1234567910L);
        assertEquals(result.getCompletedTime(), 1234567920L);
    }

    @Test
    public void testFromEntityWithEmptyImportResult() {
        // Given
        AtlasEntity entity = createTestAtlasEntity();
        entity.setAttribute(IMPORT_RESULT_PROPERTY, "");

        // When
        AtlasAsyncImportRequest result = atlasAsyncImportRequestDTO.from(entity);

        // Then
        assertNull(result);
    }

    @Test
    public void testFromEntityWithNullImportResult() {
        // Given
        AtlasEntity entity = createTestAtlasEntity();
        entity.setAttribute(IMPORT_RESULT_PROPERTY, null);

        // When
        AtlasAsyncImportRequest result = atlasAsyncImportRequestDTO.from(entity);

        // Then
        assertNull(result);
    }

    @Test
    public void testFromEntityWithExtInfo() {
        // Given
        AtlasEntity entity = createTestAtlasEntity();
        AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntityWithExtInfo(entity);
        String jsonImportResult = "{\"userName\":\"testUser\",\"operationType\":\"EXPORT\"}";
        entity.setAttribute(IMPORT_RESULT_PROPERTY, jsonImportResult);
        entity.setAttribute(REQUEST_ID_PROPERTY, "test-request-id");
        entity.setAttribute(IMPORT_ID_PROPERTY, "test-import-id");
        entity.setAttribute(STATUS_PROPERTY, "PROCESSING");
        entity.setAttribute(START_ENTITY_POSITION_PROPERTY, "50");
        entity.setAttribute(IMPORT_DETAILS_PROPERTY, "");
        entity.setAttribute(RECEIVED_TIME_PROPERTY, "9876543210");
        entity.setAttribute(STAGED_TIME_PROPERTY, "9876543220");
        entity.setAttribute(PROCESSING_START_TIME, "9876543230");
        entity.setAttribute(COMPLETED_TIME, "9876543240");

        // When
        AtlasAsyncImportRequest result = atlasAsyncImportRequestDTO.from(entityWithExtInfo);

        // Then
        assertNotNull(result);
        assertEquals(result.getGuid(), "test-guid");
        // Note: requestId gets overwritten by setImportId() in AtlasAsyncImportRequest
        assertTrue(result.getImportTrackingInfo().getRequestId().contains("test-import-id"));
        assertEquals(result.getImportId(), "test-import-id");
        assertEquals(result.getStatus(), AtlasAsyncImportRequest.ImportStatus.PROCESSING);
        assertEquals(result.getImportTrackingInfo().getStartEntityPosition(), 50);
        assertNull(result.getImportDetails());
        assertEquals(result.getReceivedTime(), 9876543210L);
        assertEquals(result.getStagedTime(), 9876543220L);
        assertEquals(result.getProcessingStartTime(), 9876543230L);
        assertEquals(result.getCompletedTime(), 9876543240L);
    }

    @Test
    public void testToEntity() throws AtlasBaseException {
        // Given
        AtlasAsyncImportRequest asyncImportRequest = createTestAsyncImportRequest();

        // When - this will use the actual AtlasType.toJson method
        AtlasEntity result = atlasAsyncImportRequestDTO.toEntity(asyncImportRequest);

        // Then
        assertNotNull(result);
        assertEquals(result.getGuid(), "test-guid");
        assertNotNull(result.getAttribute(REQUEST_ID_PROPERTY));
        assertTrue(result.getAttribute(REQUEST_ID_PROPERTY).toString().contains("test-import-id"));
        assertNotNull(result.getAttribute(IMPORT_RESULT_PROPERTY));
        assertEquals(result.getAttribute(IMPORT_ID_PROPERTY), "test-import-id");
        assertEquals(result.getAttribute(STATUS_PROPERTY), AtlasAsyncImportRequest.ImportStatus.STAGING);
        assertNotNull(result.getAttribute(IMPORT_DETAILS_PROPERTY));
        assertEquals(result.getAttribute(START_ENTITY_POSITION_PROPERTY), "100");
        assertEquals(result.getAttribute(RECEIVED_TIME_PROPERTY), "1234567890");
        assertEquals(result.getAttribute(STAGED_TIME_PROPERTY), "1234567900");
        assertEquals(result.getAttribute(PROCESSING_START_TIME), "1234567910");
        assertEquals(result.getAttribute(COMPLETED_TIME), "1234567920");
    }

    @Test
    public void testToEntityWithNullImportResult() throws AtlasBaseException {
        // Given
        AtlasAsyncImportRequest asyncImportRequest = createTestAsyncImportRequest();
        asyncImportRequest.setImportResult(null);

        // When
        AtlasEntity result = atlasAsyncImportRequestDTO.toEntity(asyncImportRequest);

        // Then
        assertNotNull(result);
        assertEquals(result.getGuid(), "test-guid");
        assertNotNull(result.getAttribute(REQUEST_ID_PROPERTY));
        assertTrue(result.getAttribute(REQUEST_ID_PROPERTY).toString().contains("test-import-id"));
        assertNull(result.getAttribute(IMPORT_RESULT_PROPERTY));
    }

    @Test
    public void testToEntityWithExtInfo() throws AtlasBaseException {
        // Given
        AtlasAsyncImportRequest asyncImportRequest = createTestAsyncImportRequest();

        // When
        AtlasEntityWithExtInfo result = atlasAsyncImportRequestDTO.toEntityWithExtInfo(asyncImportRequest);

        // Then
        assertNotNull(result);
        assertNotNull(result.getEntity());
        assertEquals(result.getEntity().getGuid(), "test-guid");
    }

    @Test
    public void testGetUniqueAttributes() {
        // Given
        AtlasAsyncImportRequest asyncImportRequest = createTestAsyncImportRequest();

        // When
        Map<String, Object> result = atlasAsyncImportRequestDTO.getUniqueAttributes(asyncImportRequest);

        // Then
        assertNotNull(result);
        assertEquals(result.size(), 1);
        assertNotNull(result.get(REQUEST_ID_PROPERTY));
        assertTrue(result.get(REQUEST_ID_PROPERTY).toString().contains("test-import-id"));
    }

    @Test
    public void testGetUniqueAttributesWithNullImportId() {
        // Given
        AtlasAsyncImportRequest asyncImportRequest = createTestAsyncImportRequest();
        asyncImportRequest.setImportId(null);

        // When
        Map<String, Object> result = atlasAsyncImportRequestDTO.getUniqueAttributes(asyncImportRequest);

        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testObjectToLongWithValidString() throws Exception {
        // Given
        Method objectToLongMethod = AtlasAsyncImportRequestDTO.class.getDeclaredMethod("objectToLong", Object.class);
        objectToLongMethod.setAccessible(true);

        // When
        Long result = (Long) objectToLongMethod.invoke(atlasAsyncImportRequestDTO, "1234567890");

        // Then
        assertEquals(result, Long.valueOf(1234567890L));
    }

    @Test
    public void testObjectToLongWithLongObject() throws Exception {
        // Given
        Method objectToLongMethod = AtlasAsyncImportRequestDTO.class.getDeclaredMethod("objectToLong", Object.class);
        objectToLongMethod.setAccessible(true);

        // When
        Long result = (Long) objectToLongMethod.invoke(atlasAsyncImportRequestDTO, 9876543210L);

        // Then
        assertEquals(result, Long.valueOf(9876543210L));
    }

    @Test
    public void testObjectToLongWithNull() throws Exception {
        // Given
        Method objectToLongMethod = AtlasAsyncImportRequestDTO.class.getDeclaredMethod("objectToLong", Object.class);
        objectToLongMethod.setAccessible(true);

        // When
        Long result = (Long) objectToLongMethod.invoke(atlasAsyncImportRequestDTO, (Object) null);

        // Then
        assertEquals(result, Long.valueOf(0L));
    }

    @Test
    public void testObjectToLongWithInvalidString() throws Exception {
        // Given
        Method objectToLongMethod = AtlasAsyncImportRequestDTO.class.getDeclaredMethod("objectToLong", Object.class);
        objectToLongMethod.setAccessible(true);

        // When/Then - Should handle NumberFormatException and return 0L
        try {
            Long result = (Long) objectToLongMethod.invoke(atlasAsyncImportRequestDTO, "invalid-number");
            assertEquals(result, Long.valueOf(0L));
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof NumberFormatException);
        }
    }

    @Test
    public void testConvertToValidJsonWithSimpleMap() {
        // Given
        String mapString = "{key1=value1, key2=value2}";

        // When
        String result = AtlasAsyncImportRequestDTO.convertToValidJson(mapString);

        // Then
        assertEquals(result, "{\"key1\":\"value1\",\"key2\":\"value2\"}");
    }

    @Test
    public void testConvertToValidJsonWithNumericValues() {
        // Given
        String mapString = "{count=123, percentage=45.67}";

        // When
        String result = AtlasAsyncImportRequestDTO.convertToValidJson(mapString);

        // Then
        assertEquals(result, "{\"count\":123,\"percentage\":45.67}");
    }

    @Test
    public void testConvertToValidJsonWithArrayValues() {
        // Given
        String mapString = "{items=[item1, item2, item3], numbers=[1, 2, 3]}";

        // When
        String result = AtlasAsyncImportRequestDTO.convertToValidJson(mapString);

        // Then
        assertEquals(result, "{\"items\":[\"item1\",\"item2\",\"item3\"],\"numbers\":[1,2,3]}");
    }

    @Test
    public void testConvertToValidJsonWithEmptyArray() {
        // Given
        String mapString = "{emptyList=[], nonEmpty=[test]}";

        // When
        String result = AtlasAsyncImportRequestDTO.convertToValidJson(mapString);

        // Then
        assertEquals(result, "{\"emptyList\":[],\"nonEmpty\":[\"test\"]}");
    }

    @Test
    public void testConvertToValidJsonWithoutBraces() {
        // Given
        String mapString = "key1=value1, key2=value2";

        // When
        String result = AtlasAsyncImportRequestDTO.convertToValidJson(mapString);

        // Then
        assertEquals(result, "{\"key1\":\"value1\",\"key2\":\"value2\"}");
    }

    @Test
    public void testIsNumericWithValidInteger() throws Exception {
        // Given
        Method isNumericMethod = AtlasAsyncImportRequestDTO.class.getDeclaredMethod("isNumeric", String.class);
        isNumericMethod.setAccessible(true);

        // When
        Boolean result = (Boolean) isNumericMethod.invoke(null, "123");

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsNumericWithValidDouble() throws Exception {
        // Given
        Method isNumericMethod = AtlasAsyncImportRequestDTO.class.getDeclaredMethod("isNumeric", String.class);
        isNumericMethod.setAccessible(true);

        // When
        Boolean result = (Boolean) isNumericMethod.invoke(null, "123.45");

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsNumericWithNegativeNumber() throws Exception {
        // Given
        Method isNumericMethod = AtlasAsyncImportRequestDTO.class.getDeclaredMethod("isNumeric", String.class);
        isNumericMethod.setAccessible(true);

        // When
        Boolean result = (Boolean) isNumericMethod.invoke(null, "-123.45");

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsNumericWithInvalidString() throws Exception {
        // Given
        Method isNumericMethod = AtlasAsyncImportRequestDTO.class.getDeclaredMethod("isNumeric", String.class);
        isNumericMethod.setAccessible(true);

        // When
        Boolean result = (Boolean) isNumericMethod.invoke(null, "not-a-number");

        // Then
        assertTrue(!result);
    }

    @Test
    public void testGetUniqueValueWithValidRequest() throws Exception {
        // Given
        Method getUniqueValueMethod = AtlasAsyncImportRequestDTO.class.getDeclaredMethod("getUniqueValue", AtlasAsyncImportRequest.class);
        getUniqueValueMethod.setAccessible(true);
        AtlasAsyncImportRequest asyncImportRequest = createTestAsyncImportRequest();

        // When
        String result = (String) getUniqueValueMethod.invoke(atlasAsyncImportRequestDTO, asyncImportRequest);

        // Then
        assertNotNull(result);
        assertTrue(result.contains("test-import-id"));
        assertTrue(result.contains("@"));
    }

    @Test
    public void testConvertToValidJsonWithMixedContent() {
        // Given
        String mapString = "{name=testName, count=42, active=true, items=[a, b, c], nested={inner=value}}";

        // When
        String result = AtlasAsyncImportRequestDTO.convertToValidJson(mapString);

        // Then
        assertNotNull(result);
        assertTrue(result.contains("\"name\":\"testName\""));
        assertTrue(result.contains("\"count\":42"));
        assertTrue(result.contains("\"active\":\"true\""));
        assertTrue(result.contains("\"items\":[\"a\",\"b\",\"c\"]"));
    }

    @Test
    public void testFromEntityWithNullValues() {
        // Given
        AtlasEntity entity = createTestAtlasEntity();
        String jsonImportResult = "{\"userName\":\"testUser\",\"operationType\":\"EXPORT\"}";
        entity.setAttribute(IMPORT_RESULT_PROPERTY, jsonImportResult);
        entity.setAttribute(REQUEST_ID_PROPERTY, "test-request-id");
        entity.setAttribute(IMPORT_ID_PROPERTY, "test-import-id");
        entity.setAttribute(STATUS_PROPERTY, "STAGING");
        entity.setAttribute(START_ENTITY_POSITION_PROPERTY, "100");
        entity.setAttribute(IMPORT_DETAILS_PROPERTY, null);
        entity.setAttribute(RECEIVED_TIME_PROPERTY, null);
        entity.setAttribute(STAGED_TIME_PROPERTY, null);
        entity.setAttribute(PROCESSING_START_TIME, null);
        entity.setAttribute(COMPLETED_TIME, null);

        // When
        AtlasAsyncImportRequest result = atlasAsyncImportRequestDTO.from(entity);

        // Then
        assertNotNull(result);
        assertEquals(result.getGuid(), "test-guid");
        assertNull(result.getImportDetails());
        assertEquals(result.getReceivedTime(), 0L);
        assertEquals(result.getStagedTime(), 0L);
        assertEquals(result.getProcessingStartTime(), 0L);
        assertEquals(result.getCompletedTime(), 0L);
    }

    private AtlasEntity createTestAtlasEntity() {
        AtlasEntity entity = new AtlasEntity();
        entity.setGuid("test-guid");
        entity.setTypeName(ASYNC_IMPORT_TYPE_NAME);
        return entity;
    }

    private AtlasAsyncImportRequest createTestAsyncImportRequest() {
        AtlasImportResult importResult = new AtlasImportResult();
        AtlasAsyncImportRequest asyncImportRequest = new AtlasAsyncImportRequest(importResult);
        asyncImportRequest.setGuid("test-guid");
        asyncImportRequest.setImportId("test-import-id");
        asyncImportRequest.setStatus(AtlasAsyncImportRequest.ImportStatus.STAGING);
        asyncImportRequest.getImportTrackingInfo().setStartEntityPosition(100);
        asyncImportRequest.setImportDetails(new AtlasAsyncImportRequest.ImportDetails());
        asyncImportRequest.setReceivedTime(1234567890L);
        asyncImportRequest.setStagedTime(1234567900L);
        asyncImportRequest.setProcessingStartTime(1234567910L);
        asyncImportRequest.setCompletedTime(1234567920L);
        return asyncImportRequest;
    }
}
