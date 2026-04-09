/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.atlas.web.rest;

import com.sun.jersey.core.header.FormDataContentDisposition;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.bulkimport.BulkImportResponse;
import org.apache.atlas.common.TestUtility;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.ClassificationAssociateRequest;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.FileUtils;
import org.apache.atlas.web.util.Servlets;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.common.TestUtility.buildAndGetMockServletRequest;
import static org.apache.atlas.common.TestUtility.generateString;
import static org.apache.atlas.web.rest.EntityREST.PREFIX_ATTR;
import static org.apache.atlas.web.rest.EntityREST.PREFIX_ATTR_;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class EntityRESTTest {
    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasEntityStore mockEntitiesStore;

    @Mock
    private EntityAuditRepository mockAuditRepository;

    @Mock
    private AtlasInstanceConverter mockInstanceConverter;

    @InjectMocks
    private EntityREST entityREST;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        entityREST = new EntityREST(mockTypeRegistry, mockEntitiesStore, mockAuditRepository, mockInstanceConverter);
    }

    @Test
    public void testGetById_Success() throws AtlasBaseException {
        String guid = "test-guid";
        boolean minExtInfo = false;
        boolean ignoreRelationships = false;

        AtlasEntityWithExtInfo expectedResult = new AtlasEntityWithExtInfo();
        when(mockEntitiesStore.getById(guid, minExtInfo, ignoreRelationships)).thenReturn(expectedResult);

        AtlasEntityWithExtInfo actualResult = entityREST.getById(guid, minExtInfo, ignoreRelationships);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockEntitiesStore, times(1)).getById(guid, minExtInfo, ignoreRelationships);
    }

    @Test
    public void testGetById_GuidNotFound_ThrowsException() throws AtlasBaseException {
        String guid = "invalid-guid";
        boolean minExtInfo = false;
        boolean ignoreRelationships = false;

        when(mockEntitiesStore.getById(guid, minExtInfo, ignoreRelationships))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid));

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.getById(guid, minExtInfo, ignoreRelationships));

        verify(mockEntitiesStore, times(1)).getById(guid, minExtInfo, ignoreRelationships);

        TestUtility.assertGUIDNotFoundException(exception, guid);
    }

    @Test
    public void testGetHeaderById_Success() throws AtlasBaseException {
        String guid = "valid-guid";
        AtlasEntityHeader expectedEntityHeader = new AtlasEntityHeader();
        when(mockEntitiesStore.getHeaderById(guid)).thenReturn(expectedEntityHeader);
        AtlasEntityHeader actualEntityHeader = entityREST.getHeaderById(guid);
        assertNotNull(actualEntityHeader);
        assertEquals(actualEntityHeader, expectedEntityHeader);
    }

    @Test
    public void testGetHeaderById_InvalidGuid_ThrowsException() throws AtlasBaseException {
        String guid = "invalid-guid";
        when(mockEntitiesStore.getHeaderById(guid)).thenThrow(new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid));

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.getHeaderById(guid));

        TestUtility.assertGUIDNotFoundException(exception, guid);
    }

    @Test
    public void testGetEntityHeaderByUniqueAttributes_Success() throws AtlasBaseException {
        String typeName = "typeName";
        Map<String, String[]> attributes = new HashMap<>();
        attributes.put(PREFIX_ATTR + "qualifiedName", new String[] {"test@domain.com"});

        AtlasEntityHeader expectedResult = new AtlasEntityHeader();

        AtlasStructDef.AtlasAttributeDef mockAttributeDef = mock(AtlasStructDef.AtlasAttributeDef.class);
        when(mockAttributeDef.getIsUnique()).thenReturn(true);

        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockEntityType.getAttributeDef("qualifiedName")).thenReturn(mockAttributeDef);

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);

        when(mockEntitiesStore.getEntityHeaderByUniqueAttributes(any(), anyMap())).thenReturn(expectedResult);
        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        AtlasEntityHeader actualResult = entityREST.getEntityHeaderByUniqueAttributes(typeName, mockHttpServletRequest);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockEntitiesStore).getEntityHeaderByUniqueAttributes(any(), anyMap());
    }

    @Test
    public void testGetEntityHeaderByUniqueAttributes_TypeNameTooLong_ThrowsException() {
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.getEntityHeaderByUniqueAttributes(typeName, mockHttpServletRequest));

        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testGetEntityHeaderByUniqueAttributes_TypeNameInvalid_ThrowsException() throws AtlasBaseException {
        String typeName = "invalidTypeName";
        Map<String, String[]> attributes = new HashMap<>();
        attributes.put(PREFIX_ATTR + "qualifiedName", new String[] {"test@domain.com"});

        AtlasEntityType mockEntityType = null; // return null EntityType

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.getEntityHeaderByUniqueAttributes(typeName, mockHttpServletRequest));

        verify(mockEntitiesStore, never()).getEntityHeaderByUniqueAttributes(any(), anyMap());
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.TYPE_NAME_INVALID);
        assertEquals(exception.getMessage(), AtlasErrorCode.TYPE_NAME_INVALID.getFormattedErrorMessage(TypeCategory.ENTITY.name(), typeName));
    }

    @Test
    public void testGetEntityHeaderByUniqueAttributes_EmptyAttributes_ThrowsException() throws AtlasBaseException {
        String typeName = "invalidTypeName";
        Map<String, String[]> attributes = new HashMap<>();

        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockEntityType.getTypeName()).thenReturn(typeName);

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.getEntityHeaderByUniqueAttributes(typeName, mockHttpServletRequest));

        verify(mockEntitiesStore, never()).getEntityHeaderByUniqueAttributes(any(), anyMap());
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID);
        assertEquals(exception.getMessage(), AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID.getFormattedErrorMessage(typeName, ""));
    }

    @Test
    public void testGetEntityHeaderByUniqueAttributes_UniqueAttributeFalse_ThrowsException() throws AtlasBaseException {
        String typeName = "typeName";
        Map<String, String[]> attributes = new HashMap<>();
        attributes.put(PREFIX_ATTR + "qualifiedName", new String[] {"test@domain.com"});

        AtlasStructDef.AtlasAttributeDef mockAttributeDef = mock(AtlasStructDef.AtlasAttributeDef.class);
        when(mockAttributeDef.getIsUnique()).thenReturn(false); // return IsUnique : false

        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockEntityType.getTypeName()).thenReturn(typeName);
        when(mockEntityType.getAttributeDef("qualifiedName")).thenReturn(mockAttributeDef);

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.getEntityHeaderByUniqueAttributes(typeName, mockHttpServletRequest));

        verify(mockEntitiesStore, never()).getEntityHeaderByUniqueAttributes(any(), anyMap());
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID);
        assertEquals(exception.getMessage(), AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID.getFormattedErrorMessage(typeName, "qualifiedName"));
    }

    @Test
    public void testGetByUniqueAttributes_Success() throws Exception {
        String typeName = "MyEntityType";
        boolean minExtInfo = true;
        boolean ignoreRelationships = false;

        Map<String, String[]> attributes = new HashMap<>();
        attributes.put(PREFIX_ATTR + "qualifiedName", new String[] {"myEntity@domain.com"});

        AtlasEntityWithExtInfo expectedResult = new AtlasEntityWithExtInfo();

        AtlasStructDef.AtlasAttributeDef mockAttributeDef = mock(AtlasStructDef.AtlasAttributeDef.class);
        when(mockAttributeDef.getIsUnique()).thenReturn(true);

        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockEntityType.getAttributeDef("qualifiedName")).thenReturn(mockAttributeDef);

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        // Mock internal method calls
        when(mockEntitiesStore.getByUniqueAttributes(eq(mockEntityType), anyMap(), eq(minExtInfo),
                eq(ignoreRelationships))).thenReturn(expectedResult);

        // When
        AtlasEntityWithExtInfo actualResult = entityREST.getByUniqueAttributes(typeName, minExtInfo,
                ignoreRelationships, mockHttpServletRequest);

        // Then
        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);

        // Verify internal calls
        verify(mockEntitiesStore).getByUniqueAttributes(eq(mockEntityType), anyMap(), eq(minExtInfo),
                eq(ignoreRelationships));
    }

    @Test
    public void testGetByUniqueAttributes_TypeNameTooLong_ThrowsException() {
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        boolean minExtInfo = true;
        boolean ignoreRelationships = false;

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);
        // When
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.getByUniqueAttributes(typeName, minExtInfo,
                ignoreRelationships, mockHttpServletRequest));

        // Then
        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testPartialUpdateEntityByUniqueAttrs_Success() throws Exception {
        // Given
        String typeName = "MyEntityType";
        AtlasEntityWithExtInfo entityInfo = new AtlasEntityWithExtInfo();

        Map<String, String[]> uniqueAttributes = new HashMap<>();
        uniqueAttributes.put(PREFIX_ATTR + "qualifiedName", new String[] {"myEntity@domain"});

        Map<String, Object> processedUniqueAttributes = processAttributes(uniqueAttributes);
        EntityMutationResponse expectedResult = new EntityMutationResponse();

        AtlasStructDef.AtlasAttributeDef mockAttributeDef = mock(AtlasStructDef.AtlasAttributeDef.class);
        when(mockAttributeDef.getIsUnique()).thenReturn(true);

        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockEntityType.getAttributeDef("qualifiedName")).thenReturn(mockAttributeDef);

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(uniqueAttributes);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        // Mock internal method calls
        when(mockEntitiesStore.updateByUniqueAttributes(mockEntityType, processedUniqueAttributes, entityInfo))
                .thenReturn(expectedResult);

        // When
        EntityMutationResponse actualResult = entityREST.partialUpdateEntityByUniqueAttrs(typeName,
                mockHttpServletRequest, entityInfo);

        // Then
        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        // Verify interactions
        verify(mockEntitiesStore).updateByUniqueAttributes(mockEntityType, processedUniqueAttributes, entityInfo);
    }

    @Test
    public void testPartialUpdateEntityByUniqueAttrs_TypeNameTooLong_ThrowsException() throws Exception {
        // Given
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        AtlasEntityWithExtInfo entityInfo = new AtlasEntityWithExtInfo();

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);

        // When
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.partialUpdateEntityByUniqueAttrs(typeName,
                mockHttpServletRequest, entityInfo));

        // Then
        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testDeleteByUniqueAttribute_Success() throws AtlasBaseException {
        // Arrange
        String typeName = "MyEntityType";
        Map<String, String[]> attributes = new HashMap<>();
        attributes.put(PREFIX_ATTR + "qualifiedName", new String[] {"someEntity@domain"});

        EntityMutationResponse expectedResult = new EntityMutationResponse();

        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        // Mock internal behavior
        when(mockEntitiesStore.deleteByUniqueAttributes(eq(mockEntityType), anyMap())).thenReturn(expectedResult);

        // Act
        EntityMutationResponse actualResult = entityREST.deleteByUniqueAttribute(typeName, mockHttpServletRequest);

        // Assert
        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);

        // Verify interactions
        verify(mockEntitiesStore).deleteByUniqueAttributes(eq(mockEntityType), anyMap());
    }

    @Test
    public void testDeleteByUniqueAttribute_TypeNameTooLong_ThrowsException() {
        // Arrange
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.deleteByUniqueAttribute(typeName, mockHttpServletRequest));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testPartialUpdateEntityAttrByGuid_Success() throws Exception {
        // Arrange
        String guid = "test-guid";
        String attrName = "qualifiedName";
        Object attrValue = "test-qualified-name";

        EntityMutationResponse expectedResult = new EntityMutationResponse();

        when(mockEntitiesStore.updateEntityAttributeByGuid(eq(guid), eq(attrName), eq(attrValue)))
                .thenReturn(expectedResult);

        // Act
        EntityMutationResponse actualResult = entityREST.partialUpdateEntityAttrByGuid(guid, attrName, attrValue);

        // Assert
        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);

        // Verify interaction
        verify(mockEntitiesStore, times(1)).updateEntityAttributeByGuid(eq(guid), eq(attrName), eq(attrValue));
    }

    @Test
    public void testPartialUpdateEntityAttrByGuid_GuidTooLong_ThrowsException() {
        // Arrange
        String guid = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        String attrName = "qualifiedName";
        Object attrValue = "test-qualified-name";

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.partialUpdateEntityAttrByGuid(guid, attrName, attrValue));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "guid");
    }

    @Test
    public void testPartialUpdateEntityAttrByGuid_AttributeNameTooLong_ThrowsException() {
        // Arrange
        String guid = "test-guid";
        String attrName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        Object attrValue = "test-qualified-name";

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.partialUpdateEntityAttrByGuid(guid, attrName, attrValue));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "name");
    }

    @Test
    public void testDeleteByGuid_Success() throws AtlasBaseException {
        // Arrange
        String guid = "test-guid";
        EntityMutationResponse expectedResponse = new EntityMutationResponse();

        when(mockEntitiesStore.deleteById(eq(guid))).thenReturn(expectedResponse);

        // Act
        EntityMutationResponse actualResponse = entityREST.deleteByGuid(guid);

        // Assert
        assertNotNull(actualResponse);
        assertEquals(actualResponse, expectedResponse);

        // Verify interaction
        verify(mockEntitiesStore, times(1)).deleteById(eq(guid));
    }

    @Test
    public void testDeleteByGuid_GuidTooLong_ThrowsException() {
        // Arrange
        String guid = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.deleteByGuid(guid));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "guid");
    }

    @Test
    public void testGetClassification_Success() throws AtlasBaseException {
        // Arrange
        String guid = "test-guid";
        String classificationName = "test-classification";
        AtlasClassification expectedClassification = new AtlasClassification(classificationName);

        // Mock entitiesStore behavior
        when(mockEntitiesStore.getClassification(guid, classificationName)).thenReturn(expectedClassification);
        AtlasClassificationType mockClassificationType = mock(AtlasClassificationType.class);
        when(mockTypeRegistry.getClassificationTypeByName(classificationName)).thenReturn(mockClassificationType);

        // Act
        AtlasClassification actualClassification = entityREST.getClassification(guid, classificationName);

        // Assert
        assertNotNull(actualClassification);
        assertEquals(actualClassification, expectedClassification);

        // Verify interaction
        verify(mockEntitiesStore, times(1)).getClassification(guid, classificationName);
    }

    @Test
    public void testGetClassification_GuidTooLong_ThrowException() throws AtlasBaseException {
        // Arrange
        String guid = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        String classificationName = "test-classification";

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.getClassification(guid, classificationName));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "guid");

        // Verify interaction
        verify(mockEntitiesStore, never()).getClassification(guid, classificationName);
    }

    @Test
    public void testGetClassification_ClassificationNameTooLong_ThrowsException() throws AtlasBaseException {
        // Arrange
        String guid = "test-guid";
        String classificationName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.getClassification(guid, classificationName));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "classificationName");

        // Verify interaction
        verify(mockEntitiesStore, never()).getClassification(guid, classificationName);
    }

    @Test
    public void testGetClassification_WithEmptyGuid_ThrowsException() throws AtlasBaseException {
        // Arrange
        String emptyGuid = "";
        String classificationName = "test-classification";

        // Act & Assert
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            entityREST.getClassification(emptyGuid, classificationName);
        });

        TestUtility.assertGUIDNotFoundException(exception, emptyGuid);

        verify(mockEntitiesStore, never()).getClassification(any(), any());
    }

    @Test
    public void testGetClassifications_Success() throws AtlasBaseException {
        // Arrange
        String guid = "valid-guid";
        List<AtlasClassification> mockClassifications = Arrays.asList(new AtlasClassification("classification1"),
                new AtlasClassification("classification2"));

        when(mockEntitiesStore.getClassifications(guid)).thenReturn(mockClassifications);

        // Act
        AtlasClassification.AtlasClassifications result = entityREST.getClassifications(guid);

        // Assert
        assertNotNull(result);
        assertEquals(result.getList().size(), 2);
        assertEquals(result.getList().get(0).getTypeName(), "classification1");

        // Verify interaction
        verify(mockEntitiesStore, times(1)).getClassifications(guid);
    }

    @Test
    public void testGetClassifications_GuidTooLong_ThrowsException() throws AtlasBaseException {
        // Arrange
        String guid = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.getClassifications(guid));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "guid");
    }

    @Test
    public void testGetClassifications_EmptyGuid_ThrowsException() throws AtlasBaseException {
        // Arrange
        String emptyGuid = "";

        // Act & Assert
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            entityREST.getClassifications(emptyGuid);
        });

        TestUtility.assertGUIDNotFoundException(exception, emptyGuid);

        // Ensure no call to store
        verify(mockEntitiesStore, never()).getClassifications(anyString());
    }

    @Test
    public void testAddClassificationsByUniqueAttribute_Success() throws Exception {
        // Arrange
        String typeName = "hive_table";
        Map<String, String[]> attributes = new HashMap<>();
        attributes.put(PREFIX_ATTR + "qualifiedName", new String[] {"db.table@cluster"});
        String guid = "test-guid";

        List<AtlasClassification> classifications = Collections.singletonList(new AtlasClassification("Sensitive"));

        // Mock behaviors
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);
        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(eq(mockEntityType), anyMap())).thenReturn(guid);
        doNothing().when(mockEntitiesStore).addClassifications(eq(guid), anyList());

        // Act
        entityREST.addClassificationsByUniqueAttribute(typeName, mockHttpServletRequest, classifications);

        // Assert
        verify(mockEntitiesStore, times(1)).addClassifications(eq(guid), eq(classifications));
    }

    @Test
    public void testAddClassificationsByUniqueAttribute_TypeNameTooLong_ThrowsException() {
        // Arrange
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');

        List<AtlasClassification> classifications = Collections.emptyList();

        // Mock behaviors
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.addClassificationsByUniqueAttribute(typeName, mockHttpServletRequest, classifications));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testAddClassificationsByUniqueAttribute_NotFound_ThrowsException() throws Exception {
        // Arrange
        String typeName = "hive_table";
        Map<String, String[]> attributes = new HashMap<>();
        attributes.put(PREFIX_ATTR + "qualifiedName", new String[] {"notfound@cluster"});
        List<AtlasClassification> classifications = Collections.singletonList(new AtlasClassification("Sensitive"));

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(eq(mockEntityType), anyMap())).thenReturn(null); // return null
        // guid

        // Act & Assert
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            entityREST.addClassificationsByUniqueAttribute(typeName, mockHttpServletRequest, classifications);
        });

        assertEquals(exception.getAtlasErrorCode().getErrorCode(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND.getErrorCode());
        assertEquals(exception.getMessage(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND.getFormattedErrorMessage(typeName,
                processAttributes(attributes).toString()));
    }

    @Test
    public void testAddClassifications_Success() throws AtlasBaseException {
        // Arrange
        String guid = "test-guid";
        List<AtlasClassification> classifications = Collections.singletonList(new AtlasClassification("PII"));

        // Act
        entityREST.addClassifications(guid, classifications);

        // Assert
        verify(mockEntitiesStore, times(1)).addClassifications(guid, classifications);
    }

    @Test
    public void testAddClassifications_GuidTooLong_ThrowsException() throws AtlasBaseException {
        // Arrange
        String guid = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        List<AtlasClassification> classifications = Collections.emptyList();

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.addClassifications(guid, classifications));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "guid");
        verify(mockEntitiesStore, never()).addClassifications(guid, classifications);
    }

    @Test
    public void testAddClassifications_EmptyGuid_ThrowsException() throws AtlasBaseException {
        // Arrange
        String guid = "";
        List<AtlasClassification> classifications = Collections.singletonList(new AtlasClassification("PII"));

        // Act & Assert
        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> {
            entityREST.addClassifications(guid, classifications);
        });

        TestUtility.assertGUIDNotFoundException(ex, guid);

        verify(mockEntitiesStore, never()).addClassifications(anyString(), anyList());
    }

    @Test
    public void testUpdateClassificationsByUniqueAttribute_Success() throws Exception {
        String typeName = "Employee";
        String guid = "1234";

        Map<String, String[]> attributes = new HashMap<>();
        attributes.put(PREFIX_ATTR + "qualifiedName", new String[] {"notfound@cluster"});

        List<AtlasClassification> classifications = Collections.singletonList(new AtlasClassification("Sensitive"));

        // Stubbing
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(mockEntityType, processAttributes(attributes)))
                .thenReturn(guid);

        // Call method
        entityREST.updateClassificationsByUniqueAttribute(typeName, mockHttpServletRequest, classifications);

        // Verify interaction
        verify(mockEntitiesStore).updateClassifications(guid, classifications);
    }

    @Test
    public void testUpdateClassificationsByUniqueAttribute_TypeNameTooLong_ThrowsException() throws Exception {
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');

        List<AtlasClassification> classifications = Collections.emptyList();
        // Stubbing
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);

        // Call method
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.updateClassificationsByUniqueAttribute(typeName, mockHttpServletRequest, classifications));

        // Verify interaction
        TestUtility.assertInvalidParamLength(exception, "typeName");
        verify(mockEntitiesStore, never()).updateClassifications(any(), eq(classifications));
    }

    @Test
    public void testUpdateClassificationsByUniqueAttribute_GuidNotFound_ThrowsException() throws Exception {
        String typeName = "Employee";
        Map<String, String[]> attributes = new HashMap<>();
        attributes.put("email", new String[] {"not.exists@example.com"});

        Map<String, Object> processAttributes = processAttributes(attributes);

        List<AtlasClassification> classifications = Collections.singletonList(new AtlasClassification("Sensitive"));

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(attributes);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(mockEntityType, processAttributes)).thenReturn(null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST
                .updateClassificationsByUniqueAttribute(typeName, mockHttpServletRequest, classifications));
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND);
        assertEquals(exception.getMessage(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND.getFormattedErrorMessage(typeName, processAttributes.toString()));
    }

    @Test
    public void testUpdateClassifications_Success() throws Exception {
        String guid = "5678";
        List<AtlasClassification> classifications = Collections.singletonList(new AtlasClassification("Sensitive"));
        entityREST.updateClassifications(guid, classifications);

        verify(mockEntitiesStore).updateClassifications(guid, classifications);
    }

    @Test
    public void testUpdateClassifications_GuidTooLong_ThrowsException() throws AtlasBaseException {
        String guid = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        List<AtlasClassification> classifications = Collections.emptyList();
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.updateClassifications(guid, classifications));

        TestUtility.assertInvalidParamLength(exception, "guid");
        verify(mockEntitiesStore, never()).updateClassifications(guid, classifications);
    }

    @Test
    public void testUpdateClassifications_EmptyGuid_ThrowsException() {
        String guid = "";
        List<AtlasClassification> classifications = Collections.singletonList(new AtlasClassification("Sensitive"));
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.updateClassifications(guid, classifications));

        TestUtility.assertGUIDNotFoundException(exception, guid);
    }

    @Test
    public void testDeleteClassificationByUniqueAttribute_Success() throws Exception {
        // Arrange
        String typeName = "TestType";
        String classificationName = "Confidential";
        String guid = "1234-guid";

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);

        // Mock behavior
        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(mockEntityType, Collections.emptyMap())).thenReturn(guid);

        // Act
        entityREST.deleteClassificationByUniqueAttribute(typeName, mockHttpServletRequest, classificationName);

        // Assert
        verify(mockEntitiesStore).deleteClassification(guid, classificationName);
    }

    @Test
    public void testDeleteClassificationByUniqueAttribute_GuidNotFound_ThrowsException() throws Exception {
        // Arrange
        String typeName = "TestType";
        String classificationName = "Confidential";

        Map<String, Object> mockAttributes = new HashMap<>();

        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);

        // Mock behavior
        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(mockEntityType, mockAttributes)).thenReturn(null);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST
                .deleteClassificationByUniqueAttribute(typeName, mockHttpServletRequest, classificationName));
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND);
        assertEquals(exception.getMessage(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND.getFormattedErrorMessage(typeName, mockAttributes.toString()));
    }

    @Test
    public void testDeleteClassificationByUniqueAttribute_TypeNameTooLong_ThrowsException() throws Exception {
        // Arrange
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        String classificationName = "Confidential";
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST
                .deleteClassificationByUniqueAttribute(typeName, mockHttpServletRequest, classificationName));

        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testDeleteClassificationByUniqueAttribute_ClassificationNameTooLong_ThrowsException() throws Exception {
        // Arrange
        String typeName = "typeName";
        String classificationName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST
                .deleteClassificationByUniqueAttribute(typeName, mockHttpServletRequest, classificationName));

        TestUtility.assertInvalidParamLength(exception, "classificationName");
    }

    @Test
    public void testDeleteClassification_Success() throws Exception {
        // Arrange
        String guid = "1234-guid";
        String classificationName = "Confidential";
        String associatedEntityGuid = "5678-guid";

        // Mock
        AtlasClassificationType mockClassficationType = mock(AtlasClassificationType.class);
        when(mockTypeRegistry.getClassificationTypeByName(classificationName)).thenReturn(mockClassficationType);

        // Act
        entityREST.deleteClassification(guid, classificationName, associatedEntityGuid);

        // Assert
        verify(mockEntitiesStore).deleteClassification(guid, classificationName, associatedEntityGuid);
    }

    @Test
    public void testDeleteClassification_EmptyGuid_ThrowsException() throws Exception {
        // Arrange
        String guid = "";
        String classificationName = "Confidential";
        String associatedEntityGuid = "5678-guid";

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.deleteClassification(guid, classificationName, associatedEntityGuid));

        TestUtility.assertGUIDNotFoundException(exception, guid);

        // Assert
        verify(mockEntitiesStore, never()).deleteClassification(guid, classificationName, associatedEntityGuid);
    }

    @Test
    public void testDeleteClassification_GuidTooLong_ThrowsException() throws Exception {
        // Arrange
        String guid = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        String classificationName = "Confidential";
        String associatedEntityGuid = "5678-guid";

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.deleteClassification(guid, classificationName, associatedEntityGuid));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "guid");
        verify(mockEntitiesStore, never()).deleteClassification(guid, classificationName, associatedEntityGuid);
    }

    @Test
    public void testDeleteClassification_ClassificationNameTooLong_ThrowsException() throws Exception {
        // Arrange
        String guid = "8923fsfse";
        String classificationName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        String associatedEntityGuid = "5678-guid";

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.deleteClassification(guid, classificationName, associatedEntityGuid));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "classificationName");
        verify(mockEntitiesStore, never()).deleteClassification(guid, classificationName, associatedEntityGuid);
    }

    @Test
    public void testDeleteClassification_AssociatedEntityGuidTooLong_ThrowsException() throws Exception {
        // Arrange
        String guid = "valid-guid";
        String classificationName = "Confidential";
        String associatedEntityGuid = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.deleteClassification(guid, classificationName, associatedEntityGuid));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "associatedEntityGuid");
        verify(mockEntitiesStore, never()).deleteClassification(guid, classificationName, associatedEntityGuid);
    }

    @Test
    public void testGetEntitiesByUniqueAttributes_Success() throws Exception {
        String typeName = "SampleType";
        boolean minExtInfo = false;
        boolean ignoreRelationships = false;

        List<Map<String, Object>> uniqAttributesList = new ArrayList<>();

        // Mock
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        AtlasEntity.AtlasEntitiesWithExtInfo expectedResult = mock(AtlasEntity.AtlasEntitiesWithExtInfo.class);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getEntitiesByUniqueAttributes(mockEntityType, uniqAttributesList, minExtInfo,
                ignoreRelationships)).thenReturn(expectedResult);

        // Act
        AtlasEntity.AtlasEntitiesWithExtInfo actualResult = entityREST.getEntitiesByUniqueAttributes(typeName,
                minExtInfo, ignoreRelationships, mockHttpServletRequest);

        // Assert
        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockEntitiesStore, times(1)).getEntitiesByUniqueAttributes(mockEntityType, uniqAttributesList,
                minExtInfo, ignoreRelationships);
    }

    @Test
    public void testGetEntitiesByUniqueAttributes_TypeNameTooLong_ThrowsException() {
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        boolean minExtInfo = false;
        boolean ignoreRelationships = false;

        // Mock
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST
                .getEntitiesByUniqueAttributes(typeName, minExtInfo, ignoreRelationships, mockHttpServletRequest));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testGetEntitiesByUniqueAttributes_InvalidEntityType_ThrowsException() throws Exception {
        String typeName = "InvalidType";
        boolean minExtInfo = false;
        boolean ignoreRelationships = false;

        // Mock
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(null);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(null); // return null entity type

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST
                .getEntitiesByUniqueAttributes(typeName, minExtInfo, ignoreRelationships, mockHttpServletRequest));

        // Assert
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.TYPE_NAME_INVALID);
        assertEquals(exception.getMessage(), AtlasErrorCode.TYPE_NAME_INVALID.getFormattedErrorMessage(TypeCategory.ENTITY.name(), typeName));
    }

    @Test
    public void testGetEntitiesByUniqueAttributes_AttributeIsUnique_False_ThrowsException() {
        String typeName = "SampleType";
        boolean minExtInfo = false;
        boolean ignoreRelationships = false;

        Map<String, String[]> uniqAttributes = new HashMap<>();
        uniqAttributes.put(PREFIX_ATTR_ + "1:" + "qualifiedName", new String[] {"database1.table1@cluster.com"});
        uniqAttributes.put(PREFIX_ATTR_ + "2:" + "qualifiedName", new String[] {"database2.table2@cluster.com"});

        // Mock
        HttpServletRequest mockHttpServletRequest = buildAndGetMockServletRequest(uniqAttributes);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockEntityType.getTypeName()).thenReturn(typeName);
        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST
                .getEntitiesByUniqueAttributes(typeName, minExtInfo, ignoreRelationships, mockHttpServletRequest));

        // Assert
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID);
        assertEquals(exception.getMessage(), AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID.getFormattedErrorMessage(typeName, "qualifiedName"));
    }

    @Test
    public void testGetByGuids_Success() throws AtlasBaseException {
        List<String> guids = Arrays.asList("123", "456");
        boolean minExtInfo = false;
        boolean ignoreRelationships = false;

        AtlasEntity.AtlasEntitiesWithExtInfo expectedResult = new AtlasEntity.AtlasEntitiesWithExtInfo();
        when(mockEntitiesStore.getByIds(guids, minExtInfo, ignoreRelationships)).thenReturn(expectedResult);

        AtlasEntity.AtlasEntitiesWithExtInfo actualResult = entityREST.getByGuids(guids, minExtInfo,
                ignoreRelationships);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockEntitiesStore, times(1)).getByIds(guids, minExtInfo, ignoreRelationships);
    }

    @Test
    public void testGetByGuids_EmptyList_ThrowsException() {
        List<String> guids = new ArrayList<>();

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.getByGuids(guids, false, false));

        TestUtility.assertGUIDNotFoundException(exception, guids.toArray(new String[guids.size()]));
    }

    @Test
    public void testGetByGuids_GuidTooLong_ThrowsException() {
        List<String> guids = Arrays.asList("valid-guid", "invalid-guid");

        // Assume validateQueryParamLength throws IllegalArgumentException for
        // "invalid-guid"
        try (MockedStatic<Servlets> servletMock = mockStatic(Servlets.class)) {
            servletMock.when(() -> Servlets.validateQueryParamLength("guid", "invalid-guid"))
                    .thenThrow(new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_PARAM_LENGTH, "guid"));

            AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                    () -> entityREST.getByGuids(guids, false, false));

            TestUtility.assertInvalidParamLength(exception, "guid");
        }
    }

    @Test
    public void testCreateOrUpdate_Success() throws AtlasBaseException {
        // Setup
        AtlasEntity.AtlasEntitiesWithExtInfo mockEntities = mock(AtlasEntity.AtlasEntitiesWithExtInfo.class);
        EntityMutationResponse expectedResponse = mock(EntityMutationResponse.class);

        when(mockEntities.getEntities()).thenReturn(Collections.emptyList());
        when(mockEntitiesStore.createOrUpdate(any(EntityStream.class), eq(false))).thenReturn(expectedResponse);

        // Execute
        EntityMutationResponse actualResponse = entityREST.createOrUpdate(mockEntities);

        // Verify
        assertEquals(actualResponse, expectedResponse);
        verify(mockEntitiesStore, times(1)).createOrUpdate(any(EntityStream.class), eq(false));
    }

    @Test
    public void testCreateOrUpdate_ThrowsException() throws AtlasBaseException {
        // Setup
        String errorMessage = "no entities to create/update.";
        AtlasEntity.AtlasEntitiesWithExtInfo mockEntities = mock(AtlasEntity.AtlasEntitiesWithExtInfo.class);

        when(mockEntities.getEntities()).thenReturn(Collections.emptyList());
        when(mockEntitiesStore.createOrUpdate(any(EntityStream.class), eq(false)))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, errorMessage));

        // Execute
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.createOrUpdate(mockEntities));

        TestUtility.assertInvalidParameters(exception, errorMessage);
    }

    @Test
    public void testDeleteByGuids_Success() throws AtlasBaseException {
        List<String> guids = Arrays.asList("guid1", "guid2");

        EntityMutationResponse expectedResponse = mock(EntityMutationResponse.class);

        when(mockEntitiesStore.deleteByIds(guids)).thenReturn(expectedResponse);

        EntityMutationResponse actualResponse = entityREST.deleteByGuids(guids);

        assertEquals(actualResponse, expectedResponse);
        verify(mockEntitiesStore).deleteByIds(guids);
    }

    @Test
    public void testDeleteByGuids_NullOrEmptyList_ThrowsException() throws AtlasBaseException {
        List<String> guids = Collections.emptyList();
        String errorMessage = "Guid(s) not specified";

        when(mockEntitiesStore.deleteByIds(guids))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, errorMessage));

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> entityREST.deleteByGuids(guids));

        TestUtility.assertInvalidParameters(exception, errorMessage);
    }

    @Test
    public void testDeleteByGuids_GuidTooLong_ThrowsException() {
        List<String> guids = Collections.singletonList("invalid-guid");
        try (MockedStatic<Servlets> servletMock = mockStatic(Servlets.class)) {
            servletMock.when(() -> Servlets.validateQueryParamLength("guid", "invalid-guid"))
                    .thenThrow(new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_PARAM_LENGTH, "guid"));

            AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                    () -> entityREST.deleteByGuids(guids));

            TestUtility.assertInvalidParamLength(exception, "guid");
        }
    }

    @Test
    public void testAddClassification_Success() throws Exception {
        List<String> guids = Arrays.asList("guid-1", "guid-2");

        ClassificationAssociateRequest classificationAssociateRequestMock = mock(ClassificationAssociateRequest.class);
        AtlasClassification classificationMock = mock(AtlasClassification.class);
        when(classificationAssociateRequestMock.getClassification()).thenReturn(classificationMock);
        when(classificationMock.getTypeName()).thenReturn("TestClassification");
        when(classificationAssociateRequestMock.getEntityGuids()).thenReturn(guids);
        when(classificationAssociateRequestMock.getEntitiesUniqueAttributes()).thenReturn(null);
        when(classificationAssociateRequestMock.getEntityTypeName()).thenReturn(null);

        entityREST.addClassification(classificationAssociateRequestMock);

        verify(mockEntitiesStore, times(1)).addClassification(guids, classificationMock);
    }

    @Test
    public void testAddClassification_nullClassification_ThrowsException() {
        ClassificationAssociateRequest classificationAssociateRequestMock = mock(ClassificationAssociateRequest.class);
        when(classificationAssociateRequestMock.getClassification()).thenReturn(null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.addClassification(classificationAssociateRequestMock));

        TestUtility.assertInvalidParameters(exception, "no classification");
    }

    @Test
    public void testAddClassification_emptyTypeName_ThrowsException() {
        ClassificationAssociateRequest classificationAssociateRequestMock = mock(ClassificationAssociateRequest.class);
        AtlasClassification classificationMock = mock(AtlasClassification.class);
        when(classificationAssociateRequestMock.getClassification()).thenReturn(classificationMock);
        when(classificationMock.getTypeName()).thenReturn("");

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.addClassification(classificationAssociateRequestMock));

        TestUtility.assertInvalidParameters(exception, "no classification");
    }

    @Test
    public void testAddClassification_noGuidsOrAttributes_ThrowsException() {
        ClassificationAssociateRequest classificationAssociateRequestMock = mock(ClassificationAssociateRequest.class);
        AtlasClassification classificationMock = mock(AtlasClassification.class);
        when(classificationAssociateRequestMock.getClassification()).thenReturn(classificationMock);
        when(classificationMock.getTypeName()).thenReturn("TestType");
        when(classificationAssociateRequestMock.getEntityGuids()).thenReturn(null);
        when(classificationAssociateRequestMock.getEntityTypeName()).thenReturn(null);
        when(classificationAssociateRequestMock.getEntitiesUniqueAttributes()).thenReturn(null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.addClassification(classificationAssociateRequestMock));

        TestUtility.assertInvalidParameters(exception, "Need either list of GUIDs or entity type and list of qualified Names");
    }

    @Test
    public void testAddClassification_withUniqueAttributes_Success() throws Exception {
        String entityTypeName = "TestEntityType";
        Map<String, Object> attr1 = new HashMap<>();
        attr1.put("qualifiedName", "entity1@domain");

        List<Map<String, Object>> attrs = Collections.singletonList(attr1);

        ClassificationAssociateRequest classificationAssociateRequestMock = mock(ClassificationAssociateRequest.class);
        AtlasClassification classificationMock = mock(AtlasClassification.class);
        AtlasEntityType entityTypeMock = mock(AtlasEntityType.class);

        when(mockTypeRegistry.getEntityTypeByName(entityTypeName)).thenReturn(entityTypeMock);
        when(classificationAssociateRequestMock.getClassification()).thenReturn(classificationMock);
        when(classificationMock.getTypeName()).thenReturn("TestClassification");
        when(classificationAssociateRequestMock.getEntityGuids()).thenReturn(null);
        when(classificationAssociateRequestMock.getEntitiesUniqueAttributes()).thenReturn(attrs);
        when(classificationAssociateRequestMock.getEntityTypeName()).thenReturn(entityTypeName);

        when(mockEntitiesStore.getGuidByUniqueAttributes(eq(entityTypeMock), eq(attr1))).thenReturn("generated-guid");

        entityREST.addClassification(classificationAssociateRequestMock);

        ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockEntitiesStore).addClassification(captor.capture(), eq(classificationMock));

        List<String> resultGuids = captor.getValue();
        assertEquals(resultGuids.size(), 1);
        assertEquals(resultGuids.get(0), "generated-guid");
    }

    @Test
    public void testGetAuditEvents_withSortAndOffset_Success() throws Exception {
        String guid = "test-guid";
        List<EntityAuditEventV2> mockEvents = Arrays.asList(new EntityAuditEventV2(), new EntityAuditEventV2());

        AtlasEntityHeader mockEntityHeader = new AtlasEntityHeader("typeName");

        when(mockEntitiesStore.getHeaderById(guid)).thenReturn(mockEntityHeader);
        when(mockAuditRepository.listEventsV2(eq(guid), any(), eq("timestamp"), eq(false), eq(10), eq((short) 2)))
                .thenReturn(mockEvents);

        List<EntityAuditEventV2> result = entityREST.getAuditEvents(guid, null, null, (short) 2, 10, "timestamp",
                "asc");

        assertEquals(result.size(), 2);
        verify(mockEntitiesStore).getHeaderById(guid);
        verify(mockAuditRepository).listEventsV2(eq(guid), any(), eq("timestamp"), eq(false), eq(10), eq((short) 2));
    }

    @Test
    public void testGetAuditEvents_withAuditActionOnly_Success() throws Exception {
        String guid = "test-guid";
        List<EntityAuditEventV2> mockEvents = Collections.singletonList(new EntityAuditEventV2());

        AtlasEntityHeader mockEntityHeader = mock(AtlasEntityHeader.class);

        when(mockEntitiesStore.getHeaderById(guid)).thenReturn(mockEntityHeader);
        when(mockAuditRepository.listEventsV2(eq(guid), eq(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE),
                eq("start-key"), eq((short) 1))).thenReturn(mockEvents);

        List<EntityAuditEventV2> result = entityREST.getAuditEvents(guid, "start-key",
                EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE, (short) 1, -1, null, null);

        assertEquals(result.size(), 1);
        verify(mockAuditRepository).listEventsV2(eq(guid), eq(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE),
                eq("start-key"), eq((short) 1));
    }

    @Test
    public void testGetAuditEvents_withFallbackConversion_Success() throws Exception {
        String guid = "test-guid";
        List<Object> legacyEvents = Arrays.asList(new EntityAuditEventV2(), new EntityAuditEvent(), "UnknownEvent");
        AtlasEntityHeader mockEntityHeader = mock(AtlasEntityHeader.class);

        when(mockEntitiesStore.getHeaderById(guid)).thenReturn(mockEntityHeader);
        when(mockAuditRepository.listEvents(eq(guid), eq("start-key"), eq((short) 3))).thenReturn(legacyEvents);
        when(mockInstanceConverter.toV2AuditEvent(any(EntityAuditEvent.class))).thenReturn(new EntityAuditEventV2());

        List<EntityAuditEventV2> result = entityREST.getAuditEvents(guid, "start-key", null, (short) 3, -1, null, null);

        // One native V2, one converted from old format, one ignored
        assertEquals(result.size(), 2);
    }

    @Test
    public void testGetAuditEvents_handlesPurgedEntity_Success() throws Exception {
        String guid = "test-guid";

        AtlasEntityHeader mockEntityHeader = mock(AtlasEntityHeader.class);
        EntityAuditEventV2 mockAuditEventV2 = mock(EntityAuditEventV2.class);

        when(mockAuditEventV2.getEntityHeader()).thenReturn(mockEntityHeader);

        List<EntityAuditEventV2> mockAuditEventLst = new ArrayList<>();
        mockAuditEventLst.add(mockAuditEventV2);

        // Simulate INSTANCE_GUID_NOT_FOUND exception
        AtlasBaseException notFoundException = mock(AtlasBaseException.class);
        when(notFoundException.getAtlasErrorCode()).thenReturn(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND);
        when(mockEntitiesStore.getHeaderById(guid)).thenThrow(notFoundException);

        when(mockAuditRepository.listEventsV2(guid, EntityAuditEventV2.EntityAuditActionV2.ENTITY_PURGE, null,
                (short) 1)).thenReturn(mockAuditEventLst);
        // Static method for authorization is not mocked here (optional)
        when(mockAuditRepository.listEvents(eq(guid), eq("start-key"), eq((short) 2)))
                .thenReturn(Collections.emptyList());
        try (MockedStatic<AtlasAuthorizationUtils> authUtilMock = mockStatic(AtlasAuthorizationUtils.class)) {
            authUtilMock
                    .when(() -> AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(mockTypeRegistry,
                            AtlasPrivilege.ENTITY_READ, mockEntityHeader), "read entity audit: guid=", guid))
                    .thenAnswer(invocation -> null);
            List<EntityAuditEventV2> result = entityREST.getAuditEvents(guid, "start-key", null, (short) 2, -1, null,
                    null);

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testGetEntityHeaders_InvalidTime_ThrowsException() {
        // Arrange
        long futureTime = System.currentTimeMillis() + 10000; // 10 seconds ahead

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.getEntityHeaders(futureTime));

        // Assert is handled by expected exception
        TestUtility.assertBadRequests(exception, "fromTimestamp should be less than toTimestamp");
    }

    @Test
    public void testAddOrUpdateBusinessAttributes_Success() throws AtlasBaseException {
        // Arrange
        String guid = "1234-abc";
        boolean isOverwrite = true;

        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("attr1", "value1");
        businessAttributes.put("BusinessMetadataType", values);

        // Act
        entityREST.addOrUpdateBusinessAttributes(guid, isOverwrite, businessAttributes);

        // Assert
        verify(mockEntitiesStore, times(1)).addOrUpdateBusinessAttributes(guid, businessAttributes, isOverwrite);
    }

    @Test
    public void testAddOrUpdateBusinessAttributes_EmptyGuid_ThrowsException() throws AtlasBaseException {
        // Arrange
        String guid = "";
        boolean isOverwrite = false;
        Map<String, Map<String, Object>> businessAttributes = Collections.emptyMap();
        String errorMessage = "guid is null/empty";

        doThrow(new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, errorMessage)).when(mockEntitiesStore)
                .addOrUpdateBusinessAttributes(eq(guid), eq(businessAttributes), eq(isOverwrite));

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.addOrUpdateBusinessAttributes(guid, isOverwrite, businessAttributes));

        TestUtility.assertInvalidParameters(exception, errorMessage);
    }

    @Test
    public void testRemoveBusinessAttributes_Success() throws AtlasBaseException {
        // Arrange
        String guid = "valid-guid";
        Map<String, Map<String, Object>> businessAttributes = Collections.emptyMap();

        // Act
        entityREST.removeBusinessAttributes(guid, businessAttributes);

        // Assert
        verify(mockEntitiesStore, times(1)).removeBusinessAttributes(guid, businessAttributes);
    }

    @Test
    public void testRemoveBusinessAttributes_EmptyGuid_ThrowsException() throws AtlasBaseException {
        // Arrange
        String guid = ""; // empty guid
        Map<String, Map<String, Object>> businessAttributes = Collections.emptyMap();
        String errorMessage = "guid is null/empty";

        doThrow(new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, errorMessage)).when(mockEntitiesStore)
                .removeBusinessAttributes(guid, businessAttributes);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.removeBusinessAttributes(guid, businessAttributes));

        // Assert
        TestUtility.assertInvalidParameters(exception, errorMessage);
    }

    @Test
    public void testAddOrUpdateBusinessAttributes_bmName_Success() throws AtlasBaseException {
        // Arrange
        String guid = "1234-abc";
        String bmName = "bmNameValue";

        Map<String, Object> businessAttributes = Collections.emptyMap();

        // Act
        entityREST.addOrUpdateBusinessAttributes(guid, bmName, businessAttributes);

        // Assert
        verify(mockEntitiesStore, times(1)).addOrUpdateBusinessAttributes(guid,
                Collections.singletonMap(bmName, businessAttributes), false);
    }

    @Test
    public void testRemoveBusinessAttributes_bmName_Success() throws AtlasBaseException {
        // Arrange
        String guid = "valid-guid";
        String bmName = "bmName";
        Map<String, Object> businessAttributes = mock(Map.class);

        // Act
        entityREST.removeBusinessAttributes(guid, bmName, businessAttributes);

        // Assert
        verify(mockEntitiesStore, times(1)).removeBusinessAttributes(guid,
                Collections.singletonMap(bmName, businessAttributes));
    }

    @Test
    public void testSetLabels_Success() throws AtlasBaseException {
        // Arrange
        String guid = "valid-guid";
        Set<String> mockLabelsSet = Collections.emptySet();

        // Act
        entityREST.setLabels(guid, mockLabelsSet);

        // Assert
        verify(mockEntitiesStore, times(1)).setLabels(guid, mockLabelsSet);
    }

    @Test
    public void testRemoveLabels_Success() throws AtlasBaseException {
        // Arrange
        String guid = "valid-guid";
        Set<String> mockLabelsSet = Collections.emptySet();

        // Act
        entityREST.removeLabels(guid, mockLabelsSet);

        // Assert
        verify(mockEntitiesStore, times(1)).removeLabels(guid, mockLabelsSet);
    }

    @Test
    public void testAddLabels_Success() throws AtlasBaseException {
        // Arrange
        String guid = "valid-guid";
        Set<String> mockLabelsSet = Collections.emptySet();

        // Act
        entityREST.addLabels(guid, mockLabelsSet);

        // Assert
        verify(mockEntitiesStore, times(1)).addLabels(guid, mockLabelsSet);
    }

    @Test
    public void testSetLabels_TypeName_Success() throws AtlasBaseException {
        // Arrange
        String guid = "valid-guid";
        String typeName = "entityTypeName";
        Set<String> mockLabelsSet = Collections.emptySet();
        HttpServletRequest mockServletRequest = buildAndGetMockServletRequest(null);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(eq(mockEntityType), anyMap())).thenReturn(guid);
        // Act
        entityREST.setLabels(typeName, mockLabelsSet, mockServletRequest);

        // Assert
        verify(mockEntitiesStore, times(1)).setLabels(guid, mockLabelsSet);
    }

    @Test
    public void testSetLabels_TypeName_Invalid_ThrowsException() throws AtlasBaseException {
        // Arrange
        String guid = null; // return null guid
        String typeName = "entityTypeName";
        Set<String> mockLabelsSet = Collections.emptySet();
        HttpServletRequest mockServletRequest = buildAndGetMockServletRequest(null);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(eq(mockEntityType), anyMap())).thenReturn(guid);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.setLabels(typeName, mockLabelsSet, mockServletRequest));

        // Assert
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND);
        assertEquals(exception.getMessage(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND.getFormattedErrorMessage(typeName, "{}"));
    }

    @Test
    public void testSetLabels_TypeNameTooLong_ThrowsException() {
        // Arrange
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        Set<String> mockLabelsSet = Collections.emptySet();
        HttpServletRequest mockServletRequest = buildAndGetMockServletRequest(null);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.setLabels(typeName, mockLabelsSet, mockServletRequest));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testAddLabels_TypeName_Success() throws AtlasBaseException {
        // Arrange
        String guid = "valid-guid";
        String typeName = "entityTypeName";
        Set<String> mockLabels = Collections.emptySet();
        HttpServletRequest mockServletRequest = buildAndGetMockServletRequest(null);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(eq(mockEntityType), anyMap())).thenReturn(guid);

        // Act
        entityREST.addLabels(typeName, mockLabels, mockServletRequest);

        // Assert
        verify(mockEntitiesStore, times(1)).addLabels(guid, mockLabels);
    }

    @Test
    public void testAddLabels_TypeName_Invalid_Exception() throws AtlasBaseException {
        // Arrange
        String guid = null; // return null guid
        String typeName = "entityTypeName";
        Set<String> mocklabels = Collections.emptySet();
        HttpServletRequest mockServletRequest = buildAndGetMockServletRequest(null);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(eq(mockEntityType), anyMap())).thenReturn(guid);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.addLabels(typeName, mocklabels, mockServletRequest));

        // Assert
        verify(mockEntitiesStore, never()).addLabels(guid, mocklabels);
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND);
        assertEquals(exception.getMessage(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND.getFormattedErrorMessage(typeName, "{}"));
    }

    @Test
    public void testAddLabels_TypeNameTooLong_Exception() {
        // Arrange
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        Set<String> mocklabels = Collections.emptySet();
        HttpServletRequest mockServletRequest = buildAndGetMockServletRequest(null);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.addLabels(typeName, mocklabels, mockServletRequest));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testRemoveLabels_TypeName_Success() throws AtlasBaseException {
        // Arrange
        String guid = "valid-guid";
        String typeName = "entityTypeName";
        Set<String> mocklabels = Collections.emptySet();
        HttpServletRequest mockServletRequest = buildAndGetMockServletRequest(null);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(eq(mockEntityType), anyMap())).thenReturn(guid);

        // Act
        entityREST.removeLabels(typeName, mocklabels, mockServletRequest);

        // Assert
        verify(mockEntitiesStore).removeLabels(guid, mocklabels);
    }

    @Test
    public void testRemoveLabels_TypeName_Invalid_Exception() throws AtlasBaseException {
        // Arrange
        String guid = null; // return null guid
        String typeName = "entityTypeName";
        Set<String> mocklabels = Collections.emptySet();
        HttpServletRequest mockServletRequest = buildAndGetMockServletRequest(null);
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);

        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);
        when(mockEntitiesStore.getGuidByUniqueAttributes(eq(mockEntityType), anyMap())).thenReturn(guid);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.removeLabels(typeName, mocklabels, mockServletRequest));

        // Assert
        verify(mockEntitiesStore, never()).removeLabels(guid, mocklabels);
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND);
        assertEquals(exception.getMessage(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND.getFormattedErrorMessage(typeName, "{}"));
    }

    @Test
    public void testRemoveLabels_TypeNameTooLong_Exception() throws AtlasBaseException {
        // Arrange
        String typeName = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        Set<String> mockLabels = Collections.emptySet();
        HttpServletRequest mockServletRequest = buildAndGetMockServletRequest(null);

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> entityREST.removeLabels(typeName, mockLabels, mockServletRequest));

        // Assert
        verify(mockEntitiesStore, never()).removeLabels(any(), eq(mockLabels));
        TestUtility.assertInvalidParamLength(exception, "typeName");
    }

    @Test
    public void testProduceTemplate_Success() throws Exception {
        // Arrange
        // Optionally mock FileUtils if it's static and returns known value
        String expectedContent = "name,description,attribute1";
        try (MockedStatic<FileUtils> fileUtilsMock = mockStatic(FileUtils.class)) {
            fileUtilsMock.when(() -> FileUtils.getBusinessMetadataHeaders()).thenReturn(expectedContent);
            // Act
            Response response = entityREST.produceTemplate();

            // Assert status code
            assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

            // Assert headers
            String contentDisposition = (String) response.getMetadata().getFirst("Content-Disposition");
            assertEquals("attachment; filename=\"template_business_metadata\"", contentDisposition);

            // Assert body content
            StreamingOutput entity = (StreamingOutput) response.getEntity();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            entity.write(outputStream);
            String actualContent = outputStream.toString();

            assertEquals(actualContent, expectedContent);
        }
    }

    @Test
    public void testImportBMAttributes_Success() throws Exception {
        // Arrange
        String testFileName = "test.csv";
        String fileContent = "EntityType,EntityUniqueAttributeValue,BusinessAttributeName,BusinessAttributeValue";
        InputStream testInputStream = new ByteArrayInputStream(fileContent.getBytes());

        FormDataContentDisposition mockDisposition = mock(FormDataContentDisposition.class);
        when(mockDisposition.getFileName()).thenReturn(testFileName);

        BulkImportResponse expectedResponse = new BulkImportResponse();
        when(mockEntitiesStore.bulkCreateOrUpdateBusinessAttributes(testInputStream, testFileName))
                .thenReturn(expectedResponse);

        // Act
        BulkImportResponse actualResponse = entityREST.importBMAttributes(testInputStream, mockDisposition);

        // Assert
        assertEquals(expectedResponse, actualResponse);
        verify(mockEntitiesStore, times(1)).bulkCreateOrUpdateBusinessAttributes(testInputStream, testFileName);
    }

    private Map<String, Object> processAttributes(Map<String, String[]> attributes) {
        Map<String, Object> processedAttributes = attributes.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getKey().startsWith(PREFIX_ATTR))
                .collect(Collectors.toMap(entry -> entry.getKey().substring(PREFIX_ATTR.length()), entry -> {
                    String[] value = entry.getValue();
                    return (value != null && value.length > 0) ? value[0] : null;
                }));
        return processedAttributes;
    }
}
