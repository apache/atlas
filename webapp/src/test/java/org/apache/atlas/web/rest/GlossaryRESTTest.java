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
import org.apache.atlas.SortOrder;
import org.apache.atlas.bulkimport.BulkImportResponse;
import org.apache.atlas.common.TestUtility;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.glossary.GlossaryService;
import org.apache.atlas.glossary.GlossaryTermUtils;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.StreamingOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class GlossaryRESTTest {
    private static final String VALID_GLOSSARY_GUID = "1234";
    private static final String INVALID_GLOSSARY_GUID = "not-found";

    @Mock
    GlossaryService mockGlossaryService;

    GlossaryREST glossaryREST;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        glossaryREST = new GlossaryREST(mockGlossaryService);
    }

    @Test
    public void testGetGlossaries_Success() throws AtlasBaseException {
        List<AtlasGlossary> mockList = Arrays.asList(new AtlasGlossary(), new AtlasGlossary());

        when(mockGlossaryService.getGlossaries(10, 0, SortOrder.ASCENDING)).thenReturn(mockList);

        List<AtlasGlossary> result = glossaryREST.getGlossaries("10", "0", "ASC");

        assertEquals(result.size(), 2);
        verify(mockGlossaryService).getGlossaries(10, 0, SortOrder.ASCENDING);
    }

    @Test
    public void testGetGlossary_Success() throws AtlasBaseException {
        AtlasGlossary mockGlossary = new AtlasGlossary();
        when(mockGlossaryService.getGlossary(VALID_GLOSSARY_GUID)).thenReturn(mockGlossary);

        AtlasGlossary result = glossaryREST.getGlossary(VALID_GLOSSARY_GUID);

        assertNotNull(result);
        verify(mockGlossaryService).getGlossary(VALID_GLOSSARY_GUID);
    }

    @Test
    public void testGetGlossary_GlossaryGUIDNotFound_ThrowsException() throws AtlasBaseException {
        when(mockGlossaryService.getGlossary(INVALID_GLOSSARY_GUID)).thenReturn(null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.getGlossary(INVALID_GLOSSARY_GUID));

        TestUtility.assertGUIDNotFoundException(exception);
    }

    @Test
    public void testGetDetailedGlossary_Success() throws AtlasBaseException {
        AtlasGlossary.AtlasGlossaryExtInfo mockExtInfo = new AtlasGlossary.AtlasGlossaryExtInfo();
        when(mockGlossaryService.getDetailedGlossary(VALID_GLOSSARY_GUID)).thenReturn(mockExtInfo);

        AtlasGlossary.AtlasGlossaryExtInfo result = glossaryREST.getDetailedGlossary(VALID_GLOSSARY_GUID);

        assertNotNull(result);
        verify(mockGlossaryService).getDetailedGlossary(VALID_GLOSSARY_GUID);
    }

    @Test
    public void testGetDetailedGlossary_GlossaryGUIDNotFound_ThrowsException() throws AtlasBaseException {
        when(mockGlossaryService.getDetailedGlossary(INVALID_GLOSSARY_GUID)).thenReturn(null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.getDetailedGlossary(INVALID_GLOSSARY_GUID));

        TestUtility.assertGUIDNotFoundException(exception);
    }

    @Test
    public void testGetGlossaryTerm_Success() throws AtlasBaseException {
        String termGuid = "term-guid-123";
        AtlasGlossaryTerm mockTerm = new AtlasGlossaryTerm();
        when(mockGlossaryService.getTerm(termGuid)).thenReturn(mockTerm);

        AtlasGlossaryTerm result = glossaryREST.getGlossaryTerm(termGuid);

        assertNotNull(result);
        verify(mockGlossaryService).getTerm(termGuid);
    }

    @Test
    public void testGetGlossaryTerm_NotFound_ThrowsException() throws AtlasBaseException {
        String termGuid = "non-existent-guid";
        when(mockGlossaryService.getTerm(termGuid)).thenReturn(null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.getGlossaryTerm(termGuid));

        TestUtility.assertGUIDNotFoundException(exception);
    }

    @Test
    public void testGetGlossaryTerm_GuidTooLong_ThrowsException() {
        String longTermGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.getGlossaryTerm(longTermGuid));

        TestUtility.assertInvalidParamLength(exception, "termGuid");
    }

    @Test
    public void testGetGlossaryCategory_Success() throws AtlasBaseException {
        String categoryGuid = "category-guid-123";
        AtlasGlossaryCategory mockCategory = new AtlasGlossaryCategory();
        when(mockGlossaryService.getCategory(categoryGuid)).thenReturn(mockCategory);

        AtlasGlossaryCategory result = glossaryREST.getGlossaryCategory(categoryGuid);

        assertNotNull(result);
        verify(mockGlossaryService).getCategory(categoryGuid);
    }

    @Test
    public void testGetGlossaryCategory_NotFound_ThrowsException() throws AtlasBaseException {
        String categoryGuid = "invalid-category-guid";
        when(mockGlossaryService.getCategory(categoryGuid)).thenReturn(null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.getGlossaryCategory(categoryGuid));

        TestUtility.assertGUIDNotFoundException(exception);
    }

    @Test
    public void testGetGlossaryCategory_GuidTooLong_ThrowsException() throws AtlasBaseException {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.getGlossaryCategory(longGuid));

        TestUtility.assertInvalidParamLength(exception, "categoryGuid");

        verify(mockGlossaryService, never()).getCategory(longGuid);
    }

    @Test
    public void testCreateGlossary_Success() throws AtlasBaseException {
        AtlasGlossary inputGlossary = new AtlasGlossary();
        AtlasGlossary createdGlossary = new AtlasGlossary();

        when(mockGlossaryService.createGlossary(inputGlossary)).thenReturn(createdGlossary);

        AtlasGlossary result = glossaryREST.createGlossary(inputGlossary);

        assertNotNull(result);
        verify(mockGlossaryService).createGlossary(inputGlossary);
    }

    @Test
    public void testCreateGlossaryTerm_Success() throws AtlasBaseException {
        AtlasGlossaryTerm inputTerm = new AtlasGlossaryTerm();
        inputTerm.setAnchor(new AtlasGlossaryHeader(VALID_GLOSSARY_GUID));

        AtlasGlossaryTerm createdTerm = new AtlasGlossaryTerm();
        when(mockGlossaryService.createTerm(inputTerm)).thenReturn(createdTerm);

        AtlasGlossaryTerm result = glossaryREST.createGlossaryTerm(inputTerm);

        assertNotNull(result);
        verify(mockGlossaryService).createTerm(inputTerm);
    }

    @Test
    public void testCreateGlossaryTerm_MissingAnchor_ThrowsException() {
        AtlasGlossaryTerm inputTerm = new AtlasGlossaryTerm();
        inputTerm.setAnchor(null); // Explicitly null

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.createGlossaryTerm(inputTerm));

        assertEquals(ex.getAtlasErrorCode(), AtlasErrorCode.MISSING_MANDATORY_ANCHOR);
    }

    @Test
    public void testCreateGlossaryTerms_Success() throws AtlasBaseException {
        AtlasGlossaryTerm term1 = new AtlasGlossaryTerm();
        term1.setAnchor(new AtlasGlossaryHeader(VALID_GLOSSARY_GUID));
        AtlasGlossaryTerm term2 = new AtlasGlossaryTerm();
        term2.setAnchor(new AtlasGlossaryHeader(VALID_GLOSSARY_GUID));
        List<AtlasGlossaryTerm> inputTerms = Arrays.asList(term1, term2);

        when(mockGlossaryService.createTerms(inputTerms)).thenReturn(inputTerms);

        List<AtlasGlossaryTerm> result = glossaryREST.createGlossaryTerms(inputTerms);

        assertNotNull(result);
        assertEquals(result.size(), 2);
        verify(mockGlossaryService).createTerms(inputTerms);
    }

    @Test
    public void testCreateGlossaryTerms_MissingAnchor_ThrowsException() {
        AtlasGlossaryTerm term1 = new AtlasGlossaryTerm();
        term1.setAnchor(new AtlasGlossaryHeader(VALID_GLOSSARY_GUID));
        AtlasGlossaryTerm term2 = new AtlasGlossaryTerm();
        term2.setAnchor(null); // Invalid
        List<AtlasGlossaryTerm> inputTerms = Arrays.asList(term1, term2);

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.createGlossaryTerms(inputTerms));

        assertEquals(ex.getAtlasErrorCode(), AtlasErrorCode.MISSING_MANDATORY_ANCHOR);
    }

    @Test
    public void testCreateGlossaryCategory_Success() throws AtlasBaseException {
        AtlasGlossaryCategory category = new AtlasGlossaryCategory();
        category.setAnchor(new AtlasGlossaryHeader(VALID_GLOSSARY_GUID));

        when(mockGlossaryService.createCategory(category)).thenReturn(category);

        AtlasGlossaryCategory result = glossaryREST.createGlossaryCategory(category);

        assertNotNull(result);
        verify(mockGlossaryService).createCategory(category);
    }

    @Test
    public void testCreateGlossaryCategory_MissingAnchor_ThrowsException() {
        AtlasGlossaryCategory category = new AtlasGlossaryCategory();
        category.setAnchor(null); // Invalid input

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.createGlossaryCategory(category));

        assertEquals(ex.getAtlasErrorCode(), AtlasErrorCode.MISSING_MANDATORY_ANCHOR);
    }

    @Test
    public void testCreateGlossaryCategories_Success() throws AtlasBaseException {
        AtlasGlossaryHeader glossaryHeader = new AtlasGlossaryHeader(VALID_GLOSSARY_GUID);

        AtlasGlossaryCategory category1 = new AtlasGlossaryCategory();
        category1.setAnchor(glossaryHeader);

        AtlasGlossaryCategory category2 = new AtlasGlossaryCategory();
        category2.setAnchor(glossaryHeader);

        List<AtlasGlossaryCategory> input = Arrays.asList(category1, category2);

        when(mockGlossaryService.createCategories(input)).thenReturn(input);

        List<AtlasGlossaryCategory> result = glossaryREST.createGlossaryCategories(input);

        assertNotNull(result);
        assertEquals(result.size(), 2);
        verify(mockGlossaryService).createCategories(input);
    }

    @Test
    public void testCreateGlossaryCategories_MissingAnchor_ThrowsException() {
        AtlasGlossaryHeader glossaryHeader = new AtlasGlossaryHeader(VALID_GLOSSARY_GUID);

        AtlasGlossaryCategory category1 = new AtlasGlossaryCategory();
        category1.setAnchor(glossaryHeader);

        AtlasGlossaryCategory category2 = new AtlasGlossaryCategory();
        category2.setAnchor(null); // Invalid input

        List<AtlasGlossaryCategory> input = Arrays.asList(category1, category2);

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.createGlossaryCategories(input));

        assertEquals(ex.getAtlasErrorCode(), AtlasErrorCode.MISSING_MANDATORY_ANCHOR);
    }

    @Test
    public void testUpdateGlossary_Success() throws AtlasBaseException {
        String glossaryGuid = "glossary-123";
        AtlasGlossary inputGlossary = new AtlasGlossary();
        AtlasGlossary updatedGlossary = new AtlasGlossary();

        when(mockGlossaryService.updateGlossary(any(AtlasGlossary.class))).thenReturn(updatedGlossary);

        AtlasGlossary actualResult = glossaryREST.updateGlossary(glossaryGuid, inputGlossary);

        assertNotNull(actualResult);
        assertEquals(inputGlossary.getGuid(), glossaryGuid); // confirm that GUID was set
        verify(mockGlossaryService).updateGlossary(inputGlossary);
    }

    @Test
    public void testUpdateGlossary_GuidTooLong_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        AtlasGlossary glossary = new AtlasGlossary();

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.updateGlossary(longGuid, glossary));

        TestUtility.assertInvalidParamLength(exception, "glossaryGuid");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testUpdateGlossary_NullGlossary_ThrowsException() throws AtlasBaseException {
        glossaryREST.updateGlossary("glossary-123", null);
    }

    @Test
    public void testPartialUpdateGlossary_Success() throws AtlasBaseException {
        String glossaryGuid = "glossary-123";
        Map<String, String> updates = new HashMap<>();
        updates.put("displayName", "Updated Glossary");
        updates.put("shortDescription", "Updated Description");

        AtlasGlossary glossary = mock(AtlasGlossary.class);
        when(mockGlossaryService.getGlossary(glossaryGuid)).thenReturn(glossary);
        when(mockGlossaryService.updateGlossary(glossary)).thenReturn(glossary);

        AtlasGlossary result = glossaryREST.partialUpdateGlossary(glossaryGuid, updates);

        assertNotNull(result);
        verify(glossary).setAttribute("displayName", "Updated Glossary");
        verify(glossary).setAttribute("shortDescription", "Updated Description");
        verify(mockGlossaryService).updateGlossary(glossary);
    }

    @Test
    public void testPartialUpdateGlossary_EmptyUpdates_ThrowsException() {
        String glossaryGuid = "glossary-123";
        Map<String, String> emptyUpdates = new HashMap<>();

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.partialUpdateGlossary(glossaryGuid, emptyUpdates));

        TestUtility.assertBadRequests(exception, "PartialUpdates missing or empty");
    }

    @Test
    public void testPartialUpdateGlossary_GuidTooLong_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        Map<String, String> updates = Collections.emptyMap();

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.partialUpdateGlossary(longGuid, updates));

        TestUtility.assertInvalidParamLength(exception, "glossaryGuid");
    }

    @Test
    public void testPartialUpdateGlossary_InvalidAttribute_ThrowsException() throws AtlasBaseException {
        String glossaryGuid = "glossary-123";
        Map<String, String> updates = new HashMap<>();
        updates.put("invalidAttribute", "someValue");

        AtlasGlossary atlasGlossary = new AtlasGlossary();
        when(mockGlossaryService.getGlossary(glossaryGuid)).thenReturn(atlasGlossary);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.partialUpdateGlossary(glossaryGuid, updates));

        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARTIAL_UPDATE_ATTR);
        assertEquals(exception.getMessage(), AtlasErrorCode.INVALID_PARTIAL_UPDATE_ATTR.getFormattedErrorMessage("invalidAttribute", "Glossary"));
    }

    @Test
    public void testUpdateGlossaryTerm_Success() throws AtlasBaseException {
        // Given
        String termGuid = "1234-5678";
        AtlasGlossaryTerm inputTerm = new AtlasGlossaryTerm();
        AtlasGlossaryTerm updatedTerm = new AtlasGlossaryTerm();
        updatedTerm.setGuid(termGuid);

        when(mockGlossaryService.updateTerm(any(AtlasGlossaryTerm.class))).thenReturn(updatedTerm);

        // When
        AtlasGlossaryTerm actualResult = glossaryREST.updateGlossaryTerm(termGuid, inputTerm);

        // Then
        assertNotNull(actualResult);
        assertEquals(actualResult.getGuid(), termGuid);
        verify(mockGlossaryService).updateTerm(inputTerm);
        assertEquals(inputTerm.getGuid(), termGuid);
    }

    @Test
    public void testUpdateGlossaryTerm_GuidTooLong_ThrowsException() {
        // Given
        String termGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a');
        AtlasGlossaryTerm inputTerm = new AtlasGlossaryTerm();

        // When
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.updateGlossaryTerm(termGuid, inputTerm));

        // Assert
        TestUtility.assertInvalidParamLength(exception, "termGuid");
    }

    @Test
    public void testPartialUpdateGlossaryTerm_Success() throws AtlasBaseException {
        // Given
        String termGuid = "guid-123";
        Map<String, String> updates = new HashMap<>();
        updates.put("name", "Updated Term");

        AtlasGlossaryTerm existingTerm = new AtlasGlossaryTerm();
        AtlasGlossaryTerm updatedTerm = new AtlasGlossaryTerm();
        updatedTerm.setGuid(termGuid);

        when(mockGlossaryService.getTerm(termGuid)).thenReturn(existingTerm);
        when(mockGlossaryService.updateTerm(existingTerm)).thenReturn(updatedTerm);

        // When
        AtlasGlossaryTerm actualResult = glossaryREST.partialUpdateGlossaryTerm(termGuid, updates);

        // Then
        assertNotNull(actualResult);
        assertEquals(actualResult.getGuid(), termGuid);
        verify(mockGlossaryService).getTerm(termGuid);
        verify(mockGlossaryService).updateTerm(existingTerm);
    }

    @Test
    public void testPartialUpdateGlossaryTerm_EmptyUpdates_ThrowsBadRequest() {
        // Given
        String termGuid = "guid-123";
        Map<String, String> emptyUpdates = new HashMap<>();

        // When
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.partialUpdateGlossaryTerm(termGuid, emptyUpdates));

        // Then
        TestUtility.assertBadRequests(exception, "PartialUpdates missing or empty");
    }

    @Test
    public void testPartialUpdateGlossaryTerm_InvalidAttributeKey_ThrowsException() throws AtlasBaseException {
        // Given
        String termGuid = "guid-123";
        Map<String, String> updates = new HashMap<>();
        updates.put("invalidKey", "SomeValue");

        AtlasGlossaryTerm existingTerm = new AtlasGlossaryTerm();

        when(mockGlossaryService.getTerm(termGuid)).thenReturn(existingTerm);

        // When
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.partialUpdateGlossaryTerm(termGuid, updates));

        // Then
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARTIAL_UPDATE_ATTR);
        assertEquals(exception.getMessage(), AtlasErrorCode.INVALID_PARTIAL_UPDATE_ATTR.getFormattedErrorMessage("Glossary Term", "invalidKey"));
    }

    @Test
    public void testUpdateGlossaryCategory_Success() throws AtlasBaseException {
        // Given
        String categoryGuid = "guid-5678";
        AtlasGlossaryCategory inputCategory = new AtlasGlossaryCategory();
        AtlasGlossaryCategory updatedCategory = new AtlasGlossaryCategory();
        updatedCategory.setGuid(categoryGuid);

        when(mockGlossaryService.updateCategory(any(AtlasGlossaryCategory.class))).thenReturn(updatedCategory);

        // When
        AtlasGlossaryCategory actualResult = glossaryREST.updateGlossaryCategory(categoryGuid, inputCategory);

        // Then
        assertNotNull(actualResult);
        assertEquals(actualResult.getGuid(), categoryGuid);
        verify(mockGlossaryService).updateCategory(inputCategory);
        assertEquals(inputCategory.getGuid(), categoryGuid);
    }

    @Test
    public void testUpdateGlossaryCategory_GuidTooLong_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'x');
        AtlasGlossaryCategory inputCategory = new AtlasGlossaryCategory();

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.updateGlossaryCategory(longGuid, inputCategory));

        TestUtility.assertInvalidParamLength(exception, "categoryGuid");
    }

    @Test
    public void testPartialUpdateGlossaryCategory_Success() throws AtlasBaseException {
        // Given
        String categoryGuid = "guid-1111";
        Map<String, String> updates = Collections.singletonMap("name", "Updated Name");

        AtlasGlossaryCategory existingCategory = new AtlasGlossaryCategory();
        AtlasGlossaryCategory expectedResult = new AtlasGlossaryCategory();
        expectedResult.setGuid(categoryGuid);

        when(mockGlossaryService.getCategory(categoryGuid)).thenReturn(existingCategory);
        when(mockGlossaryService.updateCategory(existingCategory)).thenReturn(expectedResult);

        // When
        AtlasGlossaryCategory actualResult = glossaryREST.partialUpdateGlossaryCategory(categoryGuid, updates);

        // Then
        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        assertEquals(actualResult.getGuid(), categoryGuid);
        verify(mockGlossaryService).getCategory(categoryGuid);
        verify(mockGlossaryService).updateCategory(existingCategory);
    }

    @Test
    public void testPartialUpdateGlossaryCategory_EmptyUpdates_ThrowsBadRequest() {
        String categoryGuid = "guid-2222";
        Map<String, String> emptyUpdates = new HashMap<>();

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.partialUpdateGlossaryCategory(categoryGuid, emptyUpdates));

        TestUtility.assertBadRequests(exception, "PartialUpdates missing or empty");
    }

    @Test
    public void testPartialUpdateGlossaryCategory_InvalidAttribute_ThrowsException() throws AtlasBaseException {
        // Given
        String categoryGuid = "guid-3333";
        Map<String, String> updates = Collections.singletonMap("invalidAttr", "value");
        AtlasGlossaryCategory existingCategory = new AtlasGlossaryCategory();

        when(mockGlossaryService.getCategory(categoryGuid)).thenReturn(existingCategory);

        // When
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.partialUpdateGlossaryCategory(categoryGuid, updates));

        // Then
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARTIAL_UPDATE_ATTR);
        assertEquals(exception.getMessage(), AtlasErrorCode.INVALID_PARTIAL_UPDATE_ATTR.getFormattedErrorMessage("Glossary Category", "invalidAttr"));
    }

    @Test
    public void testDeleteGlossary_Success() throws AtlasBaseException {
        String glossaryGuid = "glossary-guid-123";

        glossaryREST.deleteGlossary(glossaryGuid);

        verify(mockGlossaryService).deleteGlossary(glossaryGuid);
    }

    @Test
    public void testDeleteGlossary_GuidTooLong_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'x');

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.deleteGlossary(longGuid));

        TestUtility.assertInvalidParamLength(exception, "glossaryGuid");
    }

    @Test
    public void testDeleteGlossaryTerm_Success() throws AtlasBaseException {
        String termGuid = "term-guid-123";

        glossaryREST.deleteGlossaryTerm(termGuid);

        verify(mockGlossaryService).deleteTerm(termGuid);
    }

    @Test
    public void testDeleteGlossaryTerm_TermGuidTooLong_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'x');

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.deleteGlossaryTerm(longGuid));

        TestUtility.assertInvalidParamLength(ex, "termGuid");
    }

    @Test
    public void testDeleteGlossaryCategory_Success() throws AtlasBaseException {
        String categoryGuid = "category-guid-123";

        glossaryREST.deleteGlossaryCategory(categoryGuid);

        verify(mockGlossaryService).deleteCategory(categoryGuid);
    }

    @Test
    public void testDeleteGlossaryCategory_InvalidGuid_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'x');

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.deleteGlossaryCategory(longGuid));

        TestUtility.assertInvalidParamLength(ex, "categoryGuid");
    }

    @Test
    public void testGetGlossaryTerms_Success() throws AtlasBaseException {
        String glossaryGuid = "glossary-guid-123";
        String limit = "10";
        String offset = "0";
        String sort = "DESC";

        List<AtlasGlossaryTerm> expectedResult = Arrays.asList(new AtlasGlossaryTerm(), new AtlasGlossaryTerm());

        when(mockGlossaryService.getGlossaryTerms(glossaryGuid, 0, 10, SortOrder.DESCENDING)).thenReturn(expectedResult);

        List<AtlasGlossaryTerm> actualResult = glossaryREST.getGlossaryTerms(glossaryGuid, limit, offset, sort);

        assertNotNull(actualResult);
        assertEquals(actualResult.size(), expectedResult.size());
        verify(mockGlossaryService).getGlossaryTerms(glossaryGuid, 0, 10, SortOrder.DESCENDING);
    }

    @Test
    public void testGetGlossaryTerms_InvalidGuid_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'z');

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.getGlossaryTerms(longGuid, "10", "0", "DESC"));

        TestUtility.assertInvalidParamLength(ex, "glossaryGuid");
    }

    @Test
    public void testGetGlossaryTermHeaders_Success() throws AtlasBaseException {
        String limit = "-1";
        String offset = "0";
        String sort = "ASC";

        List<AtlasRelatedTermHeader> expectedResult = Collections.singletonList(new AtlasRelatedTermHeader());

        when(mockGlossaryService.getGlossaryTermsHeaders(VALID_GLOSSARY_GUID, Integer.parseInt(offset), Integer.parseInt(limit), SortOrder.ASCENDING)).thenReturn(expectedResult);

        List<AtlasRelatedTermHeader> actualResult = glossaryREST.getGlossaryTermHeaders(VALID_GLOSSARY_GUID, limit, offset, sort);

        assertEquals(actualResult, expectedResult);
        verify(mockGlossaryService).getGlossaryTermsHeaders(VALID_GLOSSARY_GUID, Integer.parseInt(offset), Integer.parseInt(limit), SortOrder.ASCENDING);
    }

    @Test
    public void testGetGlossaryTermHeaders_InvalidGuid_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'g');

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.getGlossaryTermHeaders(longGuid, "5", "0", "ASC"));

        TestUtility.assertInvalidParamLength(exception, "glossaryGuid");
    }

    @Test
    public void testGetGlossaryCategories_Success() throws AtlasBaseException {
        List<AtlasGlossaryCategory> expectedResult = Collections.singletonList(new AtlasGlossaryCategory());

        when(mockGlossaryService.getGlossaryCategories(VALID_GLOSSARY_GUID, 0, -1, SortOrder.ASCENDING)).thenReturn(expectedResult);

        List<AtlasGlossaryCategory> actualResult = glossaryREST.getGlossaryCategories(VALID_GLOSSARY_GUID, "-1", "0", "ASC");

        assertEquals(actualResult, expectedResult);
        verify(mockGlossaryService).getGlossaryCategories(VALID_GLOSSARY_GUID, 0, -1, SortOrder.ASCENDING);
    }

    @Test
    public void testGetGlossaryCategories_InvalidGuid_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'z');

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> glossaryREST.getGlossaryCategories(longGuid, "10", "0", "ASC"));

        TestUtility.assertInvalidParamLength(exception, "glossaryGuid");
    }

    @Test
    public void testGetGlossaryCategoriesHeaders_Success() throws AtlasBaseException {
        List<AtlasRelatedCategoryHeader> expected = Collections.singletonList(new AtlasRelatedCategoryHeader());

        when(mockGlossaryService.getGlossaryCategoriesHeaders(VALID_GLOSSARY_GUID, 0, -1, SortOrder.ASCENDING)).thenReturn(expected);

        List<AtlasRelatedCategoryHeader> actual = glossaryREST.getGlossaryCategoriesHeaders(VALID_GLOSSARY_GUID, "-1", "0", "ASC");

        assertEquals(actual, expected);
        verify(mockGlossaryService).getGlossaryCategoriesHeaders(VALID_GLOSSARY_GUID, 0, -1, SortOrder.ASCENDING);
    }

    @Test
    public void testGetGlossaryCategoriesHeaders_InvalidGuid_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 3, 'x');

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.getGlossaryCategoriesHeaders(longGuid, "10", "0", "ASC"));

        TestUtility.assertInvalidParamLength(ex, "glossaryGuid");
    }

    @Test
    public void testGetCategoryTerms_Success() throws AtlasBaseException {
        String categoryGuid = "category-123";
        List<AtlasRelatedTermHeader> expectedResult = new ArrayList<>();
        expectedResult.add(new AtlasRelatedTermHeader());

        when(mockGlossaryService.getCategoryTerms(categoryGuid, 0, -1, SortOrder.ASCENDING)).thenReturn(expectedResult);

        List<AtlasRelatedTermHeader> actualResult = glossaryREST.getCategoryTerms(categoryGuid, "-1", "0", "ASC");

        assertEquals(actualResult, expectedResult);
        verify(mockGlossaryService).getCategoryTerms(categoryGuid, 0, -1, SortOrder.ASCENDING);
    }

    @Test
    public void testGetCategoryTerms_InvalidGuid_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'c');

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.getCategoryTerms(longGuid, "10", "0", "DESC"));

        TestUtility.assertInvalidParamLength(ex, "categoryGuid");
    }

    @Test
    public void testGetRelatedTerms_Success() throws AtlasBaseException {
        String termGuid = "term-456";
        Map<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> expected = new EnumMap<>(AtlasGlossaryTerm.Relation.class);
        Set<AtlasRelatedTermHeader> headers = new HashSet<>();
        headers.add(new AtlasRelatedTermHeader());

        expected.put(AtlasGlossaryTerm.Relation.SYNONYMS, headers);
        when(mockGlossaryService.getRelatedTerms(termGuid, 0, -1, SortOrder.ASCENDING)).thenReturn(expected);

        Map<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> result = glossaryREST.getRelatedTerms(termGuid, "-1", "0", "ASC");

        assertEquals(expected, result);
    }

    @Test
    public void testGetRelatedTerms_InvalidGuid_ThrowsException() {
        String longGuid = TestUtility.generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 't');

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> glossaryREST.getRelatedTerms(longGuid, "10", "0", "DESC"));

        TestUtility.assertInvalidParamLength(ex, "termGuid");
    }

    @Test
    public void testGetEntitiesAssignedWithTerm_Success() throws AtlasBaseException {
        String termGuid = "term-123";
        String limit = "10";
        String offset = "0";
        String sort = "ASC";
        AtlasRelatedObjectId obj = new AtlasRelatedObjectId();
        obj.setGuid("id1");

        List<AtlasRelatedObjectId> mockEntities = Arrays.asList(obj, new AtlasRelatedObjectId());

        when(mockGlossaryService.getAssignedEntities(termGuid, Integer.parseInt(offset), Integer.parseInt(limit), SortOrder.ASCENDING)).thenReturn(mockEntities);

        List<AtlasRelatedObjectId> actualResult = glossaryREST.getEntitiesAssignedWithTerm(termGuid, limit, offset, sort);

        assertNotNull(actualResult);
        assertEquals(actualResult.size(), 2);
        assertEquals(actualResult.get(0).getGuid(), "id1");

        verify(mockGlossaryService).getAssignedEntities(termGuid, 0, 10, SortOrder.ASCENDING);
    }

    @Test
    public void testAssignTermToEntities_Success() throws AtlasBaseException {
        String termGuid = "term-123";
        List<AtlasRelatedObjectId> relatedObjectIds = Collections.singletonList(new AtlasRelatedObjectId());

        // Execute the method under test
        glossaryREST.assignTermToEntities(termGuid, relatedObjectIds);

        // Verify glossaryService interaction
        verify(mockGlossaryService).assignTermToEntities(termGuid, relatedObjectIds);
    }

    @Test
    public void testRemoveTermAssignmentFromEntities_Success() throws AtlasBaseException {
        String termGuid = "term-001";
        List<AtlasRelatedObjectId> relatedObjectIds = Collections.singletonList(new AtlasRelatedObjectId());
        // Test removeTermAssignmentFromEntities
        glossaryREST.removeTermAssignmentFromEntities(termGuid, relatedObjectIds);

        verify(mockGlossaryService).removeTermFromEntities(termGuid, relatedObjectIds);
    }

    @Test
    public void testDisassociateTermAssignmentFromEntities_Success() throws AtlasBaseException {
        String termGuid = "term-002";
        List<AtlasRelatedObjectId> relatedObjectIds = Collections.singletonList(new AtlasRelatedObjectId());

        // Test disassociateTermAssignmentFromEntities
        glossaryREST.disassociateTermAssignmentFromEntities(termGuid, relatedObjectIds);

        verify(mockGlossaryService).removeTermFromEntities(termGuid, relatedObjectIds);
    }

    @Test
    public void testGetRelatedCategories_Success() throws AtlasBaseException {
        // Inputs
        String categoryGuid = "category-guid-1";
        String limit = "10";
        String offset = "0";
        String sort = "ASC";

        // Expected output
        Map<String, List<AtlasRelatedCategoryHeader>> expectedMap = new HashMap<>();
        AtlasRelatedCategoryHeader relatedCategoryHeader = new AtlasRelatedCategoryHeader();
        relatedCategoryHeader.setCategoryGuid(categoryGuid);
        relatedCategoryHeader.setDisplayText("children-of-category-guid-1");

        expectedMap.put("children", Collections.singletonList(relatedCategoryHeader));

        // Mocks
        when(mockGlossaryService.getRelatedCategories(eq(categoryGuid), eq(0), eq(10), eq(SortOrder.ASCENDING)))
                .thenReturn(expectedMap);

        // Act
        Map<String, List<AtlasRelatedCategoryHeader>> actualResult = glossaryREST.getRelatedCategories(categoryGuid, limit, offset, sort);

        // Assert
        assertNotNull(actualResult);
        assertEquals(actualResult.size(), 1);
        assertTrue(actualResult.containsKey("children"));
        assertEquals(actualResult.get("children").get(0).getCategoryGuid(), "category-guid-1");

        // Verify service method was called
        verify(mockGlossaryService).getRelatedCategories(categoryGuid, 0, 10, SortOrder.ASCENDING);
    }

    @Test
    public void testProduceTemplateWritesExpectedCSV_Success() throws IOException {
        // Act
        StreamingOutput output = glossaryREST.produceTemplate();

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        output.write(byteStream);

        // Assert
        String actualOutput = byteStream.toString(String.valueOf(StandardCharsets.UTF_8));
        String expectedOutput = GlossaryTermUtils.getGlossaryTermHeaders(); // Should match exactly
        assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testImportGlossaryData() throws Exception {
        // Arrange
        String testFileName = "glossary.csv";
        String fileContent = "dummy glossary data";
        InputStream inputStream = new ByteArrayInputStream(fileContent.getBytes());

        FormDataContentDisposition fileDetail = mock(FormDataContentDisposition.class);
        when(fileDetail.getFileName()).thenReturn(testFileName);

        BulkImportResponse expectedResponse = new BulkImportResponse();
        // Set expected fields if needed
        when(mockGlossaryService.importGlossaryData(inputStream, testFileName)).thenReturn(expectedResponse);

        // Act
        BulkImportResponse result = glossaryREST.importGlossaryData(inputStream, fileDetail);

        // Assert
        assertNotNull(result);
        assertEquals(expectedResponse, result);
        verify(mockGlossaryService).importGlossaryData(inputStream, testFileName);
    }
}
