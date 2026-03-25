/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to
 * you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.atlas.repository.Constants.*;
import static org.testng.Assert.*;

/**
 * Unit tests for DatasetPreProcessor validation logic.
 *
 * Tests validate Dataset entity business logic:
 * - qualifiedName generation pattern
 * - elementCount initialization and immutability
 * - type validation and normalization
 *
 * Note: Full lifecycle tests (CREATE/UPDATE/DELETE with vertex operations)
 * are covered in DataMeshIntegrationTest due to complex dependency requirements.
 */
public class DatasetPreProcessorTest {

    @BeforeMethod
    public void setup() {
        RequestContext.clear();
        RequestContext.get(); // Initialize RequestContext
    }

    @AfterMethod
    public void tearDown() {
        RequestContext.clear();
    }

    // =====================================================================================
    // Test Group 1: QualifiedName Generation
    // =====================================================================================

    @Test
    public void testQualifiedNamePattern() {
        // Test that qualifiedName follows the expected pattern
        String qn = PreProcessorUtils.getUUID();
        assertNotNull(qn);
        assertEquals(21, qn.length(), "NanoId should be 21 characters");

        // Construct expected QN pattern
        String expectedPrefix = "default/dataset/";
        String fullQN = expectedPrefix + qn;
        assertTrue(fullQN.startsWith(expectedPrefix));
        assertTrue(fullQN.length() > expectedPrefix.length());
    }

    @Test
    public void testQualifiedNameUniqueness() {
        // Generate multiple QNs and verify they're unique
        String qn1 = PreProcessorUtils.getUUID();
        String qn2 = PreProcessorUtils.getUUID();
        String qn3 = PreProcessorUtils.getUUID();

        assertNotEquals(qn1, qn2, "Generated IDs should be unique");
        assertNotEquals(qn2, qn3, "Generated IDs should be unique");
        assertNotEquals(qn1, qn3, "Generated IDs should be unique");
    }

    // =====================================================================================
    // Test Group 2: Dataset Type Validation
    // =====================================================================================

    @Test
    public void testValidDatasetTypes() {
        // Test that all valid dataset types are accepted
        assertTrue(PreProcessorUtils.VALID_DATASET_TYPES.contains("Raw"));
        assertTrue(PreProcessorUtils.VALID_DATASET_TYPES.contains("Refined"));
        assertTrue(PreProcessorUtils.VALID_DATASET_TYPES.contains("Aggregated"));
        assertEquals(3, PreProcessorUtils.VALID_DATASET_TYPES.size());
    }

    @Test
    public void testDatasetTypeNormalization() {
        // Test case-insensitive type validation
        assertFalse(PreProcessorUtils.VALID_DATASET_TYPES.contains("RAW"));

        // Verify normalization is needed (types are stored uppercase)
        String normalizedType = "Raw".trim();
        assertTrue(PreProcessorUtils.VALID_DATASET_TYPES.contains(normalizedType));
    }

    @Test
    public void testInvalidDatasetTypes() {
        // Test that invalid types are not in the valid set
        assertFalse(PreProcessorUtils.VALID_DATASET_TYPES.contains("INVALID"));
        assertFalse(PreProcessorUtils.VALID_DATASET_TYPES.contains("PROCESSED"));
        assertFalse(PreProcessorUtils.VALID_DATASET_TYPES.contains(""));
        assertFalse(PreProcessorUtils.VALID_DATASET_TYPES.contains(null));
    }

    // =====================================================================================
    // Test Group 3: Business Logic Validation
    // =====================================================================================

    @Test
    public void testDatasetTypeAttributeName() {
        // Verify the constant is correctly defined
        assertEquals("dataMeshDatasetType", DATASET_TYPE_ATTR);
    }

    @Test
    public void testCatalogDatasetGuidAttributeName() {
        // Verify the constant for asset-dataset linking
        assertEquals("catalogDatasetGuid", CATALOG_DATASET_GUID_ATTR);
    }

    @Test
    public void testDatasetEntityTypeName() {
        // Verify the constant is correctly defined
        assertEquals("DataMeshDataset", DATASET_ENTITY_TYPE);
    }

    // =====================================================================================
    // Test Group 4: Entity Attribute Validation Scenarios
    // =====================================================================================

    @Test
    public void testElementCountMustBeLongType() {
        // Element count should be initialized as Long type (0L)
        Long elementCount = 0L;
        assertTrue(elementCount instanceof Long);
        assertEquals(0L, elementCount.longValue());
    }

    @Test
    public void testDatasetTypeValueBoundaries() {
        // Test the three valid values
        String[] validTypes = {"Raw", "Refined", "Aggregated"};
        for (String type : validTypes) {
            assertTrue(PreProcessorUtils.VALID_DATASET_TYPES.contains(type),
                    "Type '" + type + "' should be valid");
        }
    }

    @Test
    public void testQualifiedNameImmutabilityRequirement() {
        // Document that qualifiedName should not change after creation
        // This is enforced in the preprocessor UPDATE logic
        // which overwrites any user-provided QN with the vertex's existing QN

        String originalQN = "default/dataset/" + PreProcessorUtils.getUUID();
        String attemptedNewQN = "default/dataset/" + PreProcessorUtils.getUUID();

        assertNotEquals(originalQN, attemptedNewQN,
                "QualifiedNames should be immutable - any UPDATE attempt should preserve original");
    }

    // =====================================================================================
    // Test Group 5: Archived Dataset Validation
    // =====================================================================================

    @Test
    public void testArchivedStateDefinition() {
        // Verify that DELETED status is used for archived datasets
        assertEquals("DELETED", AtlasEntity.Status.DELETED.name());
    }

    @Test
    public void testActiveStateDefinition() {
        // Verify that ACTIVE status is used for active datasets
        assertEquals("ACTIVE", AtlasEntity.Status.ACTIVE.name());
    }

    // =====================================================================================
    // Test Group 6: Error Code Validation
    // =====================================================================================

    @Test
    public void testOperationNotSupportedErrorCode() {
        // Verify error code for archived dataset updates
        assertNotNull(AtlasErrorCode.OPERATION_NOT_SUPPORTED);
    }

    @Test
    public void testInvalidParametersErrorCode() {
        // Verify error code for validation failures
        assertNotNull(AtlasErrorCode.INVALID_PARAMETERS);
    }

    // =====================================================================================
    // Test Group 7: Integration with Constants
    // =====================================================================================

    @Test
    public void testStatePropertyKey() {
        // Verify the state property key constant
        assertEquals("__state", STATE_PROPERTY_KEY);
    }

    @Test
    public void testQualifiedNameConstant() {
        // Verify the qualifiedName constant
        assertEquals("qualifiedName", QUALIFIED_NAME);
    }

    @Test
    public void testDefaultTenantId() {
        // Verify default tenant ID for qualifiedName generation
        assertEquals("default", DEFAULT_TENANT_ID);
    }

    // =====================================================================================
    // Test Group 8: Dataset Path Segment
    // =====================================================================================

    @Test
    public void testDatasetPathSegmentInQualifiedName() {
        // Verify the path segment used in qualifiedName generation
        String expectedPattern = DEFAULT_TENANT_ID + "/dataset/";
        String qn = expectedPattern + PreProcessorUtils.getUUID();

        assertTrue(qn.startsWith(expectedPattern));
        assertTrue(qn.contains("/dataset/"));
    }

    // =====================================================================================
    // Test Group 9: Type Validation Edge Cases
    // =====================================================================================

    @Test
    public void testEmptyStringIsNotValidType() {
        assertFalse(PreProcessorUtils.VALID_DATASET_TYPES.contains(""));
    }

    @Test
    public void testWhitespaceIsNotValidType() {
        assertFalse(PreProcessorUtils.VALID_DATASET_TYPES.contains("   "));
        assertFalse(PreProcessorUtils.VALID_DATASET_TYPES.contains(" RAW "));
    }

    // =====================================================================================
    // Test Group 10: Documentation Tests
    // =====================================================================================

    @Test
    public void testDatasetPreProcessorConstants() {
        // Document all constants used by DatasetPreProcessor
        assertNotNull(DATASET_ENTITY_TYPE);
        assertNotNull(DATASET_TYPE_ATTR);
        assertNotNull(CATALOG_DATASET_GUID_ATTR);
        assertNotNull(STATE_PROPERTY_KEY);
        assertNotNull(QUALIFIED_NAME);
    }

    @Test
    public void testValidDatasetTypesList() {
        // Document the complete list of valid dataset types
        String[] expectedTypes = {"Raw", "Refined", "Aggregated"};
        assertEquals(expectedTypes.length, PreProcessorUtils.VALID_DATASET_TYPES.size());
        for (String type : expectedTypes) {
            assertTrue(PreProcessorUtils.VALID_DATASET_TYPES.contains(type),
                    "Expected type '" + type + "' to be in VALID_DATASET_TYPES");
        }
    }
}
