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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Test;

import java.util.*;

import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.*;
import static org.junit.Assert.*;

/**
 * Unit tests for DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct
 * 
 * Tests validate that archived DataProducts only allow business metadata removals (null values),
 * while blocking additions or updates (non-null values).
 */
public class DataProductPreProcessorTest {

    @After
    public void tearDown() {
        // Ensure no state bleeding between tests
    }

    // =====================================================================================
    // Test Group 1: Null/Empty Input Cases (should pass)
    // =====================================================================================

    @Test
    public void testValidateWithNullVertex() throws AtlasBaseException {
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "owner", "John");
        
        // Should not throw exception - null vertex is ignored
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(null, businessAttributes);
    }

    @Test
    public void testValidateWithNullBusinessAttributes() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        
        // Should not throw exception - null business attributes is ignored
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, null);
    }

    @Test
    public void testValidateWithEmptyBusinessAttributes() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> emptyAttributes = new HashMap<>();
        
        // Should not throw exception - empty map is ignored
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, emptyAttributes);
    }

    // =====================================================================================
    // Test Group 2: Non-DataProduct Entities (should pass)
    // =====================================================================================

    @Test
    public void testValidateWithNonDataProductEntity() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubVertex("Table", AtlasEntity.Status.DELETED.name());
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "owner", "John");
        
        // Should not throw exception - non-DataProduct entities are ignored
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    @Test
    public void testValidateWithArchivedTableEntity() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubVertex("hive_table", AtlasEntity.Status.DELETED.name());
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("DataQuality", "score", 95);
        
        // Should not throw exception - only DataProduct validation applies
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    // =====================================================================================
    // Test Group 3: Active DataProduct (should pass)
    // =====================================================================================

    @Test
    public void testValidateWithActiveDataProduct() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(false); // Active state
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "owner", "John");
        
        // Should not throw exception - active DataProducts allow all updates
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    @Test
    public void testValidateWithActiveDataProductMultipleAttributes() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(false);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        Map<String, Object> piiAttrs = new HashMap<>();
        piiAttrs.put("owner", "John");
        piiAttrs.put("classification", "Sensitive");
        businessAttributes.put("PII", piiAttrs);
        
        Map<String, Object> qualityAttrs = new HashMap<>();
        qualityAttrs.put("score", 95);
        businessAttributes.put("DataQuality", qualityAttrs);
        
        // Should not throw exception - active DataProducts allow all updates
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    // =====================================================================================
    // Test Group 4: Archived DataProduct with null values only (should pass)
    // =====================================================================================

    @Test
    public void testValidateArchivedDataProductWithSingleNullValue() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "owner", null);
        
        // Should not throw exception - null values (removal) are allowed on archived products
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    @Test
    public void testValidateArchivedDataProductWithAllNullValues() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        Map<String, Object> piiAttrs = new HashMap<>();
        piiAttrs.put("owner", null);
        piiAttrs.put("classification", null);
        piiAttrs.put("dataType", null);
        businessAttributes.put("PII", piiAttrs);
        
        // Should not throw exception - all null values are removals
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    @Test
    public void testValidateArchivedDataProductWithMultipleBMTypesAllNull() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        Map<String, Object> piiAttrs = new HashMap<>();
        piiAttrs.put("owner", null);
        businessAttributes.put("PII", piiAttrs);
        
        Map<String, Object> qualityAttrs = new HashMap<>();
        qualityAttrs.put("score", null);
        qualityAttrs.put("lastValidated", null);
        businessAttributes.put("DataQuality", qualityAttrs);
        
        // Should not throw exception - all null values across multiple BM types
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    @Test
    public void testValidateArchivedDataProductWithEmptyAttributeMap() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        businessAttributes.put("PII", new HashMap<>()); // Empty map for BM type
        
        // Should not throw exception - empty map means remove all attributes
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    // =====================================================================================
    // Test Group 5: Archived DataProduct with non-null values (should fail)
    // =====================================================================================

    @Test
    public void testValidateArchivedDataProductWithNonNullStringValue() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "owner", "John");
        
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for non-null value on archived DataProduct");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
            assertTrue("Error message should mention custom metadata update restriction", 
                e.getMessage().contains("Cannot add or update custom metadata on archived DataProduct"));
        }
    }

    @Test
    public void testValidateArchivedDataProductWithNonNullNumericValue() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("DataQuality", "score", 95);
        
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for non-null numeric value on archived DataProduct");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
            assertTrue(e.getMessage().contains("Cannot add or update custom metadata on archived DataProduct"));
        }
    }

    @Test
    public void testValidateArchivedDataProductWithNonNullBooleanValue() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "isConfidential", true);
        
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for non-null boolean value on archived DataProduct");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    @Test
    public void testValidateArchivedDataProductWithNonNullListValue() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "tags", 
            Arrays.asList("tag1", "tag2"));
        
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for non-null list value on archived DataProduct");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    // =====================================================================================
    // Test Group 6: Mixed scenarios (should handle intelligently)
    // =====================================================================================

    @Test
    public void testValidateArchivedDataProductWithMixedNullAndNonNull() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("owner", null);           // Removal - OK
        attrs.put("classification", "PII"); // Update - NOT OK
        businessAttributes.put("PII", attrs);
        
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException when mixing null and non-null values");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
            assertTrue(e.getMessage().contains("Cannot add or update custom metadata on archived DataProduct"));
        }
    }

    @Test
    public void testValidateArchivedDataProductWithMultipleBMTypesOnlyOneHasNonNull() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        // PII - all null (OK)
        Map<String, Object> piiAttrs = new HashMap<>();
        piiAttrs.put("owner", null);
        businessAttributes.put("PII", piiAttrs);
        
        // DataQuality - has non-null value (NOT OK)
        Map<String, Object> qualityAttrs = new HashMap<>();
        qualityAttrs.put("score", 95);
        businessAttributes.put("DataQuality", qualityAttrs);
        
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException when one BM type has non-null value");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    @Test
    public void testValidateArchivedDataProductWithOnlyNullsAcrossMultipleBMTypes() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        Map<String, Object> piiAttrs = new HashMap<>();
        piiAttrs.put("owner", null);
        piiAttrs.put("classification", null);
        businessAttributes.put("PII", piiAttrs);
        
        Map<String, Object> qualityAttrs = new HashMap<>();
        qualityAttrs.put("score", null);
        businessAttributes.put("DataQuality", qualityAttrs);
        
        // Should not throw exception - all values are null (removals)
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    // =====================================================================================
    // Test Group 7: Edge cases
    // =====================================================================================

    @Test
    public void testValidateArchivedDataProductWithEmptyString() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "owner", "");
        
        // Empty string is non-null, so should fail
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for empty string value");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    @Test
    public void testValidateArchivedDataProductWithZeroValue() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("DataQuality", "score", 0);
        
        // Zero is non-null, so should fail
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for zero value");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    @Test
    public void testValidateArchivedDataProductWithEmptyList() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "tags", 
            Collections.emptyList());
        
        // Empty list is non-null, so should fail
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for empty list value");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    @Test
    public void testValidateWithNullStateProperty() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubVertex(DATA_PRODUCT_ENTITY_TYPE, null);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("PII", "owner", "John");
        
        // Should not throw exception - null state is treated as not archived
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    // =====================================================================================
    // Test Group 8: Real-world scenarios
    // =====================================================================================

    @Test
    public void testScenario_RemovingAllAttributesFromArchivedProduct() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        // Simulates removing all attributes from PII business metadata
        Map<String, Object> piiAttrs = new HashMap<>();
        piiAttrs.put("owner", null);
        piiAttrs.put("classification", null);
        piiAttrs.put("dataType", null);
        piiAttrs.put("retentionPeriod", null);
        businessAttributes.put("PII", piiAttrs);
        
        // Should succeed - use case for attributeDef deletion
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    @Test
    public void testScenario_PartialUpdateOnArchivedProduct() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        // User tries to update one attribute while removing another
        Map<String, Object> piiAttrs = new HashMap<>();
        piiAttrs.put("owner", null);        // Removal - OK
        piiAttrs.put("classification", "Confidential"); // Update - NOT OK
        businessAttributes.put("PII", piiAttrs);
        
        // Should fail - any non-null value blocks the operation
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for partial update with non-null value");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    @Test
    public void testScenario_AddingNewAttributeToArchivedProduct() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = createBusinessAttributesWithValues("NewBM", "newAttribute", "value");
        
        // Should fail - adding new attributes not allowed
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for adding new attribute");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    @Test
    public void testScenario_UpdateViaSetBusinessAttributesOnArchivedProduct() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        // Simulates setBusinessAttributes call with one attribute to update
        Map<String, Object> piiAttrs = new HashMap<>();
        piiAttrs.put("owner", "UpdatedOwner");
        businessAttributes.put("PII", piiAttrs);
        
        // Should fail
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for setBusinessAttributes with non-null value");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    @Test
    public void testScenario_UpdateViaAddOrUpdateBusinessAttributesOnArchivedProduct() {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        // Simulates addOrUpdateBusinessAttributes call
        Map<String, Object> qualityAttrs = new HashMap<>();
        qualityAttrs.put("score", 85);
        qualityAttrs.put("lastChecked", "2024-01-01");
        businessAttributes.put("DataQuality", qualityAttrs);
        
        // Should fail
        try {
            DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
            fail("Expected AtlasBaseException for addOrUpdateBusinessAttributes with non-null values");
        } catch (AtlasBaseException e) {
            assertEquals(OPERATION_NOT_SUPPORTED, e.getAtlasErrorCode());
        }
    }

    @Test
    public void testScenario_RemoveViaRemoveBusinessAttributesOnArchivedProduct() throws AtlasBaseException {
        AtlasVertex stubVertex = createStubDataProductVertex(true);
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        
        // Simulates removeBusinessAttributes call - all values should be null
        Map<String, Object> piiAttrs = new HashMap<>();
        piiAttrs.put("owner", null);
        piiAttrs.put("classification", null);
        businessAttributes.put("PII", piiAttrs);
        
        // Should succeed - removeBusinessAttributes only sets null values
        DataProductPreProcessor.validateBusinessMetadataUpdateOnArchivedProduct(stubVertex, businessAttributes);
    }

    // =====================================================================================
    // Helper Methods (no state, creates fresh stubs each time)
    // =====================================================================================

    /**
     * Creates a stub DataProduct vertex with specified archived state.
     * 
     * @param isArchived true for DELETED state (archived), false for ACTIVE state
     * @return Stub AtlasVertex configured as a DataProduct
     */
    private AtlasVertex createStubDataProductVertex(boolean isArchived) {
        return createStubVertex(DATA_PRODUCT_ENTITY_TYPE, 
            isArchived ? AtlasEntity.Status.DELETED.name() : AtlasEntity.Status.ACTIVE.name());
    }

    /**
     * Creates a stub vertex with specified type and state.
     * Uses a simple stub implementation to avoid Mockito interface mocking issues.
     * 
     * @param typeName Entity type name
     * @param state Entity state (ACTIVE, DELETED, etc.)
     * @return Stub AtlasVertex
     */
    private AtlasVertex createStubVertex(String typeName, String state) {
        return new AtlasVertexStub(typeName, state);
    }

    /**
     * Creates a business attributes map with a single attribute.
     * 
     * @param bmName Business metadata type name
     * @param attrName Attribute name
     * @param attrValue Attribute value (can be null)
     * @return Map structure for business attributes
     */
    private Map<String, Map<String, Object>> createBusinessAttributesWithValues(
            String bmName, String attrName, Object attrValue) {
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(attrName, attrValue);
        businessAttributes.put(bmName, attrs);
        return businessAttributes;
    }

    /**
     * Simple stub implementation of AtlasVertex for testing.
     * Only implements methods needed for the validation logic.
     * Uses raw types to match how AtlasVertex is used in the codebase.
     */
    @SuppressWarnings("rawtypes")
    private static class AtlasVertexStub implements AtlasVertex {
        private final Map<String, Object> properties = new HashMap<>();

        public AtlasVertexStub(String typeName, String state) {
            properties.put(TYPE_NAME_PROPERTY_KEY, typeName);
            properties.put(STATE_PROPERTY_KEY, state);
        }

        @Override
        public <T> T getProperty(String propertyName, Class<T> clazz) {
            return clazz.cast(properties.get(propertyName));
        }

        // AtlasElement interface methods

        @Override
        public Object getId() { return null; }

        @Override
        public String getIdForDisplay() { return null; }

        @Override
        public <T> void setProperty(String propertyName, T value) {}

        @Override
        public Collection<? extends String> getPropertyKeys() { return null; }

        @Override
        public <T> Collection<T> getPropertyValues(String propertyName, Class<T> type) { return null; }

        @Override
        public List<String> getListProperty(String propertyName) { return null; }

        @Override
        public <V> List<V> getMultiValuedProperty(String propertyName, Class<V> elementType) { return null; }

        @Override
        public <V> Set<V> getMultiValuedSetProperty(String propertyName, Class<V> elementType) { return null; }

        @Override
        public <V> List<V> getListProperty(String propertyName, Class<V> elementType) { return null; }

        @Override
        public void setListProperty(String propertyName, List<String> values) {}

        @Override
        public void setPropertyFromElementsIds(String propertyName, List<org.apache.atlas.repository.graphdb.AtlasElement> values) {}

        @Override
        public void setPropertyFromElementId(String propertyName, org.apache.atlas.repository.graphdb.AtlasElement value) {}

        @Override
        public void removeProperty(String propertyName) {}

        @Override
        public void removePropertyValue(String propertyName, Object propertyValue) {}

        @Override
        public void removeAllPropertyValue(String propertyName, Object propertyValue) {}

        @Override
        public JSONObject toJson(Set<String> propertyKeys) throws JSONException { return null; }

        @Override
        public boolean exists() { return false; }

        @Override
        public <T> void setJsonProperty(String propertyName, T value) {}

        @Override
        public <T> T getJsonProperty(String propertyName) { return null; }

        @Override
        public boolean isIdAssigned() { return false; }

        @Override
        public <T> T getWrappedElement() { return null; }

        // AtlasVertex interface methods

        @Override
        public Iterable getEdges(AtlasEdgeDirection in) { return null; }

        @Override
        public void addProperty(String propertyName, Object value) {

        }


        @Override
        public Iterable getEdges(AtlasEdgeDirection out, String edgeLabel) { return null; }

        @Override
        public Iterable getEdges(AtlasEdgeDirection direction, String[] edgeLabels) { return null; }

        @Override
        public Set getInEdges(String[] edgeLabelsToExclude) { return null; }

        @Override
        public long getEdgesCount(AtlasEdgeDirection direction, String edgeLabel) { return 0; }

        @Override
        public boolean hasEdges(AtlasEdgeDirection dir, String edgeLabel) { return false; }
        

        @Override
        public void addListProperty(String propertyName, Object value) {

        }

        @Override
        public AtlasVertexQuery query() { return null; }

        @Override
        public Object getV() { return null; }
    }
}
