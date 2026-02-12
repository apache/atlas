package org.apache.atlas.discovery;

import org.apache.atlas.model.lineage.LineageListRequest;
import org.apache.atlas.model.lineage.LineageOnDemandRequest;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;

import static org.apache.atlas.model.lineage.LineageListRequest.LINEAGE_TYPE_DATASET_PROCESS_LINEAGE;
import static org.apache.atlas.model.lineage.LineageListRequest.LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for lineage context classes (AtlasLineageContext, AtlasLineageListContext, AtlasLineageOnDemandContext)
 * to verify lineageType field handling
 */
public class LineageContextTest {

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Helper method to create a properly initialized LineageListRequest
     */
    private LineageListRequest createLineageListRequest() {
        LineageListRequest request = new LineageListRequest();
        request.setGuid("test-guid-123");
        request.setSize(10);
        request.setFrom(0);
        request.setDepth(3);
        request.setDirection(LineageListRequest.LineageDirection.INPUT);
        return request;
    }

    /**
     * Helper method to create a fully mocked LineageOnDemandRequest
     * Note: Uses Mockito to completely avoid triggering LineageOnDemandBaseParams static initialization
     * which requires AtlasConfiguration to be loaded. LineageOnDemandBaseParams has a static field
     * that calls AtlasConfiguration.LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT.getInt() which fails in unit tests.
     * 
     * We cannot import LineageOnDemandBaseParams or LineageOnDemandConstraints (which extends it)
     * as even importing these classes will trigger the static initialization.
     */
    private LineageOnDemandRequest createLineageOnDemandRequest() {
        // Fully mock the request to avoid any static initialization
        LineageOnDemandRequest request = mock(LineageOnDemandRequest.class);
        
        // Set up the mocked behavior that AtlasLineageOnDemandContext constructor needs
        when(request.getLineageType()).thenReturn(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);
        when(request.getAttributes()).thenReturn(new HashSet<>());
        when(request.getRelationAttributes()).thenReturn(new HashSet<>());
        when(request.getConstraints()).thenReturn(null);
        when(request.getEntityTraversalFilters()).thenReturn(null);
        when(request.getRelationshipTraversalFilters()).thenReturn(null);
        when(request.getDefaultParams()).thenReturn(null); // Return null to avoid needing LineageOnDemandBaseParams
        
        return request;
    }

    // ==================== AtlasLineageListContext Tests ====================

    @Test
    public void testAtlasLineageListContext_DefaultLineageType() {
        // Arrange
        LineageListRequest request = createLineageListRequest();
        // Default should be DatasetProcessLineage

        // Act
        AtlasLineageListContext context = new AtlasLineageListContext(request, mockTypeRegistry);

        // Assert
        assertEquals("Default lineage type should be DatasetProcessLineage", 
                     LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, 
                     context.getLineageType());
    }

    @Test
    public void testAtlasLineageListContext_ProductAssetLineage() {
        // Arrange
        LineageListRequest request = createLineageListRequest();
        request.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Act
        AtlasLineageListContext context = new AtlasLineageListContext(request, mockTypeRegistry);

        // Assert
        assertEquals("Should preserve ProductAssetLineage type", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     context.getLineageType());
    }

    @Test
    public void testAtlasLineageListContext_DatasetProcessLineage() {
        // Arrange
        LineageListRequest request = createLineageListRequest();
        request.setLineageType(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Act
        AtlasLineageListContext context = new AtlasLineageListContext(request, mockTypeRegistry);

        // Assert
        assertEquals("Should preserve DatasetProcessLineage type", 
                     LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, 
                     context.getLineageType());
    }

    @Test
    public void testAtlasLineageListContext_SetLineageType() {
        // Arrange
        LineageListRequest request = createLineageListRequest();
        AtlasLineageListContext context = new AtlasLineageListContext(request, mockTypeRegistry);

        // Act
        context.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Assert
        assertEquals("Should allow setting lineage type", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     context.getLineageType());
    }

    // ==================== AtlasLineageOnDemandContext Tests ====================

    @Test
    public void testAtlasLineageOnDemandContext_DefaultLineageType() {
        // Arrange
        LineageOnDemandRequest request = createLineageOnDemandRequest();
        // Default should be DatasetProcessLineage

        // Act
        AtlasLineageOnDemandContext context = new AtlasLineageOnDemandContext(request, mockTypeRegistry);

        // Assert
        assertEquals("Default lineage type should be DatasetProcessLineage", 
                     LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, 
                     context.getLineageType());
    }

    @Test
    public void testAtlasLineageOnDemandContext_ProductAssetLineage() {
        // Arrange
        LineageOnDemandRequest request = createLineageOnDemandRequest();
        // Override the mock to return ProductAssetLineage
        when(request.getLineageType()).thenReturn(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Act
        AtlasLineageOnDemandContext context = new AtlasLineageOnDemandContext(request, mockTypeRegistry);

        // Assert
        assertEquals("Should preserve ProductAssetLineage type", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     context.getLineageType());
    }

    @Test
    public void testAtlasLineageOnDemandContext_DatasetProcessLineage() {
        // Arrange
        LineageOnDemandRequest request = createLineageOnDemandRequest();
        // Already returns DatasetProcessLineage by default from createLineageOnDemandRequest()

        // Act
        AtlasLineageOnDemandContext context = new AtlasLineageOnDemandContext(request, mockTypeRegistry);

        // Assert
        assertEquals("Should preserve DatasetProcessLineage type", 
                     LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, 
                     context.getLineageType());
    }

    @Test
    public void testAtlasLineageOnDemandContext_SetLineageType() {
        // Arrange
        LineageOnDemandRequest request = createLineageOnDemandRequest();
        AtlasLineageOnDemandContext context = new AtlasLineageOnDemandContext(request, mockTypeRegistry);

        // Act
        context.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Assert
        assertEquals("Should allow setting lineage type", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     context.getLineageType());
    }

    // ==================== AtlasLineageContext Tests ====================
    // Note: AtlasLineageContext does not have lineageType field/methods.
    // Lineage type is handled by AtlasLineageListContext and AtlasLineageOnDemandContext only.
    // The old AtlasLineageContext is used for the legacy graph-based lineage API which always
    // uses DatasetProcessLineage (Process.inputs/outputs edges).

    // ==================== State Isolation Tests ====================

    @Test
    public void testMultipleContexts_NoStateBleed() {
        // Arrange - create two different contexts
        LineageListRequest request1 = createLineageListRequest();
        request1.setLineageType(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);
        
        LineageListRequest request2 = createLineageListRequest();
        request2.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Act
        AtlasLineageListContext context1 = new AtlasLineageListContext(request1, mockTypeRegistry);
        AtlasLineageListContext context2 = new AtlasLineageListContext(request2, mockTypeRegistry);

        // Assert - each context should maintain its own lineage type
        assertEquals("Context 1 should have DatasetProcessLineage", 
                     LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, 
                     context1.getLineageType());
        assertEquals("Context 2 should have ProductAssetLineage", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     context2.getLineageType());
    }

    @Test
    public void testContextModification_DoesNotAffectRequest() {
        // Arrange
        LineageListRequest request = createLineageListRequest();
        request.setLineageType(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);
        AtlasLineageListContext context = new AtlasLineageListContext(request, mockTypeRegistry);

        // Act - modify context
        context.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Assert - original request should be unchanged
        assertEquals("Original request should remain unchanged", 
                     LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, 
                     request.getLineageType());
        assertEquals("Context should have new value", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     context.getLineageType());
    }

    // ==================== Consistency Tests ====================

    @Test
    public void testAllContextTypes_UseConsistentDefaults() {
        // Arrange & Act
        LineageListRequest listRequest = createLineageListRequest();
        LineageOnDemandRequest onDemandRequest = createLineageOnDemandRequest();

        AtlasLineageListContext listContext = new AtlasLineageListContext(listRequest, mockTypeRegistry);
        AtlasLineageOnDemandContext onDemandContext = new AtlasLineageOnDemandContext(onDemandRequest, mockTypeRegistry);

        // Assert - both should default to the same lineage type
        assertEquals("All context types should have consistent defaults", 
                     LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, 
                     listContext.getLineageType());
        assertEquals("All context types should have consistent defaults", 
                     LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, 
                     onDemandContext.getLineageType());
    }

    @Test
    public void testAllContextTypes_CanSetProductAssetLineage() {
        // Arrange
        LineageListRequest listRequest = createLineageListRequest();
        listRequest.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);
        
        LineageOnDemandRequest onDemandRequest = createLineageOnDemandRequest();
        // Override mock to return ProductAssetLineage
        when(onDemandRequest.getLineageType()).thenReturn(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Act
        AtlasLineageListContext listContext = new AtlasLineageListContext(listRequest, mockTypeRegistry);
        AtlasLineageOnDemandContext onDemandContext = new AtlasLineageOnDemandContext(onDemandRequest, mockTypeRegistry);

        // Assert - both should support ProductAssetLineage
        assertEquals("ListContext should support ProductAssetLineage", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     listContext.getLineageType());
        assertEquals("OnDemandContext should support ProductAssetLineage", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     onDemandContext.getLineageType());
    }

    // ==================== Edge Cases ====================

    @Test
    public void testContextWithNullRegistry() {
        // Arrange
        LineageListRequest request = createLineageListRequest();
        request.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Act - passing null registry (test that lineage type still works)
        AtlasLineageListContext context = new AtlasLineageListContext(request, null);

        // Assert - lineage type should still be set correctly
        assertEquals("Lineage type should be set even with null registry", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     context.getLineageType());
    }

    @Test
    public void testContextSetLineageType_MultipleChanges() {
        // Arrange
        LineageListRequest request = createLineageListRequest();
        AtlasLineageListContext context = new AtlasLineageListContext(request, mockTypeRegistry);

        // Act - change lineage type multiple times
        context.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);
        assertEquals(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, context.getLineageType());
        
        context.setLineageType(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);
        assertEquals(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, context.getLineageType());
        
        context.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Assert - final value should be the last set
        assertEquals("Should allow changing lineage type multiple times", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     context.getLineageType());
    }
}
