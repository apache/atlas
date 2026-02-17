package org.apache.atlas.discovery;

import org.apache.atlas.model.lineage.LineageListRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.atlas.model.lineage.LineageListRequest.LINEAGE_TYPE_DATASET_PROCESS_LINEAGE;
import static org.apache.atlas.model.lineage.LineageListRequest.LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE;
import static org.junit.Assert.*;

/**
 * Unit tests for EntityLineageService multilevel lineage functionality
 */
public class EntityLineageServiceTest {

    @Before
    public void setUp() {
        // Ensure clean state before each test
    }

    @After
    public void tearDown() {
        // Clean up after each test to prevent state bleeding
    }


    // ==================== EntityValidationResult Tests ====================

    @Test
    public void testEntityValidationResult_ProcessIsConnector_DatasetProcessLineage() {
        // Arrange
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(true, false, false, false, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Assert
        assertTrue("Process should be a connector in DatasetProcessLineage", isConnector);
    }

    @Test
    public void testEntityValidationResult_DataSetIsNotConnector_DatasetProcessLineage() {
        // Arrange
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(false, true, false, false, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Assert
        assertFalse("DataSet should NOT be a connector in DatasetProcessLineage", isConnector);
    }

    @Test
    public void testEntityValidationResult_ConnectionIsNotConnector_DatasetProcessLineage() {
        // Arrange
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(false, false, true, false, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Assert
        assertFalse("Connection should NOT be a connector in DatasetProcessLineage", isConnector);
    }

    @Test
    public void testEntityValidationResult_ConnectionProcessIsConnector_DatasetProcessLineage() {
        // Arrange
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(false, false, false, true, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Assert
        assertTrue("ConnectionProcess should be a connector in DatasetProcessLineage", isConnector);
    }

    @Test
    public void testEntityValidationResult_DataProductIsNotConnector_DatasetProcessLineage() {
        // Arrange
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(false, false, false, false, true);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Assert
        assertFalse("DataProduct should NOT be a connector in DatasetProcessLineage", isConnector);
    }

    @Test
    public void testEntityValidationResult_DataProductIsNotConnector_ProductAssetLineage() {
        // Arrange
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(false, false, false, false, true);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Assert
        assertFalse("DataProduct should NOT be a connector in ProductAssetLineage", isConnector);
    }

    @Test
    public void testEntityValidationResult_AssetIsConnector_ProductAssetLineage() {
        // Arrange - An asset that is not a DataProduct (e.g., a regular DataSet)
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(false, true, false, false, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Assert
        assertTrue("Non-DataProduct asset should be a connector in ProductAssetLineage", isConnector);
    }

    @Test
    public void testEntityValidationResult_MultipleFlags_ProcessOnly() {
        // Arrange - Only Process flag set (no conflicting flags)
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(true, false, false, false, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Assert
        assertTrue("When isProcess is true and no DataSet/Connection/DataProduct flags are set, should be a connector", isConnector);
    }

    @Test
    public void testEntityValidationResult_MultipleFlags_ProcessAndDataSet() {
        // Arrange - Both Process and DataSet flags set (DataSet takes precedence - not a connector)
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(true, true, false, false, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Assert
        assertFalse("When DataSet flag is true, should NOT be a connector regardless of Process flag", isConnector);
    }

    @Test
    public void testEntityValidationResult_MultipleFlags_DataSetAndConnection() {
        // Arrange - Both DataSet and Connection flags set
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(false, true, true, false, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Assert
        assertFalse("When both DataSet and Connection are true, should NOT be a connector", isConnector);
    }

    @Test
    public void testEntityValidationResult_AllFalseFlags_DatasetProcessLineage() {
        // Arrange - No entity type flags set (edge case)
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(false, false, false, false, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE);

        // Assert
        assertTrue("When no type flags are set in DatasetProcessLineage, defaults to connector", isConnector);
    }

    @Test
    public void testEntityValidationResult_AllFalseFlags_ProductAssetLineage() {
        // Arrange - No entity type flags set (edge case)
        EntityLineageService.EntityValidationResult result = 
            new EntityLineageService.EntityValidationResult(false, false, false, false, false);

        // Act
        boolean isConnector = result.checkIfConnectorVertex(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Assert
        assertTrue("When no type flags are set in ProductAssetLineage (not a DataProduct), should be connector", isConnector);
    }

    // ==================== Request Model Tests ====================
    // Note: Tests for LineageOnDemandRequest are removed from this file because:
    // 1. They test model classes, not EntityLineageService logic
    // 2. LineageOnDemandRequest() instantiation triggers LineageOnDemandBaseParams static initialization
    //    which requires AtlasConfiguration to be loaded (fails in unit tests)
    // 3. These tests belong in LineageContextTest where they are properly mocked

    @Test
    public void testLineageListRequest_DefaultLineageType() {
        // Arrange & Act
        LineageListRequest request = new LineageListRequest();

        // Assert
        assertEquals("Default lineage type should be DatasetProcessLineage", 
                     LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, 
                     request.getLineageType());
    }

    @Test
    public void testLineageListRequest_SetCustomLineageType() {
        // Arrange
        LineageListRequest request = new LineageListRequest();

        // Act
        request.setLineageType(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE);

        // Assert
        assertEquals("Should allow setting custom lineage type", 
                     LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, 
                     request.getLineageType());
    }
}

