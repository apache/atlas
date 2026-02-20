package org.apache.atlas;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Unit tests for RequestContext lineage label functionality
 */
public class RequestContextLineageTest {

    private RequestContext requestContext;

    @BeforeMethod
    public void setUp() {
        // Create a fresh RequestContext for each test to prevent state bleeding
        // Use RequestContext.get() which uses ThreadLocal pattern
        requestContext = RequestContext.get();
        requestContext.clearCache(); // Ensure clean state
    }

    @AfterMethod
    public void tearDown() {
        // Clean up to prevent state bleeding between tests
        RequestContext.clear(); // Clear the ThreadLocal
        requestContext = null;
    }

    // ==================== Initial State Tests ====================

    @Test
    public void testLineageLabels_InitialState() {
        // Assert - should be empty strings by default
        assertEquals(requestContext.getLineageInputLabel(), 
                     "", 
                     "lineageInputLabel should be empty string initially");
        assertEquals(requestContext.getLineageOutputLabel(), 
                     "", 
                     "lineageOutputLabel should be empty string initially");
    }

    @Test
    public void testLineageLabels_NotNull() {
        // Assert - should never be null
        assertNotNull(requestContext.getLineageInputLabel(), "lineageInputLabel should not be null");
        assertNotNull(requestContext.getLineageOutputLabel(), "lineageOutputLabel should not be null");
    }

    // ==================== Set/Get Tests ====================

    @Test
    public void testSetLineageInputLabel() {
        // Arrange
        String expectedLabel = "__Process.inputs";

        // Act
        requestContext.setLineageInputLabel(expectedLabel);

        // Assert
        assertEquals(requestContext.getLineageInputLabel(), 
                     expectedLabel, 
                     "Should set and retrieve lineageInputLabel correctly");
    }

    @Test
    public void testSetLineageOutputLabel() {
        // Arrange
        String expectedLabel = "__Process.outputs";

        // Act
        requestContext.setLineageOutputLabel(expectedLabel);

        // Assert
        assertEquals(requestContext.getLineageOutputLabel(), 
                     expectedLabel, 
                     "Should set and retrieve lineageOutputLabel correctly");
    }

    @Test
    public void testSetBothLineageLabels() {
        // Arrange
        String inputLabel = "__Process.inputs";
        String outputLabel = "__Process.outputs";

        // Act
        requestContext.setLineageInputLabel(inputLabel);
        requestContext.setLineageOutputLabel(outputLabel);

        // Assert
        assertEquals(requestContext.getLineageInputLabel(), 
                     inputLabel, 
                     "Should set and retrieve lineageInputLabel correctly");
        assertEquals(requestContext.getLineageOutputLabel(), 
                     outputLabel, 
                     "Should set and retrieve lineageOutputLabel correctly");
    }

    @Test
    public void testSetLineageLabels_ProductAssetLineage() {
        // Arrange - Product Asset Lineage labels
        String inputLabel = "__Asset.outputPortDataProducts";
        String outputLabel = "__Asset.inputPortDataProducts";

        // Act
        requestContext.setLineageInputLabel(inputLabel);
        requestContext.setLineageOutputLabel(outputLabel);

        // Assert
        assertEquals(requestContext.getLineageInputLabel(), 
                     inputLabel, 
                     "Should handle ProductAssetLineage input label");
        assertEquals(requestContext.getLineageOutputLabel(), 
                     outputLabel, 
                     "Should handle ProductAssetLineage output label");
    }

    @Test
    public void testSetLineageLabels_Overwrite() {
        // Arrange
        String firstInputLabel = "__Process.inputs";
        String secondInputLabel = "__Asset.outputPortDataProducts";

        // Act
        requestContext.setLineageInputLabel(firstInputLabel);
        assertEquals(requestContext.getLineageInputLabel(), firstInputLabel, "First value should be set");
        
        requestContext.setLineageInputLabel(secondInputLabel);

        // Assert
        assertEquals(requestContext.getLineageInputLabel(), 
                     secondInputLabel, 
                     "Should overwrite previous value");
    }

    @Test
    public void testSetLineageLabels_EmptyString() {
        // Arrange
        requestContext.setLineageInputLabel("__Process.inputs");
        requestContext.setLineageOutputLabel("__Process.outputs");

        // Act - set back to empty
        requestContext.setLineageInputLabel("");
        requestContext.setLineageOutputLabel("");

        // Assert
        assertEquals(requestContext.getLineageInputLabel(), "", "Should allow setting to empty string");
        assertEquals(requestContext.getLineageOutputLabel(), "", "Should allow setting to empty string");
    }

    // ==================== ClearCache Tests ====================

    @Test
    public void testClearCache_ResetsLineageLabels() {
        // Arrange - set some values
        requestContext.setLineageInputLabel("__Process.inputs");
        requestContext.setLineageOutputLabel("__Process.outputs");
        
        // Verify they are set
        assertEquals(requestContext.getLineageInputLabel(), "__Process.inputs");
        assertEquals(requestContext.getLineageOutputLabel(), "__Process.outputs");

        // Act
        requestContext.clearCache();

        // Assert - should be reset to empty strings
        assertEquals(requestContext.getLineageInputLabel(), 
                     "", 
                     "lineageInputLabel should be reset to empty string after clearCache");
        assertEquals(requestContext.getLineageOutputLabel(), 
                     "", 
                     "lineageOutputLabel should be reset to empty string after clearCache");
    }

    @Test
    public void testClearCache_ResetsLineageLabels_ProductAssetLineage() {
        // Arrange - set ProductAssetLineage values
        requestContext.setLineageInputLabel("__Asset.outputPortDataProducts");
        requestContext.setLineageOutputLabel("__Asset.inputPortDataProducts");
        
        // Verify they are set
        assertEquals(requestContext.getLineageInputLabel(), "__Asset.outputPortDataProducts");
        assertEquals(requestContext.getLineageOutputLabel(), "__Asset.inputPortDataProducts");

        // Act
        requestContext.clearCache();

        // Assert - should be reset to empty strings
        assertEquals(requestContext.getLineageInputLabel(), 
                     "", 
                     "lineageInputLabel should be reset after clearCache");
        assertEquals(requestContext.getLineageOutputLabel(), 
                     "", 
                     "lineageOutputLabel should be reset after clearCache");
    }

    @Test
    public void testClearCache_MultipleTimes() {
        // Arrange
        requestContext.setLineageInputLabel("__Process.inputs");
        requestContext.setLineageOutputLabel("__Process.outputs");

        // Act - clear multiple times
        requestContext.clearCache();
        requestContext.clearCache();
        requestContext.clearCache();

        // Assert - should still be empty strings
        assertEquals(requestContext.getLineageInputLabel(), 
                     "", 
                     "Multiple clearCache calls should be safe");
        assertEquals(requestContext.getLineageOutputLabel(), 
                     "", 
                     "Multiple clearCache calls should be safe");
    }

    @Test
    public void testClearCache_ThenSetAgain() {
        // Arrange
        requestContext.setLineageInputLabel("__Process.inputs");
        requestContext.clearCache();

        // Act - set again after clear
        String newLabel = "__Asset.outputPortDataProducts";
        requestContext.setLineageInputLabel(newLabel);

        // Assert
        assertEquals(requestContext.getLineageInputLabel(), 
                     newLabel, 
                     "Should be able to set new value after clearCache");
    }

    // ==================== State Isolation Tests ====================

    @Test
    public void testLineageLabels_IndependentOfEachOther() {
        // Arrange & Act
        requestContext.setLineageInputLabel("__Process.inputs");
        
        // Assert - setting input should not affect output
        assertEquals(requestContext.getLineageInputLabel(), "__Process.inputs");
        assertEquals(requestContext.getLineageOutputLabel(), "", "Output label should remain empty");

        // Act
        requestContext.setLineageOutputLabel("__Process.outputs");

        // Assert - both should be set independently
        assertEquals(requestContext.getLineageInputLabel(), "__Process.inputs");
        assertEquals(requestContext.getLineageOutputLabel(), "__Process.outputs");
    }

    @Test
    public void testRequestContextSingleton_StatePersistsAcrossGetCalls() {
        // Arrange - RequestContext is a ThreadLocal singleton
        RequestContext context1 = RequestContext.get();
        context1.setLineageInputLabel("__Process.inputs");

        // Act - get the context again (should be same instance)
        RequestContext context2 = RequestContext.get();

        // Assert - both references should point to the same instance with same state
        assertSame(context1,
                context2,
                "Multiple get() calls should return same instance in same thread");
        assertEquals(context2.getLineageInputLabel(),
                     "__Process.inputs", 
                     "State should persist across get() calls");
        
        // Verify it's the same as the test context
        assertSame(requestContext,
                context1,
                "Test context should be same as get() result");
    }

    // ==================== Edge Case Tests ====================

    @Test
    public void testSetLineageLabels_NullHandling() {
        // Note: The actual implementation may or may not allow null.
        // This test documents the expected behavior.
        
        // Arrange
        requestContext.setLineageInputLabel("__Process.inputs");

        // Act & Assert - document behavior when setting null
        // (Adjust based on actual implementation requirements)
        try {
            requestContext.setLineageInputLabel(null);
            // If no exception, verify the state
            String result = requestContext.getLineageInputLabel();
            // Document what happens - either null is stored or converted to empty string
            assertTrue(result == null || "".equals(result),
                    "Should handle null gracefully");
        } catch (NullPointerException e) {
            // If NPE is expected behavior, test passes
            fail("Should handle null input gracefully, not throw NPE");
        }
    }

    @Test
    public void testSetLineageLabels_VeryLongString() {
        // Arrange - test with a very long label
        StringBuilder longLabel = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longLabel.append("a");
        }
        String longLabelString = longLabel.toString();

        // Act
        requestContext.setLineageInputLabel(longLabelString);

        // Assert
        assertEquals(requestContext.getLineageInputLabel(),
                     longLabelString, 
                     "Should handle very long labels");
    }

    @Test
    public void testSetLineageLabels_SpecialCharacters() {
        // Arrange - test with special characters
        String specialLabel = "__Process.inputs!@#$%^&*()_+-={}[]|:;<>?,./~`";

        // Act
        requestContext.setLineageInputLabel(specialLabel);

        // Assert
        assertEquals(requestContext.getLineageInputLabel(),
                     specialLabel, 
                     "Should handle special characters in labels");
    }
}
