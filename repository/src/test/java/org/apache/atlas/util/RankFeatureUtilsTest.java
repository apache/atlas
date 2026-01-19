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
package org.apache.atlas.util;

import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

/**
 * Unit tests for RankFeatureUtils utility class.
 */
public class RankFeatureUtilsTest {

    private static final float MIN_POSITIVE_NORMAL = Float.MIN_NORMAL; // 1.17549435E-38
    private static final String CUSTOM_DEFAULT_VALUE = "1.17549435e-38";

    private AtlasAttributeDef rankFeatureAttrDef;
    private AtlasAttributeDef nonRankFeatureAttrDef;
    private AtlasAttributeDef rankFeatureWithCustomDefault;

    @BeforeMethod
    public void setup() {
        // Create a rank_feature attribute definition
        rankFeatureAttrDef = createRankFeatureAttributeDef("assetInternalPopularityScore", null);

        // Create a non-rank_feature attribute definition
        nonRankFeatureAttrDef = createNonRankFeatureAttributeDef("regularFloat");

        // Create a rank_feature attribute with custom default value
        rankFeatureWithCustomDefault = createRankFeatureAttributeDef("customRankFeature", CUSTOM_DEFAULT_VALUE);
    }

    // ==================== isRankFeatureField Tests ====================

    @Test
    public void testIsRankFeatureField_withRankFeatureType_returnsTrue() {
        assertTrue(RankFeatureUtils.isRankFeatureField(rankFeatureAttrDef));
    }

    @Test
    public void testIsRankFeatureField_withNonRankFeatureType_returnsFalse() {
        assertFalse(RankFeatureUtils.isRankFeatureField(nonRankFeatureAttrDef));
    }

    @Test
    public void testIsRankFeatureField_withNullAttrDef_returnsFalse() {
        assertFalse(RankFeatureUtils.isRankFeatureField(null));
    }

    @Test
    public void testIsRankFeatureField_withEmptyIndexTypeESFields_returnsFalse() {
        AtlasAttributeDef attrDef = new AtlasAttributeDef("testAttr", "float");
        attrDef.setIndexTypeESFields(new HashMap<>());
        assertFalse(RankFeatureUtils.isRankFeatureField(attrDef));
    }

    @Test
    public void testIsRankFeatureField_withNullIndexTypeESFields_returnsFalse() {
        AtlasAttributeDef attrDef = new AtlasAttributeDef("testAttr", "float");
        attrDef.setIndexTypeESFields(null);
        assertFalse(RankFeatureUtils.isRankFeatureField(attrDef));
    }

    @Test
    public void testIsRankFeatureField_withTextFieldType_returnsFalse() {
        AtlasAttributeDef attrDef = new AtlasAttributeDef("textAttr", "string");
        HashMap<String, HashMap<String, Object>> indexTypeESFields = new HashMap<>();
        HashMap<String, Object> textConfig = new HashMap<>();
        textConfig.put("type", "text");
        textConfig.put("analyzer", "standard");
        indexTypeESFields.put("text", textConfig);
        attrDef.setIndexTypeESFields(indexTypeESFields);

        assertFalse(RankFeatureUtils.isRankFeatureField(attrDef));
    }

    @Test
    public void testIsRankFeatureField_withMultipleFieldTypes_includesRankFeature_returnsTrue() {
        AtlasAttributeDef attrDef = new AtlasAttributeDef("multiFieldAttr", "float");
        HashMap<String, HashMap<String, Object>> indexTypeESFields = new HashMap<>();

        // Add keyword field
        HashMap<String, Object> keywordConfig = new HashMap<>();
        keywordConfig.put("type", "keyword");
        indexTypeESFields.put("keyword", keywordConfig);

        // Add rank_feature field
        HashMap<String, Object> rankFeatureConfig = new HashMap<>();
        rankFeatureConfig.put("type", "rank_feature");
        indexTypeESFields.put("rank_feature", rankFeatureConfig);

        attrDef.setIndexTypeESFields(indexTypeESFields);

        assertTrue(RankFeatureUtils.isRankFeatureField(attrDef));
    }

    // ==================== getMinimumValue Tests ====================

    @Test
    public void testGetMinimumValue_withNoDefaultValue_returnsMinNormal() {
        assertEquals(RankFeatureUtils.getMinimumValue(rankFeatureAttrDef), MIN_POSITIVE_NORMAL);
    }

    @Test
    public void testGetMinimumValue_withCustomDefaultValue_returnsCustomValue() {
        float minValue = RankFeatureUtils.getMinimumValue(rankFeatureWithCustomDefault);
        assertEquals(minValue, Float.parseFloat(CUSTOM_DEFAULT_VALUE), 1e-45f);
    }

    @Test
    public void testGetMinimumValue_withNullAttrDef_returnsMinNormal() {
        assertEquals(RankFeatureUtils.getMinimumValue(null), MIN_POSITIVE_NORMAL);
    }

    @Test
    public void testGetMinimumValue_withInvalidDefaultValue_returnsMinNormal() {
        AtlasAttributeDef attrDef = createRankFeatureAttributeDef("invalidDefault", "not-a-number");
        assertEquals(RankFeatureUtils.getMinimumValue(attrDef), MIN_POSITIVE_NORMAL);
    }

    @Test
    public void testGetMinimumValue_withEmptyDefaultValue_returnsMinNormal() {
        AtlasAttributeDef attrDef = createRankFeatureAttributeDef("emptyDefault", "");
        assertEquals(RankFeatureUtils.getMinimumValue(attrDef), MIN_POSITIVE_NORMAL);
    }

    @Test
    public void testGetMinimumValue_withHigherDefaultValue_returnsHigherValue() {
        AtlasAttributeDef attrDef = createRankFeatureAttributeDef("higherDefault", "0.001");
        assertEquals(RankFeatureUtils.getMinimumValue(attrDef), 0.001f);
    }

    // ==================== normalizeValue Tests ====================

    @Test
    public void testNormalizeValue_withZeroValue_returnsMinimum() {
        Object result = RankFeatureUtils.normalizeValue(0.0f, rankFeatureWithCustomDefault);
        assertTrue(result instanceof Float);
        assertEquals((Float) result, Float.parseFloat(CUSTOM_DEFAULT_VALUE), 1e-45f);
    }

    @Test
    public void testNormalizeValue_withNegativeValue_returnsMinimum() {
        Object result = RankFeatureUtils.normalizeValue(-1.0f, rankFeatureWithCustomDefault);
        assertTrue(result instanceof Float);
        assertEquals((Float) result, Float.parseFloat(CUSTOM_DEFAULT_VALUE), 1e-45f);
    }

    @Test
    public void testNormalizeValue_withValueAboveMinimum_returnsOriginalValue() {
        float originalValue = 0.5f;
        Object result = RankFeatureUtils.normalizeValue(originalValue, rankFeatureWithCustomDefault);
        assertEquals(result, originalValue);
    }

    @Test
    public void testNormalizeValue_withValueExactlyAtMinimum_returnsOriginalValue() {
        float minValue = Float.parseFloat(CUSTOM_DEFAULT_VALUE);
        Object result = RankFeatureUtils.normalizeValue(minValue, rankFeatureWithCustomDefault);
        assertEquals((Float) result, minValue, 1e-45f);
    }

    @Test
    public void testNormalizeValue_withNonRankFeatureAttr_returnsOriginalValue() {
        float originalValue = 0.0f;
        Object result = RankFeatureUtils.normalizeValue(originalValue, nonRankFeatureAttrDef);
        assertEquals(result, originalValue);
    }

    @Test
    public void testNormalizeValue_withNullValue_returnsNull() {
        assertNull(RankFeatureUtils.normalizeValue(null, rankFeatureWithCustomDefault));
    }

    @Test
    public void testNormalizeValue_withNullAttrDef_returnsOriginalValue() {
        float originalValue = 0.0f;
        Object result = RankFeatureUtils.normalizeValue(originalValue, (AtlasAttributeDef) null);
        assertEquals(result, originalValue);
    }

    @Test
    public void testNormalizeValue_withDoubleValue_normalizesCorrectly() {
        Object result = RankFeatureUtils.normalizeValue(0.0d, rankFeatureWithCustomDefault);
        assertTrue(result instanceof Float);
        assertEquals((Float) result, Float.parseFloat(CUSTOM_DEFAULT_VALUE), 1e-45f);
    }

    @Test
    public void testNormalizeValue_withIntegerValue_normalizesCorrectly() {
        Object result = RankFeatureUtils.normalizeValue(0, rankFeatureWithCustomDefault);
        assertTrue(result instanceof Float);
        assertEquals((Float) result, Float.parseFloat(CUSTOM_DEFAULT_VALUE), 1e-45f);
    }

    @Test
    public void testNormalizeValue_withLongValue_normalizesCorrectly() {
        Object result = RankFeatureUtils.normalizeValue(0L, rankFeatureWithCustomDefault);
        assertTrue(result instanceof Float);
        assertEquals((Float) result, Float.parseFloat(CUSTOM_DEFAULT_VALUE), 1e-45f);
    }

    @Test
    public void testNormalizeValue_withNonNumericValue_returnsOriginalValue() {
        String originalValue = "not-a-number";
        Object result = RankFeatureUtils.normalizeValue(originalValue, rankFeatureWithCustomDefault);
        assertEquals(result, originalValue);
    }

    @Test
    public void testNormalizeValue_withPositiveValueJustBelowMinimum_returnsMinimum() {
        // Value that is positive but below MIN_NORMAL
        float verySmallValue = MIN_POSITIVE_NORMAL / 2;
        Object result = RankFeatureUtils.normalizeValue(verySmallValue, rankFeatureAttrDef);
        assertTrue(result instanceof Float);
        assertEquals((Float) result, MIN_POSITIVE_NORMAL, 1e-45f);
    }

    // ==================== isValidRankFeatureValue Tests ====================

    @Test
    public void testIsValidRankFeatureValue_withValidValue_returnsTrue() {
        assertTrue(RankFeatureUtils.isValidRankFeatureValue(1.0f, rankFeatureWithCustomDefault));
    }

    @Test
    public void testIsValidRankFeatureValue_withZeroValue_returnsFalse() {
        assertFalse(RankFeatureUtils.isValidRankFeatureValue(0.0f, rankFeatureWithCustomDefault));
    }

    @Test
    public void testIsValidRankFeatureValue_withNegativeValue_returnsFalse() {
        assertFalse(RankFeatureUtils.isValidRankFeatureValue(-1.0f, rankFeatureWithCustomDefault));
    }

    @Test
    public void testIsValidRankFeatureValue_withValueAtMinimum_returnsTrue() {
        float minValue = Float.parseFloat(CUSTOM_DEFAULT_VALUE);
        assertTrue(RankFeatureUtils.isValidRankFeatureValue(minValue, rankFeatureWithCustomDefault));
    }

    @Test
    public void testIsValidRankFeatureValue_withValueJustBelowMinimum_returnsFalse() {
        float minValue = Float.parseFloat(CUSTOM_DEFAULT_VALUE);
        float justBelow = minValue / 2;
        assertFalse(RankFeatureUtils.isValidRankFeatureValue(justBelow, rankFeatureWithCustomDefault));
    }

    @Test
    public void testIsValidRankFeatureValue_withNullValue_returnsTrue() {
        // Null values are valid (they will be handled by null-check logic elsewhere)
        assertTrue(RankFeatureUtils.isValidRankFeatureValue(null, rankFeatureWithCustomDefault));
    }

    @Test
    public void testIsValidRankFeatureValue_withNullAttrDef_returnsTrue() {
        // If no attribute definition, assume valid
        assertTrue(RankFeatureUtils.isValidRankFeatureValue(0.0f, null));
    }

    @Test
    public void testIsValidRankFeatureValue_withNonRankFeatureAttr_returnsTrue() {
        // Non-rank_feature attributes don't have this constraint
        assertTrue(RankFeatureUtils.isValidRankFeatureValue(0.0f, nonRankFeatureAttrDef));
    }

    @Test
    public void testIsValidRankFeatureValue_withDoubleValue_validatesCorrectly() {
        assertFalse(RankFeatureUtils.isValidRankFeatureValue(0.0d, rankFeatureWithCustomDefault));
        assertTrue(RankFeatureUtils.isValidRankFeatureValue(1.0d, rankFeatureWithCustomDefault));
    }

    @Test
    public void testIsValidRankFeatureValue_withIntegerValue_validatesCorrectly() {
        assertFalse(RankFeatureUtils.isValidRankFeatureValue(0, rankFeatureWithCustomDefault));
        assertTrue(RankFeatureUtils.isValidRankFeatureValue(1, rankFeatureWithCustomDefault));
    }

    // ==================== Data Provider Tests ====================

    @DataProvider(name = "normalizeValueTestCases")
    public Object[][] normalizeValueTestCases() {
        return new Object[][]{
                // {inputValue, isRankFeature, expectedNormalized}
                {0.0f, true, true},           // Zero should be normalized
                {-1.0f, true, true},          // Negative should be normalized
                {0.5f, true, false},          // Valid value should not be normalized
                {1.0f, true, false},          // Valid value should not be normalized
                {0.0f, false, false},         // Non-rank_feature should not be normalized
                {Float.MAX_VALUE, true, false}, // Large value should not be normalized
        };
    }

    @Test(dataProvider = "normalizeValueTestCases")
    public void testNormalizeValue_withDataProvider(float inputValue, boolean isRankFeature, boolean shouldNormalize) {
        AtlasAttributeDef attrDef = isRankFeature ? rankFeatureWithCustomDefault : nonRankFeatureAttrDef;
        Object result = RankFeatureUtils.normalizeValue(inputValue, attrDef);

        if (shouldNormalize) {
            float minValue = RankFeatureUtils.getMinimumValue(attrDef);
            assertEquals((Float) result, minValue, 1e-45f);
        } else {
            assertEquals(result, inputValue);
        }
    }

    // ==================== Edge Case Tests ====================

    @Test
    public void testConstants_esMinPositiveNormal_equalsFloatMinNormal() {
        assertEquals(RankFeatureUtils.ES_MIN_POSITIVE_NORMAL, Float.MIN_NORMAL);
    }

    @Test
    public void testConstants_rankFeatureType_hasCorrectValue() {
        assertEquals(RankFeatureUtils.RANK_FEATURE_TYPE, "rank_feature");
    }

    @Test
    public void testConstants_esFieldTypeKey_hasCorrectValue() {
        assertEquals(RankFeatureUtils.ES_FIELD_TYPE_KEY, "type");
    }

    // ==================== AtlasAttribute-based normalizeValue Tests (O(1) cached) ====================

    @Test
    public void testNormalizeValueWithAtlasAttribute_withRankFeatureField_normalizesZeroValue() {
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);
        when(mockAttribute.isRankFeatureField()).thenReturn(true);
        when(mockAttribute.getRankFeatureMinValue()).thenReturn(Float.MIN_NORMAL);
        when(mockAttribute.getName()).thenReturn("testAttr");

        Object result = RankFeatureUtils.normalizeValue(0.0f, mockAttribute);

        assertTrue(result instanceof Float);
        assertEquals((Float) result, Float.MIN_NORMAL, 1e-45f);
    }

    @Test
    public void testNormalizeValueWithAtlasAttribute_withRankFeatureField_normalizesNegativeValue() {
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);
        when(mockAttribute.isRankFeatureField()).thenReturn(true);
        when(mockAttribute.getRankFeatureMinValue()).thenReturn(Float.MIN_NORMAL);
        when(mockAttribute.getName()).thenReturn("testAttr");

        Object result = RankFeatureUtils.normalizeValue(-1.0f, mockAttribute);

        assertTrue(result instanceof Float);
        assertEquals((Float) result, Float.MIN_NORMAL, 1e-45f);
    }

    @Test
    public void testNormalizeValueWithAtlasAttribute_withRankFeatureField_returnsValidValueUnchanged() {
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);
        when(mockAttribute.isRankFeatureField()).thenReturn(true);
        when(mockAttribute.getRankFeatureMinValue()).thenReturn(Float.MIN_NORMAL);

        float validValue = 0.5f;
        Object result = RankFeatureUtils.normalizeValue(validValue, mockAttribute);

        assertEquals(result, validValue);
    }

    @Test
    public void testNormalizeValueWithAtlasAttribute_withNonRankFeatureField_returnsOriginalValue() {
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);
        when(mockAttribute.isRankFeatureField()).thenReturn(false);

        float originalValue = 0.0f;
        Object result = RankFeatureUtils.normalizeValue(originalValue, mockAttribute);

        assertEquals(result, originalValue);
    }

    @Test
    public void testNormalizeValueWithAtlasAttribute_withNullAttribute_returnsOriginalValue() {
        float originalValue = 0.0f;
        Object result = RankFeatureUtils.normalizeValue(originalValue, (AtlasAttribute) null);

        assertEquals(result, originalValue);
    }

    @Test
    public void testNormalizeValueWithAtlasAttribute_withNullValue_returnsNull() {
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);

        Object result = RankFeatureUtils.normalizeValue(null, mockAttribute);

        assertNull(result);
    }

    @Test
    public void testNormalizeValueWithAtlasAttribute_withCustomMinValue_usesCustomMin() {
        float customMinValue = 0.001f;
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);
        when(mockAttribute.isRankFeatureField()).thenReturn(true);
        when(mockAttribute.getRankFeatureMinValue()).thenReturn(customMinValue);
        when(mockAttribute.getName()).thenReturn("customAttr");

        // Value below custom min should be normalized
        Object result = RankFeatureUtils.normalizeValue(0.0001f, mockAttribute);
        assertTrue(result instanceof Float);
        assertEquals((Float) result, customMinValue, 1e-10f);

        // Value above custom min should be unchanged
        Object result2 = RankFeatureUtils.normalizeValue(0.01f, mockAttribute);
        assertEquals(result2, 0.01f);
    }

    @Test
    public void testNormalizeValueWithAtlasAttribute_withDoubleValue_normalizesCorrectly() {
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);
        when(mockAttribute.isRankFeatureField()).thenReturn(true);
        when(mockAttribute.getRankFeatureMinValue()).thenReturn(Float.MIN_NORMAL);
        when(mockAttribute.getName()).thenReturn("testAttr");

        Object result = RankFeatureUtils.normalizeValue(0.0d, mockAttribute);

        assertTrue(result instanceof Float);
        assertEquals((Float) result, Float.MIN_NORMAL, 1e-45f);
    }

    @Test
    public void testNormalizeValueWithAtlasAttribute_withIntegerValue_normalizesCorrectly() {
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);
        when(mockAttribute.isRankFeatureField()).thenReturn(true);
        when(mockAttribute.getRankFeatureMinValue()).thenReturn(Float.MIN_NORMAL);
        when(mockAttribute.getName()).thenReturn("testAttr");

        Object result = RankFeatureUtils.normalizeValue(0, mockAttribute);

        assertTrue(result instanceof Float);
        assertEquals((Float) result, Float.MIN_NORMAL, 1e-45f);
    }

    @Test
    public void testNormalizeValueWithAtlasAttribute_withNonNumericValue_returnsOriginalValue() {
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);
        when(mockAttribute.isRankFeatureField()).thenReturn(true);

        String originalValue = "not-a-number";
        Object result = RankFeatureUtils.normalizeValue(originalValue, mockAttribute);

        assertEquals(result, originalValue);
    }

    @DataProvider(name = "atlasAttributeNormalizeValueTestCases")
    public Object[][] atlasAttributeNormalizeValueTestCases() {
        return new Object[][]{
                // {inputValue, isRankFeature, minValue, expectedToBeNormalized}
                {0.0f, true, Float.MIN_NORMAL, true},
                {-1.0f, true, Float.MIN_NORMAL, true},
                {0.5f, true, Float.MIN_NORMAL, false},
                {1.0f, true, Float.MIN_NORMAL, false},
                {0.0f, false, Float.MIN_NORMAL, false},
                {0.0001f, true, 0.001f, true},  // Custom min value
                {0.01f, true, 0.001f, false},   // Above custom min
        };
    }

    @Test(dataProvider = "atlasAttributeNormalizeValueTestCases")
    public void testNormalizeValueWithAtlasAttribute_withDataProvider(
            float inputValue, boolean isRankFeature, float minValue, boolean shouldNormalize) {
        AtlasAttribute mockAttribute = Mockito.mock(AtlasAttribute.class);
        when(mockAttribute.isRankFeatureField()).thenReturn(isRankFeature);
        when(mockAttribute.getRankFeatureMinValue()).thenReturn(minValue);
        when(mockAttribute.getName()).thenReturn("testAttr");

        Object result = RankFeatureUtils.normalizeValue(inputValue, mockAttribute);

        if (shouldNormalize) {
            assertEquals((Float) result, minValue, 1e-10f);
        } else {
            assertEquals(result, inputValue);
        }
    }

    // ==================== Helper Methods ====================

    private AtlasAttributeDef createRankFeatureAttributeDef(String name, String defaultValue) {
        AtlasAttributeDef attrDef = new AtlasAttributeDef(name, "float");

        HashMap<String, HashMap<String, Object>> indexTypeESFields = new HashMap<>();
        HashMap<String, Object> rankFeatureConfig = new HashMap<>();
        rankFeatureConfig.put("type", "rank_feature");
        indexTypeESFields.put("rank_feature", rankFeatureConfig);

        attrDef.setIndexTypeESFields(indexTypeESFields);

        if (defaultValue != null) {
            attrDef.setDefaultValue(defaultValue);
        }

        return attrDef;
    }

    private AtlasAttributeDef createNonRankFeatureAttributeDef(String name) {
        AtlasAttributeDef attrDef = new AtlasAttributeDef(name, "float");
        // No indexTypeESFields set, simulating a regular float attribute
        return attrDef;
    }
}
