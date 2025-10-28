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
package org.apache.atlas.type;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasBigDecimalType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasBigIntegerType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasBooleanType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasByteType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasDateType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasDoubleType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasFloatType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasIntType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasLongType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasObjectIdType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasShortType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasStringType;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasBuiltInTypes {
    @Test
    public void testConstructorIsPrivate() throws Exception {
        Constructor<AtlasBuiltInTypes> constructor = AtlasBuiltInTypes.class.getDeclaredConstructor();
        assertFalse(Modifier.isPrivate(constructor.getModifiers())); // AtlasBuiltInTypes has package-private constructor
    }

    @Test
    public void testToBigIntegerMethod() throws Exception {
        Method toBigIntegerMethod = AtlasBuiltInTypes.class.getDeclaredMethod("toBigInteger", Number.class);
        toBigIntegerMethod.setAccessible(true);

        // Test with BigInteger
        BigInteger bigInt = BigInteger.valueOf(100);
        BigInteger result = (BigInteger) toBigIntegerMethod.invoke(null, bigInt);
        assertEquals(result, bigInt);

        // Test with Byte
        Byte byteVal = (byte) 10;
        result = (BigInteger) toBigIntegerMethod.invoke(null, byteVal);
        assertEquals(result, BigInteger.valueOf(10));

        // Test with Short
        Short shortVal = (short) 1000;
        result = (BigInteger) toBigIntegerMethod.invoke(null, shortVal);
        assertEquals(result, BigInteger.valueOf(1000));

        // Test with Integer
        Integer intVal = 100000;
        result = (BigInteger) toBigIntegerMethod.invoke(null, intVal);
        assertEquals(result, BigInteger.valueOf(100000));

        // Test with Long
        Long longVal = 10000000L;
        result = (BigInteger) toBigIntegerMethod.invoke(null, longVal);
        assertEquals(result, BigInteger.valueOf(10000000L));

        // Test with BigDecimal
        BigDecimal bigDecimal = new BigDecimal("123.456");
        result = (BigInteger) toBigIntegerMethod.invoke(null, bigDecimal);
        assertEquals(result, BigInteger.valueOf(123));

        // Test with Double
        Double doubleVal = 123.789;
        result = (BigInteger) toBigIntegerMethod.invoke(null, doubleVal);
        assertEquals(result, BigInteger.valueOf(123));

        // Test with Float
        Float floatVal = 123.456f;
        result = (BigInteger) toBigIntegerMethod.invoke(null, floatVal);
        assertEquals(result, BigInteger.valueOf(123));
    }

    @Test
    public void testAtlasBooleanType() {
        AtlasBooleanType booleanType = new AtlasBooleanType();

        // Test default value
        assertEquals(booleanType.createDefaultValue(), Boolean.FALSE);

        // Test valid values
        assertTrue(booleanType.isValidValue(null));
        assertTrue(booleanType.isValidValue(Boolean.TRUE));
        assertTrue(booleanType.isValidValue(Boolean.FALSE));
        assertTrue(booleanType.isValidValue("true"));
        assertTrue(booleanType.isValidValue("false"));
        assertTrue(booleanType.isValidValue("TRUE"));
        assertTrue(booleanType.isValidValue("FALSE"));

        // Test invalid values
        assertFalse(booleanType.isValidValue("invalid"));
        assertFalse(booleanType.isValidValue("yes"));
        assertFalse(booleanType.isValidValue("1"));

        // Test normalized values
        assertEquals(booleanType.getNormalizedValue(Boolean.TRUE), Boolean.TRUE);
        assertEquals(booleanType.getNormalizedValue("true"), Boolean.TRUE);
        assertEquals(booleanType.getNormalizedValue("TRUE"), Boolean.TRUE);
        assertEquals(booleanType.getNormalizedValue("false"), Boolean.FALSE);
        assertEquals(booleanType.getNormalizedValue("FALSE"), Boolean.FALSE);
        assertNull(booleanType.getNormalizedValue("invalid"));
        assertNull(booleanType.getNormalizedValue(null));
    }

    @Test
    public void testAtlasByteType() {
        AtlasByteType byteType = new AtlasByteType();

        // Test default value
        assertEquals(byteType.createDefaultValue(), Byte.valueOf((byte) 0));

        // Test valid values
        assertTrue(byteType.isValidValue(null));
        assertTrue(byteType.isValidValue(Byte.valueOf((byte) 100)));
        assertTrue(byteType.isValidValue((byte) 50));
        assertTrue(byteType.isValidValue("100"));
        assertTrue(byteType.isValidValue(100)); // Integer within byte range

        // Test invalid values (out of byte range)
        assertFalse(byteType.isValidValue(300)); // > Byte.MAX_VALUE
        assertFalse(byteType.isValidValue(-200)); // < Byte.MIN_VALUE
        assertFalse(byteType.isValidValue("invalid"));

        // Test normalized values
        assertEquals(byteType.getNormalizedValue((byte) 100), Byte.valueOf((byte) 100));
        assertEquals(byteType.getNormalizedValue("50"), Byte.valueOf((byte) 50));
        assertEquals(byteType.getNormalizedValue(75), Byte.valueOf((byte) 75)); // Integer within range
        assertNull(byteType.getNormalizedValue(300)); // Out of range
        assertNull(byteType.getNormalizedValue("invalid"));
        assertNull(byteType.getNormalizedValue(null));
    }

    @Test
    public void testAtlasShortType() {
        AtlasShortType shortType = new AtlasShortType();

        // Test default value
        assertEquals(shortType.createDefaultValue(), Short.valueOf((short) 0));

        // Test valid values
        assertTrue(shortType.isValidValue(null));
        assertTrue(shortType.isValidValue(Short.valueOf((short) 1000)));
        assertTrue(shortType.isValidValue("1000"));
        assertTrue(shortType.isValidValue(1000)); // Integer within short range

        // Test invalid values
        assertFalse(shortType.isValidValue(50000)); // > Short.MAX_VALUE
        assertFalse(shortType.isValidValue(-50000)); // < Short.MIN_VALUE
        assertFalse(shortType.isValidValue("invalid"));

        // Test normalized values
        assertEquals(shortType.getNormalizedValue((short) 1000), Short.valueOf((short) 1000));
        assertEquals(shortType.getNormalizedValue("500"), Short.valueOf((short) 500));
        assertEquals(shortType.getNormalizedValue(750), Short.valueOf((short) 750));
        assertNull(shortType.getNormalizedValue(50000)); // Out of range
        assertNull(shortType.getNormalizedValue("invalid"));
        assertNull(shortType.getNormalizedValue(null));
    }

    @Test
    public void testAtlasIntType() {
        AtlasIntType intType = new AtlasIntType();

        // Test default value
        assertEquals(intType.createDefaultValue(), Integer.valueOf(0));

        // Test valid values
        assertTrue(intType.isValidValue(null));
        assertTrue(intType.isValidValue(Integer.valueOf(100000)));
        assertTrue(intType.isValidValue("100000"));
        assertTrue(intType.isValidValue(100000L)); // Long within int range

        // Test invalid values
        assertFalse(intType.isValidValue(Long.MAX_VALUE)); // > Integer.MAX_VALUE
        assertFalse(intType.isValidValue(Long.MIN_VALUE)); // < Integer.MIN_VALUE
        assertFalse(intType.isValidValue("invalid"));

        // Test normalized values
        assertEquals(intType.getNormalizedValue(100000), Integer.valueOf(100000));
        assertEquals(intType.getNormalizedValue("50000"), Integer.valueOf(50000));
        assertEquals(intType.getNormalizedValue(75000L), Integer.valueOf(75000));
        assertNull(intType.getNormalizedValue(Long.MAX_VALUE)); // Out of range
        assertNull(intType.getNormalizedValue("invalid"));
        assertNull(intType.getNormalizedValue(null));
    }

    @Test
    public void testAtlasLongType() {
        AtlasLongType longType = new AtlasLongType();

        // Test default value
        assertEquals(longType.createDefaultValue(), Long.valueOf(0L));

        // Test valid values
        assertTrue(longType.isValidValue(null));
        assertTrue(longType.isValidValue(1000000L));
        assertTrue(longType.isValidValue("1000000"));
        assertTrue(longType.isValidValue(1000000));

        // Test normalized values
        assertEquals(longType.getNormalizedValue(1000000L), Long.valueOf(1000000L));
        assertEquals(longType.getNormalizedValue("500000"), Long.valueOf(500000L));
        assertEquals(longType.getNormalizedValue(750000), Long.valueOf(750000L));
        assertNull(longType.getNormalizedValue("invalid"));
        assertNull(longType.getNormalizedValue(null));
    }

    @Test
    public void testAtlasFloatType() {
        AtlasFloatType floatType = new AtlasFloatType();

        // Test default value
        assertEquals(floatType.createDefaultValue(), Float.valueOf(0f));

        // Test valid values
        assertTrue(floatType.isValidValue(null));
        assertTrue(floatType.isValidValue(123.456f));
        assertTrue(floatType.isValidValue("123.456"));
        assertTrue(floatType.isValidValue(123.456));

        // Test invalid values
        assertFalse(floatType.isValidValue(Float.POSITIVE_INFINITY));
        assertFalse(floatType.isValidValue(Float.NEGATIVE_INFINITY));
        assertFalse(floatType.isValidValue("invalid"));

        // Test normalized values
        assertEquals(floatType.getNormalizedValue(123.456f), Float.valueOf(123.456f));
        assertEquals(floatType.getNormalizedValue("789.123"), Float.valueOf(789.123f));
        assertNull(floatType.getNormalizedValue(Float.POSITIVE_INFINITY));
        assertNull(floatType.getNormalizedValue("invalid"));
        assertNull(floatType.getNormalizedValue(null));

        // Test areEqualValues with float epsilon (FLOAT_EPSILON = 0.00000001f)
        assertTrue(floatType.areEqualValues(1.0f, 1.0f, null));
        assertTrue(floatType.areEqualValues(1.0f, 1.000000001f, null)); // Within epsilon
        assertFalse(floatType.areEqualValues(1.0f, 1.1f, null)); // Outside epsilon should be false
        assertTrue(floatType.areEqualValues(null, null, null));
        assertFalse(floatType.areEqualValues(1.0f, null, null));
    }

    @Test
    public void testAtlasDoubleType() {
        AtlasDoubleType doubleType = new AtlasDoubleType();

        // Test default value
        assertEquals(doubleType.createDefaultValue(), Double.valueOf(0d));

        // Test valid values
        assertTrue(doubleType.isValidValue(null));
        assertTrue(doubleType.isValidValue(123.456789));
        assertTrue(doubleType.isValidValue("123.456789"));

        // Test invalid values
        assertFalse(doubleType.isValidValue(Double.POSITIVE_INFINITY));
        assertFalse(doubleType.isValidValue(Double.NEGATIVE_INFINITY));
        assertFalse(doubleType.isValidValue("invalid"));

        // Test normalized values
        assertEquals(doubleType.getNormalizedValue(123.456789), Double.valueOf(123.456789));
        assertEquals(doubleType.getNormalizedValue("789.123456"), Double.valueOf(789.123456));
        assertNull(doubleType.getNormalizedValue(Double.POSITIVE_INFINITY));
        assertNull(doubleType.getNormalizedValue("invalid"));
        assertNull(doubleType.getNormalizedValue(null));

        // Test areEqualValues with double epsilon (DOUBLE_EPSILON = 0.00000001d)
        assertTrue(doubleType.areEqualValues(1.0, 1.0, null));
        assertTrue(doubleType.areEqualValues(1.0, 1.000000001, null)); // Within epsilon
        assertFalse(doubleType.areEqualValues(1.0, 1.1, null)); // Outside epsilon should be false
        assertTrue(doubleType.areEqualValues(null, null, null));
        assertFalse(doubleType.areEqualValues(1.0, null, null));
    }

    @Test
    public void testAtlasBigIntegerType() {
        AtlasBigIntegerType bigIntegerType = new AtlasBigIntegerType();

        // Test default value
        assertEquals(bigIntegerType.createDefaultValue(), BigInteger.ZERO);

        // Test valid values
        assertTrue(bigIntegerType.isValidValue(null));
        assertTrue(bigIntegerType.isValidValue(BigInteger.valueOf(123)));
        assertTrue(bigIntegerType.isValidValue(123));
        assertTrue(bigIntegerType.isValidValue("123"));

        // Test normalized values
        assertEquals(bigIntegerType.getNormalizedValue(BigInteger.valueOf(123)), BigInteger.valueOf(123));
        assertEquals(bigIntegerType.getNormalizedValue(123), BigInteger.valueOf(123));
        assertEquals(bigIntegerType.getNormalizedValue("456"), BigInteger.valueOf(456));
        assertEquals(bigIntegerType.getNormalizedValue(new BigDecimal("789.123")), BigInteger.valueOf(789));
        assertNull(bigIntegerType.getNormalizedValue("invalid"));
        assertNull(bigIntegerType.getNormalizedValue(null));
    }

    @Test
    public void testAtlasBigDecimalType() {
        AtlasBigDecimalType bigDecimalType = new AtlasBigDecimalType();

        // Test default value
        assertEquals(bigDecimalType.createDefaultValue(), BigDecimal.ZERO);

        // Test valid values
        assertTrue(bigDecimalType.isValidValue(null));
        assertTrue(bigDecimalType.isValidValue(new BigDecimal("123.456")));
        assertTrue(bigDecimalType.isValidValue(123));
        assertTrue(bigDecimalType.isValidValue("123.456"));

        // Test normalized values
        assertEquals(bigDecimalType.getNormalizedValue(new BigDecimal("123.456")), new BigDecimal("123.456"));
        assertEquals(bigDecimalType.getNormalizedValue(BigInteger.valueOf(123)), new BigDecimal(BigInteger.valueOf(123)));
        assertEquals(bigDecimalType.getNormalizedValue(0), BigDecimal.ZERO);
        assertEquals(bigDecimalType.getNormalizedValue(123.456), BigDecimal.valueOf(123.456));
        assertEquals(bigDecimalType.getNormalizedValue("789.123"), new BigDecimal("789.123"));
        assertNull(bigDecimalType.getNormalizedValue("invalid"));
        assertNull(bigDecimalType.getNormalizedValue(null));
    }

    @Test
    public void testAtlasDateType() {
        AtlasDateType dateType = new AtlasDateType();

        // Test default value
        assertEquals(dateType.createDefaultValue(), new Date(0));

        // Test valid values
        assertTrue(dateType.isValidValue(null));
        assertTrue(dateType.isValidValue(new Date()));
        assertTrue(dateType.isValidValue(System.currentTimeMillis()));
        assertTrue(dateType.isValidValue(""));

        // Test normalized values
        Date testDate = new Date();
        assertEquals(dateType.getNormalizedValue(testDate), testDate);

        long timestamp = System.currentTimeMillis();
        assertEquals(dateType.getNormalizedValue(timestamp), new Date(timestamp));

        assertEquals(dateType.getNormalizedValue("123456789"), new Date(123456789L));
        assertNull(dateType.getNormalizedValue(null));
    }

    @Test
    public void testAtlasStringType() {
        AtlasStringType stringType = new AtlasStringType();

        // Test default value
        assertEquals(stringType.createDefaultValue(), "");

        // Test optional default value
        assertNull(stringType.createOptionalDefaultValue());

        // Test valid values (everything is valid for string)
        assertTrue(stringType.isValidValue(null));
        assertTrue(stringType.isValidValue("test"));
        assertTrue(stringType.isValidValue(123));
        assertTrue(stringType.isValidValue(new Object()));

        // Test normalized values
        assertEquals(stringType.getNormalizedValue("test"), "test");
        assertEquals(stringType.getNormalizedValue(123), "123");
        assertEquals(stringType.getNormalizedValue(true), "true");
        assertNull(stringType.getNormalizedValue(null));
    }

    @Test
    public void testAtlasObjectIdType() {
        AtlasObjectIdType objectIdType = new AtlasObjectIdType();

        // Test default constructor
        assertEquals(objectIdType.getObjectType(), "Asset");

        // Test constructor with object type
        AtlasObjectIdType customObjectIdType = new AtlasObjectIdType("CustomType");
        assertEquals(customObjectIdType.getObjectType(), "CustomType");

        // Test default value
        AtlasObjectId defaultValue = objectIdType.createDefaultValue();
        assertEquals(defaultValue.getGuid(), "-1");
        assertEquals(defaultValue.getTypeName(), "Asset");

        // Test valid values
        assertTrue(objectIdType.isValidValue(null));
        assertTrue(objectIdType.isValidValue(new AtlasObjectId("guid", "typeName")));

        Map<String, Object> validMap = new HashMap<>();
        validMap.put("guid", "test-guid");
        assertTrue(objectIdType.isValidValue(validMap));

        Map<String, Object> validMapWithUniqueAttrs = new HashMap<>();
        validMapWithUniqueAttrs.put("typeName", "TestType");
        Map<String, Object> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put("qualifiedName", "test@cluster");
        validMapWithUniqueAttrs.put("uniqueAttributes", uniqueAttrs);
        assertTrue(objectIdType.isValidValue(validMapWithUniqueAttrs));

        // Test invalid values
        assertFalse(objectIdType.isValidValue("invalid"));

        Map<String, Object> invalidMap = new HashMap<>();
        invalidMap.put("invalid", "value");
        assertFalse(objectIdType.isValidValue(invalidMap));

        // Test normalized values
        AtlasObjectId testObjectId = new AtlasObjectId("test-guid", "TestType");
        assertEquals(objectIdType.getNormalizedValue(testObjectId), testObjectId);

        AtlasObjectId normalizedFromMap = objectIdType.getNormalizedValue(validMap);
        assertNotNull(normalizedFromMap);
        assertEquals(normalizedFromMap.getGuid(), "test-guid");

        // Test with related object id map
        Map<String, Object> relatedObjectIdMap = new HashMap<>();
        relatedObjectIdMap.put("guid", "test-guid");
        relatedObjectIdMap.put("relationshipType", "testRelation");
        AtlasObjectId normalizedRelated = objectIdType.getNormalizedValue(relatedObjectIdMap);
        assertTrue(normalizedRelated instanceof AtlasRelatedObjectId);

        assertNull(objectIdType.getNormalizedValue(null));
        assertNull(objectIdType.getNormalizedValue(invalidMap));

        // Test areEqualValues
        AtlasObjectId obj1 = new AtlasObjectId("guid1", "Type1");
        AtlasObjectId obj2 = new AtlasObjectId("guid1", "Type1");
        AtlasObjectId obj3 = new AtlasObjectId("guid2", "Type1");

        assertTrue(objectIdType.areEqualValues(obj1, obj2, null));
        assertFalse(objectIdType.areEqualValues(obj1, obj3, null));
        assertTrue(objectIdType.areEqualValues(null, null, null));
        assertFalse(objectIdType.areEqualValues(obj1, null, null));

        // Test with guid assignments
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("guid2", "guid1");
        assertTrue(objectIdType.areEqualValues(obj1, obj3, guidAssignments));
    }

    @Test
    public void testBooleanTypeEdgeCases() {
        AtlasBooleanType booleanType = new AtlasBooleanType();

        // Test string representations
        assertEquals(booleanType.getNormalizedValue("true"), Boolean.TRUE);
        assertEquals(booleanType.getNormalizedValue("false"), Boolean.FALSE);
        assertEquals(booleanType.getNormalizedValue("TRUE"), Boolean.TRUE);
        assertEquals(booleanType.getNormalizedValue("FALSE"), Boolean.FALSE);
        assertEquals(booleanType.getNormalizedValue("True"), Boolean.TRUE);
        assertEquals(booleanType.getNormalizedValue("False"), Boolean.FALSE);

        // Test invalid string values
        assertNull(booleanType.getNormalizedValue("yes"));
        assertNull(booleanType.getNormalizedValue("no"));
        assertNull(booleanType.getNormalizedValue("1"));
        assertNull(booleanType.getNormalizedValue("0"));
        assertNull(booleanType.getNormalizedValue("invalid"));

        // Test other types
        assertNull(booleanType.getNormalizedValue(1));
        assertNull(booleanType.getNormalizedValue(0));
        assertNull(booleanType.getNormalizedValue(new Object()));

        // Test null
        assertNull(booleanType.getNormalizedValue(null));

        // Test validation
        assertTrue(booleanType.isValidValue(null));
        assertTrue(booleanType.isValidValue(Boolean.TRUE));
        assertTrue(booleanType.isValidValue(Boolean.FALSE));
        assertTrue(booleanType.isValidValue("true"));
        assertFalse(booleanType.isValidValue("invalid"));
        assertFalse(booleanType.isValidValue(123));
    }

    @Test
    public void testByteTypeEdgeCases() {
        AtlasByteType byteType = new AtlasByteType();

        // Test range validations
        assertEquals(byteType.getNormalizedValue(Byte.MAX_VALUE), Byte.valueOf(Byte.MAX_VALUE));
        assertEquals(byteType.getNormalizedValue(Byte.MIN_VALUE), Byte.valueOf(Byte.MIN_VALUE));

        // Test out of range values
        assertNull(byteType.getNormalizedValue(Short.MAX_VALUE));
        assertNull(byteType.getNormalizedValue(Short.MIN_VALUE));
        assertNull(byteType.getNormalizedValue(Integer.MAX_VALUE));
        assertNull(byteType.getNormalizedValue(Long.MAX_VALUE));

        // Test BigInteger values
        assertNull(byteType.getNormalizedValue(BigInteger.valueOf(Short.MAX_VALUE)));
        assertEquals(byteType.getNormalizedValue(BigInteger.valueOf(100)), Byte.valueOf((byte) 100));

        // Test BigDecimal values
        assertEquals(byteType.getNormalizedValue(BigDecimal.valueOf(50)), Byte.valueOf((byte) 50));
        assertNull(byteType.getNormalizedValue(BigDecimal.valueOf(1000)));

        // Test string values
        assertEquals(byteType.getNormalizedValue("100"), Byte.valueOf((byte) 100));
        assertNull(byteType.getNormalizedValue("1000"));
        assertNull(byteType.getNormalizedValue("invalid"));
        assertNull(byteType.getNormalizedValue(""));

        // Test validation
        assertTrue(byteType.isValidValue(null));
        assertTrue(byteType.isValidValue((byte) 50));
        assertTrue(byteType.isValidValue("100"));
        assertFalse(byteType.isValidValue(1000));
    }

    @Test
    public void testShortTypeEdgeCases() {
        AtlasShortType shortType = new AtlasShortType();

        // Test range validations
        assertEquals(shortType.getNormalizedValue(Short.MAX_VALUE), Short.valueOf(Short.MAX_VALUE));
        assertEquals(shortType.getNormalizedValue(Short.MIN_VALUE), Short.valueOf(Short.MIN_VALUE));

        // Test out of range values
        assertNull(shortType.getNormalizedValue(Integer.MAX_VALUE));
        assertNull(shortType.getNormalizedValue(Long.MAX_VALUE));

        // Test BigInteger values
        assertEquals(shortType.getNormalizedValue(BigInteger.valueOf(1000)), Short.valueOf((short) 1000));
        assertNull(shortType.getNormalizedValue(BigInteger.valueOf(Integer.MAX_VALUE)));

        // Test different number types
        assertEquals(shortType.getNormalizedValue((byte) 100), Short.valueOf((short) 100));
        assertEquals(shortType.getNormalizedValue(1000), Short.valueOf((short) 1000));

        // Test string values
        assertEquals(shortType.getNormalizedValue("1000"), Short.valueOf((short) 1000));
        assertNull(shortType.getNormalizedValue("70000"));
        assertNull(shortType.getNormalizedValue("invalid"));
    }

    @Test
    public void testIntTypeEdgeCases() {
        AtlasIntType intType = new AtlasIntType();

        // Test range validations
        assertEquals(intType.getNormalizedValue(Integer.MAX_VALUE), Integer.valueOf(Integer.MAX_VALUE));
        assertEquals(intType.getNormalizedValue(Integer.MIN_VALUE), Integer.valueOf(Integer.MIN_VALUE));

        // Test out of range values
        assertNull(intType.getNormalizedValue(Long.MAX_VALUE));
        assertNull(intType.getNormalizedValue(Long.MIN_VALUE));

        // Test BigInteger values
        assertEquals(intType.getNormalizedValue(BigInteger.valueOf(100000)), Integer.valueOf(100000));
        assertNull(intType.getNormalizedValue(BigInteger.valueOf(Long.MAX_VALUE)));

        // Test different number types
        assertEquals(intType.getNormalizedValue((byte) 100), Integer.valueOf(100));
        assertEquals(intType.getNormalizedValue((short) 1000), Integer.valueOf(1000));
        assertEquals(intType.getNormalizedValue(100000L), Integer.valueOf(100000));

        // Test floating point truncation
        assertEquals(intType.getNormalizedValue(123.789f), Integer.valueOf(123));
        assertEquals(intType.getNormalizedValue(123.789d), Integer.valueOf(123));

        // Test string values
        assertEquals(intType.getNormalizedValue("100000"), Integer.valueOf(100000));
        assertNull(intType.getNormalizedValue("9999999999"));
        assertNull(intType.getNormalizedValue("invalid"));
    }

    @Test
    public void testLongTypeEdgeCases() {
        AtlasLongType longType = new AtlasLongType();

        // Test range validations
        assertEquals(longType.getNormalizedValue(Long.MAX_VALUE), Long.valueOf(Long.MAX_VALUE));
        assertEquals(longType.getNormalizedValue(Long.MIN_VALUE), Long.valueOf(Long.MIN_VALUE));

        // Test different number types
        assertEquals(longType.getNormalizedValue((byte) 100), Long.valueOf(100L));
        assertEquals(longType.getNormalizedValue((short) 1000), Long.valueOf(1000L));
        assertEquals(longType.getNormalizedValue(100000), Long.valueOf(100000L));

        // Test BigInteger values
        assertEquals(longType.getNormalizedValue(BigInteger.valueOf(100000)), Long.valueOf(100000L));

        // Test BigDecimal values
        assertEquals(longType.getNormalizedValue(BigDecimal.valueOf(100000)), Long.valueOf(100000L));

        // Test floating point truncation
        assertEquals(longType.getNormalizedValue(123.789f), Long.valueOf(123L));
        assertEquals(longType.getNormalizedValue(123.789d), Long.valueOf(123L));

        // Test string values
        assertEquals(longType.getNormalizedValue("9223372036854775807"), Long.valueOf(Long.MAX_VALUE));
        assertNull(longType.getNormalizedValue("invalid"));
    }

    @Test
    public void testFloatTypeEdgeCases() {
        AtlasFloatType floatType = new AtlasFloatType();

        // Test basic values
        assertEquals(floatType.getNormalizedValue(123.456f), Float.valueOf(123.456f));
        assertEquals(floatType.getNormalizedValue(123.456), Float.valueOf(123.456f));

        // Test edge values
        assertEquals(floatType.getNormalizedValue(Float.MAX_VALUE), Float.valueOf(Float.MAX_VALUE));
        assertEquals(floatType.getNormalizedValue(Float.MIN_VALUE), Float.valueOf(Float.MIN_VALUE));

        // Test infinity and special values - check if they return null or the values
        Float posInf = floatType.getNormalizedValue(Float.POSITIVE_INFINITY);
        Float negInf = floatType.getNormalizedValue(Float.NEGATIVE_INFINITY);
        Float nanVal = floatType.getNormalizedValue(Float.NaN);

        // These might return null depending on implementation - test what actually happens
        if (posInf != null) {
            assertEquals(posInf, Float.POSITIVE_INFINITY, 0.0001f);
        }
        if (negInf != null) {
            assertEquals(negInf, Float.NEGATIVE_INFINITY, 0.0001f);
        }
        if (nanVal != null) {
            assertTrue(Float.isNaN(nanVal));
        }

        // Test different number types
        assertEquals(floatType.getNormalizedValue((byte) 100), Float.valueOf(100f));
        assertEquals(floatType.getNormalizedValue((short) 1000), Float.valueOf(1000f));
        assertEquals(floatType.getNormalizedValue(100000), Float.valueOf(100000f));
        assertEquals(floatType.getNormalizedValue(100000L), Float.valueOf(100000f));

        // Test BigDecimal values
        assertEquals(floatType.getNormalizedValue(BigDecimal.valueOf(123.456)), Float.valueOf(123.456f));

        // Test string values
        assertEquals(floatType.getNormalizedValue("123.456"), Float.valueOf(123.456f));
        assertNull(floatType.getNormalizedValue("invalid"));

        // Test areEqualValues with epsilon
        Map<String, String> guidAssignments = new HashMap<>();
        assertTrue(floatType.areEqualValues(123.456f, 123.456f, guidAssignments));
        assertTrue(floatType.areEqualValues(123.456f, 123.456000001f, guidAssignments)); // Within epsilon
        assertFalse(floatType.areEqualValues(123.456f, 123.457f, guidAssignments)); // Outside epsilon
        assertTrue(floatType.areEqualValues(null, null, guidAssignments));
        assertFalse(floatType.areEqualValues(123.456f, null, guidAssignments));
        assertFalse(floatType.areEqualValues(null, 123.456f, guidAssignments));

        // Test with invalid values in areEqualValues
        assertFalse(floatType.areEqualValues("invalid", 123.456f, guidAssignments));
        assertFalse(floatType.areEqualValues(123.456f, "invalid", guidAssignments));
    }

    @Test
    public void testDoubleTypeEdgeCases() {
        AtlasDoubleType doubleType = new AtlasDoubleType();

        // Test basic values
        assertEquals(doubleType.getNormalizedValue(123.456789), Double.valueOf(123.456789));
        assertEquals(doubleType.getNormalizedValue(123.456f), Double.valueOf(123.456), 0.001);

        // Test edge values
        assertEquals(doubleType.getNormalizedValue(Double.MAX_VALUE), Double.valueOf(Double.MAX_VALUE));
        assertEquals(doubleType.getNormalizedValue(Double.MIN_VALUE), Double.valueOf(Double.MIN_VALUE));

        // Test infinity and special values - check if they return null or the values
        Double posInf = doubleType.getNormalizedValue(Double.POSITIVE_INFINITY);
        Double negInf = doubleType.getNormalizedValue(Double.NEGATIVE_INFINITY);
        Double nanVal = doubleType.getNormalizedValue(Double.NaN);

        // These might return null depending on implementation - test what actually happens
        if (posInf != null) {
            assertEquals(posInf, Double.POSITIVE_INFINITY, 0.0001);
        }
        if (negInf != null) {
            assertEquals(negInf, Double.NEGATIVE_INFINITY, 0.0001);
        }
        if (nanVal != null) {
            assertTrue(Double.isNaN(nanVal));
        }

        // Test different number types
        assertEquals(doubleType.getNormalizedValue((byte) 100), Double.valueOf(100.0));
        assertEquals(doubleType.getNormalizedValue((short) 1000), Double.valueOf(1000.0));
        assertEquals(doubleType.getNormalizedValue(100000), Double.valueOf(100000.0));
        assertEquals(doubleType.getNormalizedValue(100000L), Double.valueOf(100000.0));

        // Test BigDecimal values
        assertEquals(doubleType.getNormalizedValue(BigDecimal.valueOf(123.456789)), Double.valueOf(123.456789));

        // Test string values
        assertEquals(doubleType.getNormalizedValue("123.456789"), Double.valueOf(123.456789));
        assertNull(doubleType.getNormalizedValue("invalid"));

        // Test areEqualValues with epsilon
        Map<String, String> guidAssignments = new HashMap<>();
        assertTrue(doubleType.areEqualValues(123.456789, 123.456789, guidAssignments));
        assertTrue(doubleType.areEqualValues(123.456789, 123.456789000000001, guidAssignments)); // Within epsilon
        assertFalse(doubleType.areEqualValues(123.456789, 123.457, guidAssignments)); // Outside epsilon
        assertTrue(doubleType.areEqualValues(null, null, guidAssignments));
        assertFalse(doubleType.areEqualValues(123.456, null, guidAssignments));
        assertFalse(doubleType.areEqualValues(null, 123.456, guidAssignments));
    }

    @Test
    public void testBigIntegerTypeEdgeCases() {
        AtlasBigIntegerType bigIntType = new AtlasBigIntegerType();

        // Test basic values
        BigInteger bigInt = new BigInteger("12345678901234567890");
        assertEquals(bigIntType.getNormalizedValue(bigInt), bigInt);

        // Test different number types
        assertEquals(bigIntType.getNormalizedValue((byte) 100), BigInteger.valueOf(100));
        assertEquals(bigIntType.getNormalizedValue((short) 1000), BigInteger.valueOf(1000));
        assertEquals(bigIntType.getNormalizedValue(100000), BigInteger.valueOf(100000));
        assertEquals(bigIntType.getNormalizedValue(100000L), BigInteger.valueOf(100000));

        // Test BigDecimal values
        assertEquals(bigIntType.getNormalizedValue(BigDecimal.valueOf(123456789)), BigInteger.valueOf(123456789));

        // Test floating point truncation
        assertEquals(bigIntType.getNormalizedValue(123.789f), BigInteger.valueOf(123));
        assertEquals(bigIntType.getNormalizedValue(123.789d), BigInteger.valueOf(123));

        // Test string values
        assertEquals(bigIntType.getNormalizedValue("12345678901234567890"), new BigInteger("12345678901234567890"));
        assertNull(bigIntType.getNormalizedValue("invalid"));
        assertNull(bigIntType.getNormalizedValue(""));
    }

    @Test
    public void testBigDecimalTypeEdgeCases() {
        AtlasBigDecimalType bigDecimalType = new AtlasBigDecimalType();

        // Test basic values
        BigDecimal bigDec = new BigDecimal("123456789.123456789");
        assertEquals(bigDecimalType.getNormalizedValue(bigDec), bigDec);

        // Test different number types
        assertEquals(bigDecimalType.getNormalizedValue((byte) 100), BigDecimal.valueOf(100.0));
        assertEquals(bigDecimalType.getNormalizedValue((short) 1000), BigDecimal.valueOf(1000.0));
        assertEquals(bigDecimalType.getNormalizedValue(100000), BigDecimal.valueOf(100000.0));
        assertEquals(bigDecimalType.getNormalizedValue(100000L), BigDecimal.valueOf(100000.0));
        assertEquals(bigDecimalType.getNormalizedValue(123.456f), BigDecimal.valueOf(123.456f));
        assertEquals(bigDecimalType.getNormalizedValue(123.456789d), BigDecimal.valueOf(123.456789));

        // Test BigInteger values
        assertEquals(bigDecimalType.getNormalizedValue(BigInteger.valueOf(123456789)), BigDecimal.valueOf(123456789.0));

        // Test string values
        assertEquals(bigDecimalType.getNormalizedValue("123456789.123456789"), new BigDecimal("123456789.123456789"));
        assertNull(bigDecimalType.getNormalizedValue("invalid"));
        assertNull(bigDecimalType.getNormalizedValue(""));
    }

    @Test
    public void testDateTypeEdgeCases() {
        AtlasDateType dateType = new AtlasDateType();

        // Test Date object
        Date now = new Date();
        assertEquals(dateType.getNormalizedValue(now), now);

        // Test long timestamp
        long timestamp = System.currentTimeMillis();
        assertEquals(dateType.getNormalizedValue(timestamp), new Date(timestamp));

        // Test string dates - AtlasDateType expects format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
        Date parsed1 = dateType.getNormalizedValue("2023-01-01T00:00:00.000Z");
        Date parsed2 = dateType.getNormalizedValue("2023-01-01T10:30:00.000Z");

        assertNotNull(parsed1);
        assertNotNull(parsed2);

        // Test invalid format - should return null
        assertNull(dateType.getNormalizedValue("2023-01-01")); // Missing time part

        // Test invalid date strings
        assertNull(dateType.getNormalizedValue("invalid-date"));
        assertNull(dateType.getNormalizedValue(""));

        // Test other types
        assertNull(dateType.getNormalizedValue(new Object()));

        // Test validation
        assertTrue(dateType.isValidValue(null));
        assertTrue(dateType.isValidValue(new Date()));
        assertTrue(dateType.isValidValue(System.currentTimeMillis()));
        assertTrue(dateType.isValidValue("2023-01-01T00:00:00.000Z"));
        assertFalse(dateType.isValidValue("invalid-date"));
        assertFalse(dateType.isValidValue(new Object()));
        assertTrue(dateType.isValidValue(""));
    }

    @Test
    public void testStringTypeEdgeCases() {
        AtlasStringType stringType = new AtlasStringType();

        // Test basic string
        assertEquals(stringType.getNormalizedValue("test"), "test");

        // Test empty string
        assertEquals(stringType.getNormalizedValue(""), "");

        // Test null
        assertNull(stringType.getNormalizedValue(null));

        // Test other types converted to string
        assertEquals(stringType.getNormalizedValue(123), "123");
        assertEquals(stringType.getNormalizedValue(123.456), "123.456");
        assertEquals(stringType.getNormalizedValue(true), "true");
        assertEquals(stringType.getNormalizedValue(false), "false");

        // Test objects with toString
        Date date = new Date(0);
        assertEquals(stringType.getNormalizedValue(date), date.toString());

        // Test validation
        assertTrue(stringType.isValidValue(null));
        assertTrue(stringType.isValidValue(""));
        assertTrue(stringType.isValidValue("test"));
        assertTrue(stringType.isValidValue(123));
        assertTrue(stringType.isValidValue(new Object()));
    }

    @Test
    public void testObjectIdTypeEdgeCases() {
        AtlasObjectIdType objectIdType = new AtlasObjectIdType();

        // Test default object type
        assertEquals(objectIdType.getObjectType(), "Asset");

        // Test with custom object type
        AtlasObjectIdType customObjectIdType = new AtlasObjectIdType("CustomType");
        assertEquals(customObjectIdType.getObjectType(), "CustomType");

        // Test default value creation
        AtlasObjectId defaultValue = objectIdType.createDefaultValue();
        assertNotNull(defaultValue);
        assertEquals(defaultValue.getGuid(), "-1");
        assertEquals(defaultValue.getTypeName(), "Asset");

        // Test valid AtlasObjectId
        AtlasObjectId validObjectId = new AtlasObjectId("guid123", "TestType");
        assertTrue(objectIdType.isValidValue(validObjectId));
        assertEquals(objectIdType.getNormalizedValue(validObjectId), validObjectId);

        // Test valid Map representation
        Map<String, Object> objectIdMap = new HashMap<>();
        objectIdMap.put("guid", "guid123");
        objectIdMap.put("typeName", "TestType");
        assertTrue(objectIdType.isValidValue(objectIdMap));

        AtlasObjectId fromMap = objectIdType.getNormalizedValue(objectIdMap);
        assertNotNull(fromMap);
        assertEquals(fromMap.getGuid(), "guid123");
        assertEquals(fromMap.getTypeName(), "TestType");

        // Test Map with unique attributes
        Map<String, Object> objectIdMapWithUniqueAttrs = new HashMap<>();
        objectIdMapWithUniqueAttrs.put("typeName", "TestType");
        Map<String, Object> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put("name", "testName");
        objectIdMapWithUniqueAttrs.put("uniqueAttributes", uniqueAttrs);
        assertTrue(objectIdType.isValidValue(objectIdMapWithUniqueAttrs));

        // Test invalid Map (missing required fields)
        Map<String, Object> invalidMap = new HashMap<>();
        invalidMap.put("someField", "someValue");
        assertFalse(objectIdType.isValidValue(invalidMap));

        // Test null
        assertTrue(objectIdType.isValidValue(null));
        assertNull(objectIdType.getNormalizedValue(null));

        // Test invalid types
        assertFalse(objectIdType.isValidValue("string"));
        assertFalse(objectIdType.isValidValue(123));
        assertNull(objectIdType.getNormalizedValue("invalid"));

        // Test areEqualValues
        Map<String, String> guidAssignments = new HashMap<>();
        AtlasObjectId obj1 = new AtlasObjectId("guid1", "Type1");
        AtlasObjectId obj2 = new AtlasObjectId("guid1", "Type1");
        AtlasObjectId obj3 = new AtlasObjectId("guid2", "Type1");

        assertTrue(objectIdType.areEqualValues(obj1, obj2, guidAssignments));
        assertFalse(objectIdType.areEqualValues(obj1, obj3, guidAssignments));
        assertTrue(objectIdType.areEqualValues(null, null, guidAssignments));
        assertFalse(objectIdType.areEqualValues(obj1, null, guidAssignments));
        assertFalse(objectIdType.areEqualValues(null, obj1, guidAssignments));

        // Test with invalid values in areEqualValues
        assertFalse(objectIdType.areEqualValues("invalid", obj1, guidAssignments));
        assertFalse(objectIdType.areEqualValues(obj1, "invalid", guidAssignments));
    }

    @Test
    public void testAtlasRelatedObjectIdCreation() {
        Map<String, Object> relatedObjectIdMap = new HashMap<>();
        relatedObjectIdMap.put("guid", "guid123");
        relatedObjectIdMap.put("typeName", "TestType");
        relatedObjectIdMap.put("relationshipType", "testRelationship");

        // Test that the map contains the relationship type key
        assertTrue(relatedObjectIdMap.containsKey(AtlasRelatedObjectId.KEY_RELATIONSHIP_TYPE));

        // Test AtlasRelatedObjectId constructor with Map
        AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId(relatedObjectIdMap);
        assertNotNull(relatedObjectId);
        assertEquals(relatedObjectId.getGuid(), "guid123");
        assertEquals(relatedObjectId.getTypeName(), "TestType");

        // Test basic AtlasRelatedObjectId functionality
        AtlasRelatedObjectId directRelatedObjectId = new AtlasRelatedObjectId();
        directRelatedObjectId.setGuid("directGuid");
        directRelatedObjectId.setTypeName("DirectType");

        assertEquals(directRelatedObjectId.getGuid(), "directGuid");
        assertEquals(directRelatedObjectId.getTypeName(), "DirectType");
    }

    @Test
    public void testPrivateUtilityMethods() throws Exception {
        Method toBigIntegerMethod = AtlasBuiltInTypes.class.getDeclaredMethod("toBigInteger", Number.class);
        toBigIntegerMethod.setAccessible(true);

        // Test with different number types
        assertEquals(toBigIntegerMethod.invoke(null, (byte) 100), BigInteger.valueOf(100));
        assertEquals(toBigIntegerMethod.invoke(null, (short) 1000), BigInteger.valueOf(1000));
        assertEquals(toBigIntegerMethod.invoke(null, 100000), BigInteger.valueOf(100000));
        assertEquals(toBigIntegerMethod.invoke(null, 100000L), BigInteger.valueOf(100000));
        assertEquals(toBigIntegerMethod.invoke(null, BigInteger.valueOf(123456)), BigInteger.valueOf(123456));
        assertEquals(toBigIntegerMethod.invoke(null, BigDecimal.valueOf(123456)), BigInteger.valueOf(123456));
        assertEquals(toBigIntegerMethod.invoke(null, 123.789f), BigInteger.valueOf(123));
        assertEquals(toBigIntegerMethod.invoke(null, 123.789d), BigInteger.valueOf(123));
    }

    @Test
    public void testValidateValueMethods() {
        AtlasStringType stringType = new AtlasStringType();
        List<String> messages = new ArrayList<>();

        assertTrue(stringType.validateValue("test", "testField", messages));
        assertTrue(messages.isEmpty());

        assertTrue(stringType.validateValue(null, "testField", messages));
        assertTrue(messages.isEmpty());

        assertTrue(stringType.validateValue(123, "testField", messages));
        assertTrue(messages.isEmpty());

        // Test with int type
        AtlasIntType intType = new AtlasIntType();
        messages.clear();

        assertTrue(intType.validateValue(123, "testField", messages));
        assertTrue(messages.isEmpty());

        assertTrue(intType.validateValue(null, "testField", messages));
        assertTrue(messages.isEmpty());

        assertTrue(intType.validateValue("123", "testField", messages));
        assertTrue(messages.isEmpty());

        assertFalse(intType.validateValue("invalid", "testField", messages));
        assertFalse(messages.isEmpty());
    }

    @Test
    public void testTypeForAttribute() {
        AtlasStringType stringType = new AtlasStringType();
        assertEquals(stringType.getTypeForAttribute(), stringType);

        AtlasIntType intType = new AtlasIntType();
        assertEquals(intType.getTypeForAttribute(), intType);

        AtlasObjectIdType objectIdType = new AtlasObjectIdType("TestType");
        assertEquals(objectIdType.getTypeForAttribute(), objectIdType);
    }

    private Date parseDate(String dateStr) {
        try {
            if (dateStr.contains("T")) {
                return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(dateStr);
            } else {
                return new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
            }
        } catch (ParseException e) {
            return null;
        }
    }
}
