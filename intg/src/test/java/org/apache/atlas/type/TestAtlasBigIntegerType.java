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

import org.apache.atlas.type.AtlasBuiltInTypes.AtlasBigIntegerType;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasBigIntegerType {
    private final AtlasBigIntegerType bigIntegerType = new AtlasBigIntegerType();

    private final Object[] validValues = {
            null, (byte) 1, (short) 1, 1, 1L, 1F,
            1.0, BigInteger.valueOf(1), BigDecimal.valueOf(1), "1",
    };

    private final Object[] negativeValues = {
            (byte) -1, (short) -1, -1, -1L, (float) -1,
            (double) -1, BigInteger.valueOf(-1), BigDecimal.valueOf(-1), "-1",
    };

    private final Object[] invalidValues = {"", "12ab", "abcd", "-12ab", };

    @Test
    public void testBigIntegerTypeDefaultValue() {
        BigInteger defValue = bigIntegerType.createDefaultValue();

        assertEquals(defValue, BigInteger.valueOf(0));
    }

    @Test
    public void testBigIntegerTypeIsValidValue() {
        for (Object value : validValues) {
            assertTrue(bigIntegerType.isValidValue(value), "value=" + value);
        }

        for (Object value : negativeValues) {
            assertTrue(bigIntegerType.isValidValue(value), "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(bigIntegerType.isValidValue(value), "value=" + value);
        }
    }

    @Test
    public void testBigIntegerTypeGetNormalizedValue() {
        assertNull(bigIntegerType.getNormalizedValue(null), "value=" + null);

        for (Object value : validValues) {
            if (value == null) {
                continue;
            }

            BigInteger normalizedValue = bigIntegerType.getNormalizedValue(value);

            assertNotNull(normalizedValue, "value=" + value);
            assertEquals(normalizedValue, BigInteger.valueOf(1), "value=" + value);
        }

        for (Object value : negativeValues) {
            BigInteger normalizedValue = bigIntegerType.getNormalizedValue(value);

            assertNotNull(normalizedValue, "value=" + value);
            assertEquals(normalizedValue, BigInteger.valueOf(-1), "value=" + value);
        }

        for (Object value : invalidValues) {
            assertNull(bigIntegerType.getNormalizedValue(value), "value=" + value);
        }
    }

    @Test
    public void testBigIntegerTypeValidateValue() {
        List<String> messages = new ArrayList<>();
        for (Object value : validValues) {
            assertTrue(bigIntegerType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 0, "value=" + value);
        }

        for (Object value : negativeValues) {
            assertTrue(bigIntegerType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 0, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(bigIntegerType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 1, "value=" + value);
            messages.clear();
        }
    }
}
