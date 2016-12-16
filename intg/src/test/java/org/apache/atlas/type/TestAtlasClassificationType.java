/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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
package org.apache.atlas.type;

import java.util.*;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.ModelTestUtil;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestAtlasClassificationType {
    private final AtlasClassificationType classificationType;
    private final List<Object>            validValues        = new ArrayList<>();
    private final List<Object>            invalidValues      = new ArrayList<>();

    {
        classificationType = getClassificationType(ModelTestUtil.getClassificationDefWithSuperTypes());

        AtlasClassification invalidValue1 = classificationType.createDefaultValue();
        AtlasClassification invalidValue2 = classificationType.createDefaultValue();
        Map<String, Object> invalidValue3 = classificationType.createDefaultValue().getAttributes();

        // invalid value for int
        invalidValue1.setAttribute(ModelTestUtil.getDefaultAttributeName(AtlasBaseTypeDef.ATLAS_TYPE_INT), "xyz");
        // invalid value for date
        invalidValue2.setAttribute(ModelTestUtil.getDefaultAttributeName(AtlasBaseTypeDef.ATLAS_TYPE_DATE), "xyz");
        // invalid value for bigint
        invalidValue3.put(ModelTestUtil.getDefaultAttributeName(AtlasBaseTypeDef.ATLAS_TYPE_BIGINTEGER), "xyz");

        validValues.add(null);
        validValues.add(classificationType.createDefaultValue());
        validValues.add(classificationType.createDefaultValue().getAttributes()); // Map<String, Object>
        invalidValues.add(invalidValue1);
        invalidValues.add(invalidValue2);
        invalidValues.add(invalidValue3);
        invalidValues.add(new AtlasClassification());     // no values for mandatory attributes
        invalidValues.add(new HashMap<>()); // no values for mandatory attributes
        invalidValues.add(1);               // incorrect datatype
        invalidValues.add(new HashSet());   // incorrect datatype
        invalidValues.add(new ArrayList()); // incorrect datatype
        invalidValues.add(new String[] {}); // incorrect datatype
    }

    @Test
    public void testClassificationTypeDefaultValue() {
        AtlasClassification defValue = classificationType.createDefaultValue();

        assertNotNull(defValue);
        assertEquals(defValue.getTypeName(), classificationType.getTypeName());
    }

    @Test
    public void testClassificationTypeIsValidValue() {
        for (Object value : validValues) {
            assertTrue(classificationType.isValidValue(value), "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(classificationType.isValidValue(value), "value=" + value);
        }
    }

    @Test
    public void testClassificationTypeGetNormalizedValue() {
        assertNull(classificationType.getNormalizedValue(null), "value=" + null);

        for (Object value : validValues) {
            if (value == null) {
                continue;
            }

            Object normalizedValue = classificationType.getNormalizedValue(value);

            assertNotNull(normalizedValue, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertNull(classificationType.getNormalizedValue(value), "value=" + value);
        }
    }

    @Test
    public void testClassificationTypeValidateValue() {
        List<String> messages = new ArrayList<>();
        for (Object value : validValues) {
            assertTrue(classificationType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 0, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(classificationType.validateValue(value, "testObj", messages));
            assertTrue(messages.size() > 0, "value=" + value);
            messages.clear();
        }
    }

    private static AtlasClassificationType getClassificationType(AtlasClassificationDef classificationDef) {
        try {
            return new AtlasClassificationType(classificationDef, ModelTestUtil.getTypesRegistry());
        } catch (AtlasBaseException excp) {
            return null;
        }
    }
}
