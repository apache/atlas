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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestAtlasEntityType {
    private final AtlasEntityType entityType;
    private final List<Object>    validValues   = new ArrayList<Object>();
    private final List<Object>    invalidValues = new ArrayList<Object>();

    {
        entityType = getEntityType(ModelTestUtil.getEntityDefWithSuperTypes());

        AtlasEntity         invalidValue1 = entityType.createDefaultValue();
        AtlasEntity         invalidValue2 = entityType.createDefaultValue();
        Map<String, Object> invalidValue3 = entityType.createDefaultValue().getAttributes();

        // invalid value for int
        invalidValue1.setAttribute(ModelTestUtil.getDefaultAttributeName(AtlasBaseTypeDef.ATLAS_TYPE_INT), "xyz");
        // invalid value for date
        invalidValue2.setAttribute(ModelTestUtil.getDefaultAttributeName(AtlasBaseTypeDef.ATLAS_TYPE_DATE), "xyz");
        // invalid value for bigint
        invalidValue3.put(ModelTestUtil.getDefaultAttributeName(AtlasBaseTypeDef.ATLAS_TYPE_BIGINTEGER), "xyz");

        validValues.add(null);
        validValues.add(entityType.createDefaultValue());
        validValues.add(entityType.createDefaultValue().getAttributes()); // Map<String, Object>
        invalidValues.add(invalidValue1);
        invalidValues.add(invalidValue2);
        invalidValues.add(invalidValue3);
        invalidValues.add(new AtlasEntity());             // no values for mandatory attributes
        invalidValues.add(new HashMap<Object, Object>()); // no values for mandatory attributes
        invalidValues.add(1);               // incorrect datatype
        invalidValues.add(new HashSet());   // incorrect datatype
        invalidValues.add(new ArrayList()); // incorrect datatype
        invalidValues.add(new String[] {}); // incorrect datatype
    }

    @Test
    public void testEntityTypeDefaultValue() {
        AtlasEntity defValue = entityType.createDefaultValue();

        assertNotNull(defValue);
        assertEquals(defValue.getTypeName(), entityType.getTypeName());
    }

    @Test
    public void testEntityTypeIsValidValue() {
        for (Object value : validValues) {
            assertTrue(entityType.isValidValue(value), "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(entityType.isValidValue(value), "value=" + value);
        }
    }

    @Test
    public void testEntityTypeGetNormalizedValue() {
        assertNull(entityType.getNormalizedValue(null), "value=" + null);

        for (Object value : validValues) {
            if (value == null) {
                continue;
            }

            Object normalizedValue = entityType.getNormalizedValue(value);

            assertNotNull(normalizedValue, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertNull(entityType.getNormalizedValue(value), "value=" + value);
        }
    }

    @Test
    public void testEntityTypeValidateValue() {
        List<String> messages = new ArrayList<String>();
        for (Object value : validValues) {
            assertTrue(entityType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 0, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(entityType.validateValue(value, "testObj", messages));
            assertTrue(messages.size() > 0, "value=" + value);
            messages.clear();
        }
    }

    private static AtlasEntityType getEntityType(AtlasEntityDef entityDef) {
        try {
            return new AtlasEntityType(entityDef, ModelTestUtil.getTypesRegistry());
        } catch (AtlasBaseException excp) {
            return null;
        }
    }
}
