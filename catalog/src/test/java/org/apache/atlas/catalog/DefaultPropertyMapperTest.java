/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog;

import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.testng.Assert.assertEquals;

/**
 * Unit tests for DefaultPropertyMapper.
 */
public class DefaultPropertyMapperTest {
    @Test
    public void testToCleanName_defaultMappings() {
        String typeName = "testType";
        HierarchicalType dataType = createNiceMock(HierarchicalType.class);

        // currently only use key in map
        Map<String, AttributeInfo> fields = new HashMap<>();
        fields.put("foo", null);
        fields.put("prop", null);
        // can't mock FieldMapping due to direct access to final instance var 'fields'
        FieldMapping fieldMapping = new FieldMapping(fields, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        // mock expectations
        expect(dataType.fieldMapping()).andReturn(fieldMapping).anyTimes();
        replay(dataType);

        PropertyMapper propertyMapper = new TestDefaultPropertyMapper(dataType);
        assertEquals(propertyMapper.toCleanName("Prefix.prop", typeName), "prop");
        assertEquals(propertyMapper.toCleanName("foo", typeName), "foo");
        assertEquals(propertyMapper.toCleanName("other", typeName), "other");
        assertEquals(propertyMapper.toCleanName("Prefix.other", typeName), "Prefix.other");

        verify(dataType);
    }

    @Test
    public void testToQualifiedName_defaultMappings() throws Exception {
        String typeName = "testType";
        HierarchicalType dataType = createNiceMock(HierarchicalType.class);

        // currently only use key in map
        Map<String, AttributeInfo> fields = new HashMap<>();
        fields.put("foo", null);
        fields.put("prop", null);
        // can't mock FieldMapping due to direct access to final instance var 'fields'
        FieldMapping fieldMapping = new FieldMapping(fields, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        // mock expectations
        expect(dataType.fieldMapping()).andReturn(fieldMapping).anyTimes();
        expect(dataType.getQualifiedName("foo")).andReturn("foo");
        expect(dataType.getQualifiedName("prop")).andReturn("Prefix.prop");
        replay(dataType);

        PropertyMapper propertyMapper = new TestDefaultPropertyMapper(dataType);
        assertEquals(propertyMapper.toFullyQualifiedName("foo", typeName), "foo");
        assertEquals(propertyMapper.toFullyQualifiedName("prop", typeName), "Prefix.prop");
        assertEquals(propertyMapper.toFullyQualifiedName("other", typeName), "other");
        assertEquals(propertyMapper.toFullyQualifiedName("Prefix.other", typeName), "Prefix.other");

        verify(dataType);
    }

    @Test
    public void testToCleanName_specifiedMappings() {
        String typeName = "testType";
        HierarchicalType dataType = createNiceMock(HierarchicalType.class);

        // currently only use key in map
        Map<String, AttributeInfo> fields = new HashMap<>();
        fields.put("foo", null);
        fields.put("prop", null);
        // can't mock FieldMapping due to direct access to final instance var 'fields'
        FieldMapping fieldMapping = new FieldMapping(fields, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        // mock expectations
        expect(dataType.fieldMapping()).andReturn(fieldMapping).anyTimes();
        replay(dataType);

        Map<String, String> cleanToQualifiedMap = new HashMap<>();
        cleanToQualifiedMap.put("prop1", "property_1");
        Map<String, String> qualifiedToCleanMap = new HashMap<>();
        qualifiedToCleanMap.put("property_1", "prop1");

        PropertyMapper propertyMapper = new TestDefaultPropertyMapper(
                typeName, qualifiedToCleanMap, cleanToQualifiedMap, dataType);

        assertEquals(propertyMapper.toCleanName("property_1", typeName), "prop1");
        assertEquals(propertyMapper.toCleanName("Prefix.prop", typeName), "prop");
        assertEquals(propertyMapper.toCleanName("foo", typeName), "foo");
        assertEquals(propertyMapper.toCleanName("other", typeName), "other");
        assertEquals(propertyMapper.toCleanName("Prefix.other", typeName), "Prefix.other");

        verify(dataType);
    }

    @Test
    public void testToQualifiedName_specifiedMappings() throws Exception {
        String typeName = "testType";
        HierarchicalType dataType = createNiceMock(HierarchicalType.class);

        // currently only use key in map
        Map<String, AttributeInfo> fields = new HashMap<>();
        fields.put("foo", null);
        fields.put("prop", null);
        // can't mock FieldMapping due to direct access to final instance var 'fields'
        FieldMapping fieldMapping = new FieldMapping(fields, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        // mock expectations
        expect(dataType.fieldMapping()).andReturn(fieldMapping).anyTimes();
        expect(dataType.getQualifiedName("foo")).andReturn("foo");
        expect(dataType.getQualifiedName("prop")).andReturn("Prefix.prop");
        replay(dataType);

        Map<String, String> cleanToQualifiedMap = new HashMap<>();
        cleanToQualifiedMap.put("prop1", "property_1");
        Map<String, String> qualifiedToCleanMap = new HashMap<>();
        qualifiedToCleanMap.put("property_1", "prop1");

        PropertyMapper propertyMapper = new TestDefaultPropertyMapper(
                typeName, qualifiedToCleanMap, cleanToQualifiedMap, dataType);

        assertEquals(propertyMapper.toFullyQualifiedName("prop1", typeName), "property_1");
        assertEquals(propertyMapper.toFullyQualifiedName("foo", typeName), "foo");
        assertEquals(propertyMapper.toFullyQualifiedName("prop", typeName), "Prefix.prop");
        assertEquals(propertyMapper.toFullyQualifiedName("other", typeName), "other");
        assertEquals(propertyMapper.toFullyQualifiedName("Prefix.other", typeName), "Prefix.other");

        verify(dataType);
    }

    private static class TestDefaultPropertyMapper extends DefaultPropertyMapper {
        private HierarchicalType dataType;
        public TestDefaultPropertyMapper(HierarchicalType dataType) {
            super();
            this.dataType = dataType;
        }

        public TestDefaultPropertyMapper(String type,
                                         Map<String, String> qualifiedToCleanMap,
                                         Map<String, String> cleanToQualifiedMap,
                                         HierarchicalType dataType) {

            super(qualifiedToCleanMap, cleanToQualifiedMap);
            this.dataType = dataType;
        }

        @Override
        protected HierarchicalType createDataType(String type) {
            return dataType;
        }
    }
}
