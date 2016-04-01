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
package org.apache.atlas.typesystem.types;

import java.util.Arrays;
import java.util.List;

import org.apache.atlas.AtlasException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;


public class HierarchicalTypeDependencySorterTest {

    @BeforeMethod
    public void setup() throws Exception {
        TypeSystem ts = TypeSystem.getInstance();
        ts.reset();
    }
    
    @SuppressWarnings("rawtypes")
    @Test
    public void testSimpleModel() throws AtlasException {
        TypeSystem ts = TypeSystem.getInstance();
        HierarchicalType a = new ClassType(ts, "a", null, ImmutableSet.<String>of(), 0);
        HierarchicalType b = new ClassType(ts, "B", null, ImmutableSet.of("a"), 0);
        HierarchicalType c = new ClassType(ts, "C", null, ImmutableSet.of("B"), 0);

        List<HierarchicalType> unsortedTypes = Arrays.asList(c, a, b);
        List<HierarchicalType> sortedTypes = HierarchicalTypeDependencySorter.sortTypes(unsortedTypes);
        Assert.assertEquals(sortedTypes.size(), 3);
        Assert.assertEquals(sortedTypes.indexOf(a), 0);
        Assert.assertEquals(sortedTypes.indexOf(b), 1);
        Assert.assertEquals(sortedTypes.indexOf(c), 2);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testLargerModel() throws Exception {
        TypeSystem ts = TypeSystem.getInstance();
        HierarchicalType testObjectType = new ClassType(ts, "TestObject", null, ImmutableSet.<String>of(), 0);
        HierarchicalType testDataSetType = new ClassType(ts, "TestDataSet", null, ImmutableSet.of("TestObject"), 0);
        HierarchicalType testColumnType = new ClassType(ts, "TestColumn", null, ImmutableSet.of("TestObject"), 0);
        HierarchicalType testRelationalDataSetType = new ClassType(ts, "TestRelationalDataSet", null, ImmutableSet.of("TestDataSet"), 0);
        HierarchicalType testTableType = new ClassType(ts, "Table", null, ImmutableSet.of("TestDataSet"), 0);
        HierarchicalType testDataFileType = new ClassType(ts, "TestDataFile", null, ImmutableSet.of("TestRelationalDataSet"), 0);
        HierarchicalType testDocumentType = new ClassType(ts, "TestDocument", null, ImmutableSet.of("TestDataSet"), 0);
        HierarchicalType testAnnotationType = new ClassType(ts, "TestAnnotation", null, ImmutableSet.<String>of(), 0);
        HierarchicalType myNewAnnotationType = new ClassType(ts, "MyNewAnnotation", null, ImmutableSet.of("TestAnnotation"), 0);

        List<HierarchicalType> unsortedTypes = Arrays.asList(testTableType, testColumnType, myNewAnnotationType, testDataSetType,
            testDataFileType, testAnnotationType, testRelationalDataSetType, testObjectType, testDocumentType);
        List<HierarchicalType> sortedTypes = HierarchicalTypeDependencySorter.sortTypes(unsortedTypes);
        // Verify that super types were sorted before their subtypes.
        Assert.assertTrue(sortedTypes.indexOf(testObjectType) < sortedTypes.indexOf(testDataSetType));
        Assert.assertTrue(sortedTypes.indexOf(testObjectType) < sortedTypes.indexOf(testColumnType));
        Assert.assertTrue(sortedTypes.indexOf(testDataSetType) < sortedTypes.indexOf(testRelationalDataSetType));
        Assert.assertTrue(sortedTypes.indexOf(testDataSetType) < sortedTypes.indexOf(testDocumentType));
        Assert.assertTrue(sortedTypes.indexOf(testDataSetType) < sortedTypes.indexOf(testTableType));
        Assert.assertTrue(sortedTypes.indexOf(testRelationalDataSetType) < sortedTypes.indexOf(testDataFileType));
        Assert.assertTrue(sortedTypes.indexOf(testAnnotationType) < sortedTypes.indexOf(myNewAnnotationType));
    }
}
