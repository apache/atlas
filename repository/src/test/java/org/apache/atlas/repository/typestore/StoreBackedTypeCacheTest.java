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
package org.apache.atlas.repository.typestore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;


/**
 * Unit test for {@link StoreBackedTypeCache}
 */
@Guice(modules = RepositoryMetadataModule.class)
public class StoreBackedTypeCacheTest {

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @Inject
    private ITypeStore typeStore;

    @Inject
    private StoreBackedTypeCache typeCache;

    private TypeSystem ts;

    private Map<String, ClassType> classTypesToTest = new HashMap<>();

    @BeforeClass
    public void setUp() throws Exception {
        ts = TypeSystem.getInstance();
        ts.reset();
        ts.setTypeCache(typeCache);

        // Populate the type store for testing.
        TestUtils.defineDeptEmployeeTypes(ts);
        TestUtils.createHiveTypes(ts);
        ImmutableList<String> typeNames = ts.getTypeNames();
        typeStore.store(ts, typeNames);

        ClassType type = ts.getDataType(ClassType.class, "Manager");
        classTypesToTest.put("Manager", type);
        type = ts.getDataType(ClassType.class, TestUtils.TABLE_TYPE);
        classTypesToTest.put(TestUtils.TABLE_TYPE, type);
    }

    @AfterClass
    public void tearDown() throws Exception {
        ts.reset();
        try {
            graphProvider.get().shutdown();
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        try {
            TitanCleanup.clear(graphProvider.get());
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeMethod
    public void setupTestMethod() throws Exception {
        ts.reset();
    }

    @Test
    public void testGetClassType() throws Exception {
        for (Map.Entry<String, ClassType> typeEntry : classTypesToTest.entrySet()) {
            // Not cached yet
            Assert.assertFalse(typeCache.isCachedInMemory(typeEntry.getKey()));

            IDataType dataType = ts.getDataType(IDataType.class, typeEntry.getKey());
            // Verify the type is now cached.
            Assert.assertTrue(typeCache.isCachedInMemory(typeEntry.getKey()));

            Assert.assertTrue(dataType instanceof ClassType);
            ClassType cachedType = (ClassType)dataType;
            // Verify that get() also loaded and cached any dependencies of this type from the type store.
            verifyHierarchicalType(cachedType, typeEntry.getValue());
        }
    }

    @Test
    public void testGetTraitType() throws Exception {
        ImmutableList<String> traitNames = ts.getTypeNamesByCategory(TypeCategory.TRAIT);
        for (String traitTypeName : traitNames) {
            // Not cached yet
            Assert.assertFalse(typeCache.isCachedInMemory(traitTypeName));

            IDataType dataType = typeCache.get(traitTypeName);
            // Verify the type is now cached.
            Assert.assertTrue(typeCache.isCachedInMemory(traitTypeName));

            Assert.assertTrue(dataType instanceof TraitType);
            TraitType cachedType = (TraitType)dataType;
            // Verify that get() also loaded and cached any dependencies of this type from the type store.
            verifyHierarchicalType(cachedType, ts.getDataType(TraitType.class, traitTypeName));
        }
    }

    private <T extends HierarchicalType> void verifyHierarchicalType(T dataType, T expectedDataType) throws AtlasException {
        Assert.assertEquals(dataType.numFields, expectedDataType.numFields);
        Assert.assertEquals(dataType.immediateAttrs.size(), expectedDataType.immediateAttrs.size());
        Assert.assertEquals(dataType.fieldMapping().fields.size(), expectedDataType.fieldMapping().fields.size());
        ImmutableSet<String> superTypes = dataType.superTypes;
        Assert.assertEquals(superTypes.size(), expectedDataType.superTypes.size());

        // Verify that any attribute and super types were also cached.
        for (String superTypeName : superTypes) {
            Assert.assertTrue(typeCache.has(superTypeName));
        }
        for (AttributeInfo attrInfo : dataType.fieldMapping().fields.values()) {
            switch (attrInfo.dataType().getTypeCategory()) {
            case CLASS:
            case STRUCT:
            case ENUM:
                Assert.assertTrue(typeCache.has(attrInfo.dataType().getName()), attrInfo.dataType().getName() + " should be cached");
                break;
            case ARRAY:
                String elementTypeName = TypeUtils.parseAsArrayType(attrInfo.dataType().getName());
                if (!ts.getCoreTypes().contains(elementTypeName)) {
                    Assert.assertTrue(typeCache.has(elementTypeName), elementTypeName + " should be cached");
                }
                break;
            case MAP:
                String[] mapTypeNames = TypeUtils.parseAsMapType(attrInfo.dataType().getName());
                for (String typeName : mapTypeNames) {
                    if (!ts.getCoreTypes().contains(typeName)) {
                        Assert.assertTrue(typeCache.has(typeName), typeName + " should be cached");
                    }
                }
                break;
            default:
                break;
            }
        }
    }
}
