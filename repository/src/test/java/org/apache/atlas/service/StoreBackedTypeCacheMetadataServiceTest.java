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
package org.apache.atlas.service;

import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.repository.typestore.StoreBackedTypeCache;
import org.apache.atlas.repository.typestore.StoreBackedTypeCacheTestModule;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUpdateException;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;


/**
 *  Verify MetadataService type operations trigger StoreBackedTypeCache to load non-cached types from the store.
 *  StoreBackedTypeCacheTestModule Guice module sets Atlas configuration
 *  to use {@link StoreBackedTypeCache} as the TypeCache implementation class.
 */
@Guice(modules = StoreBackedTypeCacheTestModule.class)
public class StoreBackedTypeCacheMetadataServiceTest
{
    @Inject
    private MetadataService metadataService;

    @Inject
    private ITypeStore typeStore;

    @Inject
    TypeCache typeCache;

    private StoreBackedTypeCache storeBackedTypeCache;

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    private TypeSystem ts;

    @BeforeClass
    public void oneTimeSetup() throws Exception {
        Assert.assertTrue(typeCache instanceof StoreBackedTypeCache);
        storeBackedTypeCache = (StoreBackedTypeCache) typeCache;

        ts = TypeSystem.getInstance();
        ts.reset();

        // Populate the type store for testing.
        TestUtils.defineDeptEmployeeTypes(ts);
        TestUtils.createHiveTypes(ts);
        ImmutableList<String> typeNames = ts.getTypeNames();
        typeStore.store(ts, typeNames);
    }

    @BeforeMethod
    public void setUp() throws Exception {
        ts.reset();
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

    @Test
    public void testGetTypeDefinition() throws Exception {
        // Cache should be empty
        Assert.assertFalse(storeBackedTypeCache.isCachedInMemory("Manager"));

        // Type lookup on MetadataService should cause Manager type to be loaded from the type store
        // and cached.
        Assert.assertNotNull(metadataService.getTypeDefinition("Manager"));
        Assert.assertTrue(storeBackedTypeCache.isCachedInMemory("Manager"));
    }

    @Test
    public void testValidUpdateType() throws Exception {
        // Cache should be empty
        Assert.assertFalse(storeBackedTypeCache.isCachedInMemory(TestUtils.TABLE_TYPE));

        TypesDef typesDef = TestUtils.defineHiveTypes();
        String json = TypesSerialization.toJson(typesDef);

        // Update types with same definition, which should succeed.
        metadataService.updateType(json);

        // hive_table type should now be cached.
        Assert.assertTrue(storeBackedTypeCache.isCachedInMemory(TestUtils.TABLE_TYPE));
    }

    @Test
    public void testInvalidUpdateType() throws Exception {
        // Cache should be empty
        Assert.assertFalse(storeBackedTypeCache.isCachedInMemory(TestUtils.TABLE_TYPE));

        HierarchicalTypeDefinition<ClassType> classTypeDef = TypesUtil.createClassTypeDef(TestUtils.TABLE_TYPE, ImmutableSet.<String>of(),
            new AttributeDefinition("attr1", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null));
        String json = TypesSerialization.toJson(classTypeDef, false);

        // Try to update the type with disallowed changes.  Should fail with TypeUpdateException.
        try {
            metadataService.updateType(json);
            Assert.fail(TypeUpdateException.class.getSimpleName() + " was expected but none thrown");
        }
        catch(TypeUpdateException e) {
            // good
        }

        // hive_table type should now be cached.
        Assert.assertTrue(storeBackedTypeCache.isCachedInMemory(TestUtils.TABLE_TYPE));
    }
}
