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
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.repository.typestore.StoreBackedTypeCache;
import org.apache.atlas.repository.typestore.StoreBackedTypeCacheTestModule;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;


/**
 *  Verify MetadataService type lookup triggers StoreBackedTypeCache to load type from the store.
 *  StoreBackedTypeCacheTestModule Guice module uses Atlas configuration
 *  which has type cache implementation class set to {@link StoreBackedTypeCache}.
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

    private TypeSystem ts;

    @BeforeClass
    public void setUp() throws Exception {
        ts = TypeSystem.getInstance();
        ts.reset();

        // Populate the type store for testing.
        TestUtils.defineDeptEmployeeTypes(ts);
        TestUtils.createHiveTypes(ts);
        ImmutableList<String> typeNames = ts.getTypeNames();
        typeStore.store(ts, typeNames);
        ts.reset();
    }

    @Test
    public void testIt() throws Exception {
        Assert.assertTrue(typeCache instanceof StoreBackedTypeCache);
        StoreBackedTypeCache storeBackedCache = (StoreBackedTypeCache) typeCache;

        // Cache should be empty
        Assert.assertFalse(storeBackedCache.isCachedInMemory("Manager"));

        // Type lookup on MetadataService should cause Manager type to be loaded from the type store
        // and cached.
        Assert.assertNotNull(metadataService.getTypeDefinition("Manager"));
        Assert.assertTrue(storeBackedCache.isCachedInMemory("Manager"));
    }
}
