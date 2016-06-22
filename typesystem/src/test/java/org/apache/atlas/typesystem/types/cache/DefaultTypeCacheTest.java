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
package org.apache.atlas.typesystem.types.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Tests functional behavior of {@link DefaultTypeCache}
 */
@SuppressWarnings("rawtypes")
public class DefaultTypeCacheTest {

    private String CLASSTYPE_CUSTOMER = "Customer";
    private String STRUCTTYPE_ADDRESS = "Address";
    private String TRAITTYPE_PRIVILEGED = "Privileged";
    private String ENUMTYPE_SHIPPING = "Shipping";

    private String UNKNOWN_TYPE = "UndefinedType";

    private ClassType customerType;
    private StructType addressType;
    private TraitType privilegedTrait;
    private EnumType shippingEnum;

    private DefaultTypeCache cache;

    @BeforeClass
    public void onetimeSetup() throws Exception {

        // init TypeSystem
        TypeSystem ts = TypeSystem.getInstance().reset();

        // Customer ClassType
        customerType = ts.defineClassType(TypesUtil
            .createClassTypeDef(CLASSTYPE_CUSTOMER, ImmutableSet.<String>of(),
                TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                TypesUtil.createRequiredAttrDef("id", DataTypes.LONG_TYPE)));

        // Address StructType
        addressType = ts.defineStructType(STRUCTTYPE_ADDRESS, true,
                TypesUtil.createRequiredAttrDef("first line", DataTypes.STRING_TYPE),
                TypesUtil.createOptionalAttrDef("second line", DataTypes.STRING_TYPE),
                TypesUtil.createRequiredAttrDef("city", DataTypes.STRING_TYPE),
                TypesUtil.createRequiredAttrDef("pincode", DataTypes.INT_TYPE));

        // Privileged TraitType
        privilegedTrait = ts.defineTraitType(TypesUtil
                .createTraitTypeDef(TRAITTYPE_PRIVILEGED, ImmutableSet.<String>of(),
                        TypesUtil.createRequiredAttrDef("category", DataTypes.INT_TYPE)));

        // Shipping EnumType
        shippingEnum = ts.defineEnumType(TypesUtil.createEnumTypeDef(ENUMTYPE_SHIPPING,
            new EnumValue("Domestic", 1), new EnumValue("International", 2)));
    }

    @BeforeMethod
    public void eachTestSetup() throws Exception {

        cache = new DefaultTypeCache();

        cache.put(customerType);
        cache.put(addressType);
        cache.put(privilegedTrait);
        cache.put(shippingEnum);
    }

    @Test
    public void testCacheGetType() throws Exception {

        IDataType custType = cache.get(CLASSTYPE_CUSTOMER);
        verifyType(custType, CLASSTYPE_CUSTOMER, ClassType.class);

        IDataType addrType = cache.get(STRUCTTYPE_ADDRESS);
        verifyType(addrType, STRUCTTYPE_ADDRESS, StructType.class);

        IDataType privTrait = cache.get(TRAITTYPE_PRIVILEGED);
        verifyType(privTrait, TRAITTYPE_PRIVILEGED, TraitType.class);

        IDataType shippingEnum = cache.get(ENUMTYPE_SHIPPING);
        verifyType(shippingEnum, ENUMTYPE_SHIPPING, EnumType.class);

        assertNull(cache.get(UNKNOWN_TYPE));
    }

    @Test
    public void testCacheGetTypeByCategory() throws Exception {

        IDataType custType = cache.get(TypeCategory.CLASS, CLASSTYPE_CUSTOMER);
        verifyType(custType, CLASSTYPE_CUSTOMER, ClassType.class);

        IDataType addrType = cache.get(TypeCategory.STRUCT, STRUCTTYPE_ADDRESS);
        verifyType(addrType, STRUCTTYPE_ADDRESS, StructType.class);

        IDataType privTrait = cache.get(TypeCategory.TRAIT, TRAITTYPE_PRIVILEGED);
        verifyType(privTrait, TRAITTYPE_PRIVILEGED, TraitType.class);

        IDataType shippingEnum = cache.get(TypeCategory.ENUM, ENUMTYPE_SHIPPING);
        verifyType(shippingEnum, ENUMTYPE_SHIPPING, EnumType.class);

        assertNull(cache.get(UNKNOWN_TYPE));
    }

    private void verifyType(IDataType actualType, String expectedName, Class<? extends IDataType> typeClass) {

        assertNotNull(actualType, "The " + expectedName + " type not in cache");
        assertTrue(typeClass.isInstance(actualType));
        assertEquals(actualType.getName(), expectedName, "The type name does not match");
    }

    @Test
    public void testCacheHasType() throws Exception {

        assertTrue(cache.has(CLASSTYPE_CUSTOMER));
        assertTrue(cache.has(STRUCTTYPE_ADDRESS));
        assertTrue(cache.has(TRAITTYPE_PRIVILEGED));
        assertTrue(cache.has(ENUMTYPE_SHIPPING));

        assertFalse(cache.has(UNKNOWN_TYPE));
    }

    @Test
    public void testCacheHasTypeByCategory() throws Exception {

        assertTrue(cache.has(TypeCategory.CLASS, CLASSTYPE_CUSTOMER));
        assertTrue(cache.has(TypeCategory.STRUCT, STRUCTTYPE_ADDRESS));
        assertTrue(cache.has(TypeCategory.TRAIT, TRAITTYPE_PRIVILEGED));
        assertTrue(cache.has(TypeCategory.ENUM, ENUMTYPE_SHIPPING));

        assertFalse(cache.has(UNKNOWN_TYPE));
    }

    @Test
    public void testCacheGetAllTypeNames() throws Exception {

        List<String> allTypeNames = new ArrayList<String>(cache.getAllTypeNames());
        Collections.sort(allTypeNames);

        final int EXPECTED_TYPE_COUNT = 4;
        assertEquals(allTypeNames.size(), EXPECTED_TYPE_COUNT, "Total number of types does not match.");

        assertEquals(STRUCTTYPE_ADDRESS, allTypeNames.get(0));
        assertEquals(CLASSTYPE_CUSTOMER, allTypeNames.get(1));
        assertEquals(TRAITTYPE_PRIVILEGED, allTypeNames.get(2));
        assertEquals(ENUMTYPE_SHIPPING, allTypeNames.get(3));
    }

    @Test
    public void testCacheGetTypeNamesByCategory() throws Exception {

        List<String> classTypes = new ArrayList<String>(cache.getTypeNames(TypeCategory.CLASS));
        final int EXPECTED_CLASSTYPE_COUNT = 1;
        assertEquals(classTypes.size(), EXPECTED_CLASSTYPE_COUNT);
        assertEquals(CLASSTYPE_CUSTOMER, classTypes.get(0));

        List<String> structTypes = new ArrayList<String>(cache.getTypeNames(TypeCategory.STRUCT));
        final int EXPECTED_STRUCTTYPE_COUNT = 1;
        assertEquals(structTypes.size(), EXPECTED_STRUCTTYPE_COUNT);
        assertEquals(STRUCTTYPE_ADDRESS, structTypes.get(0));

        List<String> traitTypes = new ArrayList<String>(cache.getTypeNames(TypeCategory.TRAIT));
        final int EXPECTED_TRAITTYPE_COUNT = 1;
        assertEquals(traitTypes.size(), EXPECTED_TRAITTYPE_COUNT);
        assertEquals(TRAITTYPE_PRIVILEGED, traitTypes.get(0));

        List<String> enumTypes = new ArrayList<String>(cache.getTypeNames(TypeCategory.ENUM));
        final int EXPECTED_ENUMTYPE_COUNT = 1;
        assertEquals(enumTypes.size(), EXPECTED_ENUMTYPE_COUNT);
        assertEquals(ENUMTYPE_SHIPPING, enumTypes.get(0));
    }

    @Test
    public void testCacheBulkInsert() throws Exception {

        List<IDataType> allTypes = new ArrayList<>();
        allTypes.add(customerType);
        allTypes.add(addressType);
        allTypes.add(privilegedTrait);
        allTypes.add(shippingEnum);

        // create a new cache instead of using the one setup for every method call
        cache = new DefaultTypeCache();
        cache.putAll(allTypes);

        IDataType custType = cache.get(CLASSTYPE_CUSTOMER);
        verifyType(custType, CLASSTYPE_CUSTOMER, ClassType.class);

        IDataType addrType = cache.get(STRUCTTYPE_ADDRESS);
        verifyType(addrType, STRUCTTYPE_ADDRESS, StructType.class);

        IDataType privTrait = cache.get(TRAITTYPE_PRIVILEGED);
        verifyType(privTrait, TRAITTYPE_PRIVILEGED, TraitType.class);

        IDataType shippingEnum = cache.get(ENUMTYPE_SHIPPING);
        verifyType(shippingEnum, ENUMTYPE_SHIPPING, EnumType.class);
    }

    @Test
    public void testCacheRemove() throws Exception {

        cache.remove(CLASSTYPE_CUSTOMER);
        assertNull(cache.get(CLASSTYPE_CUSTOMER));
        assertFalse(cache.has(CLASSTYPE_CUSTOMER));
        assertTrue(cache.getTypeNames(TypeCategory.CLASS).isEmpty());

        final int EXPECTED_TYPE_COUNT = 3;
        assertEquals(cache.getAllTypeNames().size(), EXPECTED_TYPE_COUNT);
    }

    @Test
    public void testCacheRemoveByCategory() throws Exception {

        cache.remove(TypeCategory.CLASS, CLASSTYPE_CUSTOMER);
        assertNull(cache.get(CLASSTYPE_CUSTOMER));
        assertFalse(cache.has(CLASSTYPE_CUSTOMER));
        assertTrue(cache.getTypeNames(TypeCategory.CLASS).isEmpty());

        final int EXPECTED_TYPE_COUNT = 3;
        assertEquals(cache.getAllTypeNames().size(), EXPECTED_TYPE_COUNT);
    }

    @Test
    public void testCacheClear() throws Exception {

        cache.clear();

        assertNull(cache.get(CLASSTYPE_CUSTOMER));
        assertFalse(cache.has(CLASSTYPE_CUSTOMER));

        assertNull(cache.get(STRUCTTYPE_ADDRESS));
        assertFalse(cache.has(STRUCTTYPE_ADDRESS));

        assertNull(cache.get(TRAITTYPE_PRIVILEGED));
        assertFalse(cache.has(TRAITTYPE_PRIVILEGED));

        assertNull(cache.get(ENUMTYPE_SHIPPING));
        assertFalse(cache.has(ENUMTYPE_SHIPPING));

        assertTrue(cache.getTypeNames(TypeCategory.CLASS).isEmpty());
        assertTrue(cache.getTypeNames(TypeCategory.STRUCT).isEmpty());
        assertTrue(cache.getTypeNames(TypeCategory.TRAIT).isEmpty());
        assertTrue(cache.getTypeNames(TypeCategory.ENUM).isEmpty());

        assertTrue(cache.getAllTypeNames().isEmpty());
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testPutTypeWithNullType() throws Exception {

        cache.put(null);
        fail("Null type should be not allowed in 'put'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testPutTypeWithInvalidType() throws Exception {

        cache.put(DataTypes.BOOLEAN_TYPE);
        fail("type should only be an instance of ClassType | EnumType | StructType | TraitType in 'put'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testGetTypeWithNullCategory() throws Exception {

        cache.get(null, CLASSTYPE_CUSTOMER);
        fail("Null TypeCategory should be not allowed in 'get'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testGetTypeWithInvalidCategory() throws Exception {

        cache.get(TypeCategory.PRIMITIVE, DataTypes.BOOLEAN_TYPE.getName());
        fail("TypeCategory should only be one of TypeCategory.CLASS | ENUM | STRUCT | TRAIT in 'get'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testCacheHasTypeWithNullCategory() throws Exception {

        cache.has(null, CLASSTYPE_CUSTOMER);
        fail("Null TypeCategory should be not allowed in 'has'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testCacheHasTypeWithInvalidCategory() throws Exception {

        cache.has(TypeCategory.PRIMITIVE, DataTypes.BOOLEAN_TYPE.getName());
        fail("TypeCategory should only be one of TypeCategory.CLASS | ENUM | STRUCT | TRAIT in 'has'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testCacheGetTypeNamesByNullCategory() throws Exception {

        cache.getTypeNames(null);
        fail("Null TypeCategory should be not allowed in 'getNames'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testCacheGetTypeNamesByInvalidCategory() throws Exception {

        cache.getTypeNames(TypeCategory.PRIMITIVE);
        fail("TypeCategory should only be one of TypeCategory.CLASS | ENUM | STRUCT | TRAIT in 'getNames'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testCacheBulkInsertWithNullType() throws Exception {

        List<IDataType> allTypes = new ArrayList<>();
        allTypes.add(null);

        // create a new cache instead of using the one setup for every method call
        cache = new DefaultTypeCache();
        cache.putAll(allTypes);

        fail("Null type should be not allowed in 'putAll'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testCacheBulkInsertWithInvalidType() throws Exception {

        List<IDataType> allTypes = new ArrayList<>();
        allTypes.add(DataTypes.BOOLEAN_TYPE);

        // create a new cache instead of using the one setup for every method call
        cache = new DefaultTypeCache();
        cache.putAll(allTypes);

        fail("type should only one of ClassType | EnumType | StructType | TraitType in 'putAll'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testCacheRemoveByNullCategory() throws Exception {

        cache.remove(null, CLASSTYPE_CUSTOMER);
        fail("Null type should be not allowed in 'remove'");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testCacheRemoveByInvalidCategory() throws Exception {

        cache.remove(TypeCategory.PRIMITIVE, DataTypes.BOOLEAN_TYPE.getName());
        fail("TypeCategory should only be one of TypeCategory.CLASS | ENUM | STRUCT | TRAIT in 'remove'");
    }
}
