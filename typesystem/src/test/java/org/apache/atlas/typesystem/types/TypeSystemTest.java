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

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.actors.threadpool.Arrays;

import java.util.Collections;
import java.util.List;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createStructTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createTraitTypeDef;

public class TypeSystemTest extends BaseTest {

    @BeforeClass
    public void setUp() throws Exception {
        super.setup();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        getTypeSystem().reset();
    }

    @Test
    public void testGetTypeNames() throws Exception {
        getTypeSystem().defineEnumType("enum_test", new EnumValue("0", 0), new EnumValue("1", 1), new EnumValue("2", 2),
                new EnumValue("3", 3));
        Assert.assertTrue(getTypeSystem().getTypeNames().contains("enum_test"));
    }

    @Test
    public void testIsRegistered() throws Exception {
        getTypeSystem().defineEnumType("enum_test", new EnumValue("0", 0), new EnumValue("1", 1), new EnumValue("2", 2),
                new EnumValue("3", 3));
        Assert.assertTrue(getTypeSystem().isRegistered("enum_test"));
    }

    @Test
    public void testGetTraitsNames() throws Exception {
        HierarchicalTypeDefinition<TraitType> classificationTraitDefinition = TypesUtil
                .createTraitTypeDef("Classification", ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("tag", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef("PII", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> phiTrait =
                TypesUtil.createTraitTypeDef("PHI", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> pciTrait =
                TypesUtil.createTraitTypeDef("PCI", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> soxTrait =
                TypesUtil.createTraitTypeDef("SOX", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> secTrait =
                TypesUtil.createTraitTypeDef("SEC", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> financeTrait =
                TypesUtil.createTraitTypeDef("Finance", ImmutableList.<String>of());

        getTypeSystem().defineTypes(ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(classificationTraitDefinition, piiTrait, phiTrait, pciTrait, soxTrait, secTrait,
                        financeTrait), ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());

        final ImmutableList<String> traitsNames = getTypeSystem().getTypeNamesByCategory(DataTypes.TypeCategory.TRAIT);
        Assert.assertEquals(traitsNames.size(), 7);
        List traits = Arrays.asList(new String[]{"Classification", "PII", "PHI", "PCI", "SOX", "SEC", "Finance",});

        Assert.assertFalse(Collections.disjoint(traitsNames, traits));
    }

    private String random() {
        return RandomStringUtils.random(10);
    }

    @Test
    public void testUTFNames() throws Exception {
        TypeSystem ts = getTypeSystem();

        String enumType = random();
        EnumTypeDefinition orgLevelEnum =
                new EnumTypeDefinition(enumType, new EnumValue(random(), 1), new EnumValue(random(), 2));

        String structName = random();
        String attrType = random();
        StructTypeDefinition structType =
                createStructTypeDef(structName, createRequiredAttrDef(attrType, DataTypes.STRING_TYPE));

        String className = random();
        HierarchicalTypeDefinition<ClassType> classType = createClassTypeDef(className, ImmutableList.<String>of(),
                createRequiredAttrDef(attrType, DataTypes.STRING_TYPE));

        String traitName = random();
        HierarchicalTypeDefinition<TraitType> traitType = createTraitTypeDef(traitName, ImmutableList.<String>of(),
                createRequiredAttrDef(attrType, DataTypes.INT_TYPE));

        ts.defineTypes(ImmutableList.of(orgLevelEnum), ImmutableList.of(structType),
                ImmutableList.of(traitType), ImmutableList.of(classType));
    }

    @Test
    public void testHierarchy() throws AtlasException {
        HierarchicalTypeDefinition<ClassType> a = TypesUtil.createClassTypeDef("a", ImmutableList.<String>of());
        HierarchicalTypeDefinition<ClassType> b = TypesUtil.createClassTypeDef("B", ImmutableList.of("a"));
        HierarchicalTypeDefinition<ClassType> c = TypesUtil.createClassTypeDef("C", ImmutableList.of("B"));

        TypeSystem ts = getTypeSystem();
        ts.defineTypes(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(a, b, c));
        ClassType ac = ts.getDataType(ClassType.class, "a");
        ClassType bc = ts.getDataType(ClassType.class, "B");
        ClassType cc = ts.getDataType(ClassType.class, "C");

        Assert.assertTrue(ac.compareTo(bc) < 0);
        Assert.assertTrue(bc.compareTo(cc) < 0);
        Assert.assertTrue(ac.compareTo(cc) < 0);
    }

    @Test
    public void testTypeCategory() throws AtlasException {
        TypeSystem ts = getTypeSystem();
        ts.reset();

        StructTypeDefinition struct_A = createStructTypeDef("struct_A", createRequiredAttrDef("s_A", DataTypes.STRING_TYPE));
        StructTypeDefinition struct_B = createStructTypeDef("struct_B", createRequiredAttrDef("s_B", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<TraitType> trait_A = createTraitTypeDef("trait_A", null,
                createRequiredAttrDef("t_A", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> trait_B = createTraitTypeDef("trait_B", ImmutableList.<String>of("trait_A"),
                createRequiredAttrDef("t_B", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> trait_C = createTraitTypeDef("trait_C", ImmutableList.<String>of("trait_A"),
                createRequiredAttrDef("t_C", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> trait_D = createTraitTypeDef("trait_D", ImmutableList.<String>of("trait_B", "trait_C"),
                createRequiredAttrDef("t_D", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> class_A = createClassTypeDef("class_A", null,
                createRequiredAttrDef("c_A", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> class_B = createClassTypeDef("class_B", ImmutableList.<String>of("class_A"),
                createRequiredAttrDef("c_B", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> class_C = createClassTypeDef("class_C", ImmutableList.<String>of("class_B"),
                createRequiredAttrDef("c_C", DataTypes.STRING_TYPE));

        ts.defineTypes(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.of(struct_A, struct_B),
                ImmutableList.of(trait_A, trait_B, trait_C, trait_D),
                ImmutableList.of(class_A, class_B, class_C));

        final ImmutableList<String> structNames = ts.getTypeNamesByCategory(DataTypes.TypeCategory.STRUCT);
        final ImmutableList<String> traitNames = ts.getTypeNamesByCategory(DataTypes.TypeCategory.TRAIT);
        final ImmutableList<String> classNames = ts.getTypeNamesByCategory(DataTypes.TypeCategory.CLASS);

        Assert.assertEquals(structNames.size(), 2);
        Assert.assertEquals(traitNames.size(), 4);
        Assert.assertEquals(classNames.size(), 3);
    }
}
