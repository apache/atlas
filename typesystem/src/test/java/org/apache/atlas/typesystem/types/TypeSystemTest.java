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
import com.google.common.collect.ImmutableSet;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.exception.TypeExistsException;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import scala.actors.threadpool.Arrays;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createStructTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createTraitTypeDef;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TypeSystemTest extends BaseTest {

    public static final long TEST_DATE_IN_LONG = 1418265358440L;
    public static final String TEST_DATE_STRING = "2014-12-11T02:35:58.440Z";

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
        assertTrue(getTypeSystem().getTypeNames().contains("enum_test"));
    }

    @Test
    public void testGetTypeDescription() throws Exception {
        String typeName = "enum_type";
        String description = "_description";
        String typeDescription = typeName + description;
        getTypeSystem().defineEnumType(typeName, typeDescription, new EnumValue("0", 0), new EnumValue("1", 1), new EnumValue("2", 2),
                new EnumValue("3", 3));
        assertTrue(getTypeSystem().getTypeNames().contains(typeName));
        IDataType type = getTypeSystem().getDataType(EnumType.class, typeName);
        Assert.assertNotNull(type);
        Assert.assertEquals(type.getDescription(), typeDescription);

        typeName = "trait_type";
        typeDescription = typeName + description;
        HierarchicalTypeDefinition<TraitType> trait = TypesUtil
            .createTraitTypeDef(typeName, typeDescription, ImmutableSet.<String>of(),
                TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE));
        getTypeSystem().defineTraitType(trait);
        assertTrue(getTypeSystem().getTypeNames().contains(typeName));
        type = getTypeSystem().getDataType(TraitType.class, typeName);
        Assert.assertNotNull(type);
        Assert.assertEquals(type.getDescription(), typeDescription);

        typeName = "class_type";
        typeDescription = typeName + description;
        HierarchicalTypeDefinition<ClassType> classType = TypesUtil
            .createClassTypeDef(typeName, typeDescription, ImmutableSet.<String>of(),
                TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE));
        getTypeSystem().defineClassType(classType);
        assertTrue(getTypeSystem().getTypeNames().contains(typeName));
        type = getTypeSystem().getDataType(ClassType.class, typeName);
        Assert.assertNotNull(type);
        Assert.assertEquals(type.getDescription(), typeDescription);

        typeName = "struct_type";
        typeDescription = typeName + description;
        getTypeSystem().defineStructType(typeName, typeDescription, true, createRequiredAttrDef("a", DataTypes.INT_TYPE));
        assertTrue(getTypeSystem().getTypeNames().contains(typeName));
        type = getTypeSystem().getDataType(StructType.class, typeName);
        Assert.assertNotNull(type);
        Assert.assertEquals(type.getDescription(), typeDescription);
        
    }

    @Test
    public void testIsRegistered() throws Exception {
        getTypeSystem().defineEnumType("enum_test", new EnumValue("0", 0), new EnumValue("1", 1), new EnumValue("2", 2),
                new EnumValue("3", 3));
        assertTrue(getTypeSystem().isRegistered("enum_test"));
    }

    @Test
    public void testGetTraitsNames() throws Exception {
        HierarchicalTypeDefinition<TraitType> classificationTraitDefinition = TypesUtil
                .createTraitTypeDef("Classification", ImmutableSet.<String>of(),
                        TypesUtil.createRequiredAttrDef("tag", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef("PII", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> phiTrait =
                TypesUtil.createTraitTypeDef("PHI", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> pciTrait =
                TypesUtil.createTraitTypeDef("PCI", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> soxTrait =
                TypesUtil.createTraitTypeDef("SOX", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> secTrait =
                TypesUtil.createTraitTypeDef("SEC", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> financeTrait =
                TypesUtil.createTraitTypeDef("Finance", ImmutableSet.<String>of());

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
        HierarchicalTypeDefinition<ClassType> classType = createClassTypeDef(className, ImmutableSet.<String>of(),
                createRequiredAttrDef(attrType, DataTypes.STRING_TYPE));

        String traitName = random();
        HierarchicalTypeDefinition<TraitType> traitType = createTraitTypeDef(traitName, ImmutableSet.<String>of(),
                createRequiredAttrDef(attrType, DataTypes.INT_TYPE));

        ts.defineTypes(ImmutableList.of(orgLevelEnum), ImmutableList.of(structType),
                ImmutableList.of(traitType), ImmutableList.of(classType));
    }

    @Test
    public void testTypeCategory() throws AtlasException {
        TypeSystem ts = getTypeSystem();
        ts.reset();

        StructTypeDefinition struct_A = createStructTypeDef("struct_A", createRequiredAttrDef("s_A", DataTypes.STRING_TYPE));
        StructTypeDefinition struct_B = createStructTypeDef("struct_B", createRequiredAttrDef("s_B", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<TraitType> trait_A = createTraitTypeDef("trait_A", null,
                createRequiredAttrDef("t_A", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> trait_B = createTraitTypeDef("trait_B", ImmutableSet.<String>of("trait_A"),
                createRequiredAttrDef("t_B", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> trait_C = createTraitTypeDef("trait_C", ImmutableSet.<String>of("trait_A"),
                createRequiredAttrDef("t_C", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> trait_D = createTraitTypeDef("trait_D", ImmutableSet.<String>of("trait_B", "trait_C"),
                createRequiredAttrDef("t_D", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> class_A = createClassTypeDef("class_A", null,
                createRequiredAttrDef("c_A", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> class_B = createClassTypeDef("class_B", ImmutableSet.<String>of("class_A"),
                createRequiredAttrDef("c_B", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> class_C = createClassTypeDef("class_C", ImmutableSet.<String>of("class_B"),
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

    @Test
    public void testTypeNamesAreNotDuplicated() throws Exception {
        TypeSystem typeSystem = getTypeSystem();
        ImmutableList<String> traitNames = typeSystem.getTypeNamesByCategory(DataTypes.TypeCategory.TRAIT);
        int numTraits = traitNames.size();

        HashMap<String, IDataType> typesAdded = new HashMap<>();
        String traitName = "dup_type_test" + random();
        TraitType traitType = new TraitType(typeSystem, traitName, null, null, 0);
        typesAdded.put(traitName, traitType);
        typeSystem.commitTypes(typesAdded);

        traitNames = typeSystem.getTypeNamesByCategory(DataTypes.TypeCategory.TRAIT);
        Assert.assertEquals(traitNames.size(), numTraits+1);

        // add again with another trait this time
        traitName = "dup_type_test" + random();
        TraitType traitTypeNew = new TraitType(typeSystem, traitName, null, null, 0);
        typesAdded.put(traitName, traitTypeNew);

        typeSystem.commitTypes(typesAdded);
        traitNames = typeSystem.getTypeNamesByCategory(DataTypes.TypeCategory.TRAIT);
        Assert.assertEquals(traitNames.size(), numTraits+2);
    }

    @Test
    public void testHierarchy() throws Exception {
        HierarchicalTypeDefinition<ClassType> testObjectDef = TypesUtil.createClassTypeDef("TestObject", ImmutableSet.<String>of(),
            createOptionalAttrDef("name", DataTypes.STRING_TYPE),
            createOptionalAttrDef("description", DataTypes.STRING_TYPE),
            createOptionalAttrDef("topAttribute", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> testDataSetDef = TypesUtil.createClassTypeDef("TestDataSet", ImmutableSet.of("TestObject"));
        HierarchicalTypeDefinition<ClassType> testColumnDef = TypesUtil.createClassTypeDef("TestColumn", ImmutableSet.of("TestObject"),
            createRequiredAttrDef("name", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> testRelationalDataSetDef = 
            TypesUtil.createClassTypeDef("TestRelationalDataSet", ImmutableSet.of("TestDataSet"),
                new AttributeDefinition("columns", DataTypes.arrayTypeName("TestColumn"),
                    Multiplicity.OPTIONAL, true, null));
        HierarchicalTypeDefinition<ClassType> testTableDef = TypesUtil.createClassTypeDef("TestTable", ImmutableSet.of("TestRelationalDataSet"),
            createOptionalAttrDef("schema", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> testDataFileDef = TypesUtil.createClassTypeDef("TestDataFile", ImmutableSet.of("TestRelationalDataSet"),
            createOptionalAttrDef("urlString", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> testDocumentDef = TypesUtil.createClassTypeDef("TestDocument", ImmutableSet.of("TestDataSet"),
            createOptionalAttrDef("urlString", DataTypes.STRING_TYPE),
            createOptionalAttrDef("encoding", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> testAnnotationDef =TypesUtil.createClassTypeDef("TestAnnotation",  ImmutableSet.<String>of(),
            createOptionalAttrDef("inheritedAttribute", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> myNewAnnotationDef = TypesUtil.createClassTypeDef("MyNewAnnotation", ImmutableSet.of("TestAnnotation"),
            createRequiredAttrDef("myNewAnnotationAttribute", DataTypes.STRING_TYPE));
        getTypeSystem().defineTypes(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
            ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
            ImmutableList.of(testObjectDef, testDataSetDef, testColumnDef, testRelationalDataSetDef, testTableDef, testDataFileDef, testDocumentDef, testAnnotationDef, myNewAnnotationDef));

        // Verify that field mappings for MyNewAnnotation contains the attribute inherited from the TestAnnotation superclass.
        // Prior to fix for ATLAS-573, the inherited attribute was missing.
        ClassType dataType = getTypeSystem().getDataType(ClassType.class, "MyNewAnnotation");
        Assert.assertTrue(dataType.fieldMapping.fields.containsKey("inheritedAttribute"));
    }

    @Test
    public void testDuplicateTypenames() throws Exception {
        TypeSystem typeSystem = getTypeSystem();
        HierarchicalTypeDefinition<TraitType> trait = TypesUtil
                .createTraitTypeDef(random(), "description", ImmutableSet.<String>of(),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE));
        typeSystem.defineTraitType(trait);

        try {
            typeSystem.defineTraitType(trait);
            fail("Expected TypeExistsException");
        } catch(TypeExistsException e) {
            //expected
        }
    }

    @Test(expectedExceptions = ValueConversionException.class)
    public void testConvertInvalidDate() throws Exception {
       DataTypes.DATE_TYPE.convert("", Multiplicity.OPTIONAL);
    }

    @Test()
    public void testConvertValidDate() throws Exception {
        Date date = DataTypes.DATE_TYPE.convert(TEST_DATE_STRING, Multiplicity.OPTIONAL);
        Assert.assertEquals(date, new Date(TEST_DATE_IN_LONG));


        StringBuilder buf = new StringBuilder();
        DataTypes.DATE_TYPE.output(new Date(TEST_DATE_IN_LONG), buf, "", new HashSet<Date>());
        Assert.assertEquals(buf.toString(), TEST_DATE_STRING);
    }
}
