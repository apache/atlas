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

import java.util.HashSet;

import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


/**
 * Unit test for {@link FieldMapping}
 *
 */
public class FieldMappingTest {

    @BeforeTest
    public void beforeTest() throws Exception {
        TypeSystem typeSystem = TypeSystem.getInstance();
        typeSystem.reset();
    }

    @Test
    public void testOutputReferenceableInstance() throws Exception {
        // ATLAS-645: verify that FieldMapping.output(IReferenceableInstance)
        // does not infinitely recurse when ITypedReferenceableInstance's reference each other.
        HierarchicalTypeDefinition<ClassType> valueDef = TypesUtil.createClassTypeDef("Value",
            ImmutableSet.<String>of(),
            new AttributeDefinition("owner", "Owner", Multiplicity.OPTIONAL, false, null));

        // Define class type with reference, where the value is a class reference to Value.
        HierarchicalTypeDefinition<ClassType> ownerDef = TypesUtil.createClassTypeDef("Owner",
            ImmutableSet.<String>of(),
            new AttributeDefinition("value", "Value", Multiplicity.OPTIONAL, false, null));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
            ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
            ImmutableList.of(ownerDef, valueDef));

        TypeSystem typeSystem = TypeSystem.getInstance();
        typeSystem.defineTypes(typesDef);
        ClassType ownerType = typeSystem.getDataType(ClassType.class, "Owner");

        // Prior to fix for ATLAS-645, this call would throw a StackOverflowError
        try {
            ownerType.toString();
        }
        catch (StackOverflowError e) {
            Assert.fail("Infinite recursion in ClassType.toString() caused StackOverflowError");
        }

        ClassType valueType = typeSystem.getDataType(ClassType.class, "Value");

        // Create instances of Owner and Value that reference each other.
        ITypedReferenceableInstance ownerInstance = ownerType.createInstance();
        ITypedReferenceableInstance valueInstance = valueType.createInstance();
        // Set Owner.value reference to Value instance.
        ownerInstance.set("value", valueInstance);
        // Set Value.owner reference on Owner instance.
        valueInstance.set("owner", ownerInstance);

        // Prior to fix for ATLAS-645, this call would throw a StackOverflowError
        try {
            ownerInstance.fieldMapping().output(ownerInstance, new StringBuilder(), "", new HashSet<IReferenceableInstance>());
        }
        catch (StackOverflowError e) {
            Assert.fail("Infinite recursion in FieldMapping.output() caused StackOverflowError");
        }
    }

    @Test
    public void testOutputStruct() throws Exception {
        // ATLAS-645: verify that FieldMapping.output(IStruct) does not infinitely recurse
        // when an IStruct and ITypedReferenceableInstance reference each other.
        HierarchicalTypeDefinition<ClassType> valueDef = TypesUtil.createClassTypeDef("Value",
            ImmutableSet.<String>of(),
            new AttributeDefinition("owner", "Owner", Multiplicity.OPTIONAL, false, null));


        // Define struct type with reference, where the value is a class reference to Value.
        StructTypeDefinition ownerDef = TypesUtil.createStructTypeDef("Owner",
             new AttributeDefinition("value", "Value", Multiplicity.OPTIONAL, false, null));

        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
            ImmutableList.of(ownerDef), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
            ImmutableList.of(valueDef));

        TypeSystem typeSystem = TypeSystem.getInstance();
        typeSystem.reset();
        typeSystem.defineTypes(typesDef);
        StructType ownerType = typeSystem.getDataType(StructType.class, "Owner");
        ClassType valueType = typeSystem.getDataType(ClassType.class, "Value");

        // Prior to fix for ATLAS-645, this call would throw a StackOverflowError
        try {
            ownerType.toString();
        }
        catch (StackOverflowError e) {
            Assert.fail("Infinite recursion in StructType.toString() caused StackOverflowError");
        }


        // Create instances of Owner and Value that reference each other.
        ITypedStruct ownerInstance = ownerType.createInstance();
        ITypedReferenceableInstance valueInstance = valueType.createInstance();
        // Set Owner.value reference to Value instance.
        ownerInstance.set("value", valueInstance);
        // Set Value.owner reference on Owner instance.
        valueInstance.set("owner", ownerInstance);

        // Prior to fix for ATLAS-645, this call would throw a StackOverflowError
        try {
            ownerInstance.fieldMapping().output(ownerInstance, new StringBuilder(), "", null);
        }
        catch (StackOverflowError e) {
            Assert.fail("Infinite recursion in FieldMapping.output() caused StackOverflowError");
        }

    }
}
