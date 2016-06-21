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

package org.apache.atlas.typesystem.types;

import com.google.common.collect.ImmutableSet;

import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public abstract class HierarchicalTypeTest<T extends HierarchicalType> extends TypeUpdateBaseTest {
    @Test
    public void testTypeUpdate() throws Exception {
        testTypeUpdateForAttributes();

        //Test super types
        HierarchicalTypeDefinition baseSuperType =
                getTypeDefinition(newName(), TypesUtil.createOptionalAttrDef("s", DataTypes.INT_TYPE));

        HierarchicalTypeDefinition classType = getTypeDefinition(newName(), ImmutableSet.of(baseSuperType.typeName),
                        TypesUtil.createRequiredAttrDef("a", DataTypes.INT_TYPE));
        TypeSystem ts = getTypeSystem();
        ts.defineTypes(getTypesDef(baseSuperType, classType));

        //Add super type with optional attribute
        HierarchicalTypeDefinition superType =
                getTypeDefinition(newName(), TypesUtil.createOptionalAttrDef("s", DataTypes.INT_TYPE));
        classType = getTypeDefinition(classType.typeName, ImmutableSet.of(superType.typeName, baseSuperType.typeName),
                TypesUtil.createRequiredAttrDef("a", DataTypes.INT_TYPE));
        try {
            ts.updateTypes(getTypesDef(superType, classType));
            Assert.fail("Expected TypeUpdateException");
        } catch (TypeUpdateException e) {
            //expected
        }

        //Add super type with required attribute should fail
        HierarchicalTypeDefinition superTypeRequired =
                getTypeDefinition(newName(), TypesUtil.createRequiredAttrDef("s", DataTypes.INT_TYPE));
        classType = getTypeDefinition(classType.typeName,
                ImmutableSet.of(superTypeRequired.typeName, baseSuperType.typeName),
                TypesUtil.createRequiredAttrDef("a", DataTypes.INT_TYPE));
        try {
            ts.updateTypes(getTypesDef(superTypeRequired, classType));
            Assert.fail("Expected TypeUpdateException");
        } catch (TypeUpdateException e) {
            //expected
        }

        //Deleting super type should fail
        classType = getTypeDefinition(classType.typeName, TypesUtil.createRequiredAttrDef("a", DataTypes.INT_TYPE));
        try {
            ts.updateTypes(getTypesDef(classType));
            Assert.fail("Expected TypeUpdateException");
        } catch (TypeUpdateException e) {
            //expected
        }
    }

    @Override
    protected abstract HierarchicalTypeDefinition<T> getTypeDefinition(String name, AttributeDefinition... attributes);

    protected abstract HierarchicalTypeDefinition<T> getTypeDefinition(String name, ImmutableSet<String> superTypes,
                                                                       AttributeDefinition... attributes);

    @Override
    protected abstract TypesDef getTypesDef(StructTypeDefinition typeDefinition);

    protected abstract TypesDef getTypesDef(HierarchicalTypeDefinition<T>... typeDefinitions);

    @Override
    protected int getNumberOfFields(TypeSystem ts, String typeName) throws Exception {
        return ts.getDataType(HierarchicalType.class, typeName).numFields;
    }
}