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

import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;

public abstract class TypeUpdateBaseTest extends BaseTest {
    protected void testTypeUpdateForAttributes() throws Exception {
        StructTypeDefinition typeDefinition =
                getTypeDefinition(newName(), TypesUtil.createRequiredAttrDef("a", DataTypes.INT_TYPE));
        TypeSystem ts = getTypeSystem();
        TypesDef typesDef = getTypesDef(typeDefinition);
        ts.defineTypes(typesDef);
        String typeName = typeDefinition.typeName;

        //Allow modifying required to optional attribute
        typeDefinition = getTypeDefinition(typeName, TypesUtil.createOptionalAttrDef("a", DataTypes.INT_TYPE));
        ts.updateTypes(getTypesDef(typeDefinition));

        //Allow adding new optional attribute
        typeDefinition = getTypeDefinition(typeName, TypesUtil.createOptionalAttrDef("a", DataTypes.INT_TYPE),
                TypesUtil.createOptionalAttrDef("b", DataTypes.INT_TYPE));
        ts.updateTypes(getTypesDef(typeDefinition));

        //Don't allow adding required attribute
        typeDefinition = getTypeDefinition(typeName, TypesUtil.createOptionalAttrDef("a", DataTypes.INT_TYPE),
                TypesUtil.createOptionalAttrDef("b", DataTypes.INT_TYPE),
                TypesUtil.createRequiredAttrDef("c", DataTypes.INT_TYPE));
        try {
            ts.updateTypes(getTypesDef(typeDefinition));
            Assert.fail("Expected TypeUpdateException");
        } catch (TypeUpdateException e) {
            //assert that type is not updated when validation fails
            Assert.assertEquals(getNumberOfFields(ts, typeDefinition.typeName), 2);
        }

        //Don't allow removing attribute
        typeDefinition = getTypeDefinition(typeName, TypesUtil.createOptionalAttrDef("a", DataTypes.INT_TYPE));
        try {
            ts.updateTypes(getTypesDef(typeDefinition));
        } catch (TypeUpdateException e) {
            //expected
        }

        //Don't allow modifying other fields of attribute definition - optional to required
        typeDefinition = getTypeDefinition(typeName, TypesUtil.createOptionalAttrDef("a", DataTypes.INT_TYPE),
                TypesUtil.createRequiredAttrDef("b", DataTypes.INT_TYPE));
        try {
            ts.updateTypes(getTypesDef(typeDefinition));
        } catch (TypeUpdateException e) {
            //expected
        }

        //Don't allow modifying other fields of attribute definition - attribute type change
        typeDefinition = getTypeDefinition(typeName, TypesUtil.createOptionalAttrDef("a", DataTypes.INT_TYPE),
                TypesUtil.createOptionalAttrDef("b", DataTypes.STRING_TYPE));
        try {
            ts.updateTypes(getTypesDef(typeDefinition));
        } catch (TypeUpdateException e) {
            //expected
        }

        //Don't allow modifying other fields of attribute definition - attribute type change
        typeDefinition = getTypeDefinition(typeName, TypesUtil.createRequiredAttrDef("a", DataTypes.INT_TYPE),
                new AttributeDefinition("b", DataTypes.arrayTypeName(DataTypes.STRING_TYPE.getName()),
                        Multiplicity.COLLECTION, false, null));
        try {
            ts.updateTypes(getTypesDef(typeDefinition));
        } catch (TypeUpdateException e) {
            //expected
        }
    }

    protected abstract int getNumberOfFields(TypeSystem ts, String typeName) throws Exception;

    protected abstract TypesDef getTypesDef(StructTypeDefinition typeDefinition);

    protected abstract StructTypeDefinition getTypeDefinition(String typeName,
                                                              AttributeDefinition... attributeDefinitions);
}
