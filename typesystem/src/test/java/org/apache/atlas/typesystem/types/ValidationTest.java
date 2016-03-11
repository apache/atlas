/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ValidationTest {
    @DataProvider(name = "attributeData")
    private Object[][] createAttributeData() {
        return new String[][]{{null, "type"}, {"", "type"}, {"name", null}, {"name", ""}};
    }

    @Test(dataProvider = "attributeData", expectedExceptions = {IllegalArgumentException.class})
    public void testAttributes(String name, String type) {
        TypesUtil.createRequiredAttrDef(name, type);
    }

    @DataProvider(name = "enumValueData")
    private Object[][] createEnumValueData() {
        return new String[][]{{null}, {""}};
    }

    @Test(dataProvider = "enumValueData", expectedExceptions = {IllegalArgumentException.class})
    public void testEnumValue(String name) {
        new EnumValue(name, 1);
    }

    @DataProvider(name = "enumTypeData")
    private Object[][] createEnumTypeData() {
        EnumValue value = new EnumValue("name", 1);
        return new Object[][]{{null, value}, {"", value}, {"name"}};
    }

    @Test(dataProvider = "enumTypeData", expectedExceptions = {IllegalArgumentException.class})
    public void testEnumType(String name, EnumValue... values) {
        new EnumTypeDefinition(name, values);
    }

    @DataProvider(name = "structTypeData")
    private Object[][] createStructTypeData() {
        AttributeDefinition value = TypesUtil.createRequiredAttrDef("name", "type");
        return new Object[][]{{null, value}, {"", value}, {"name"}};
    }

    @Test(dataProvider = "structTypeData", expectedExceptions = {IllegalArgumentException.class})
    public void testStructType(String name, AttributeDefinition... values) {
        new StructTypeDefinition(name, values);
    }

    @DataProvider(name = "classTypeData")
    private Object[][] createClassTypeData() {
        return new Object[][]{{null}, {""}};
    }

    @Test(dataProvider = "classTypeData", expectedExceptions = {IllegalArgumentException.class})
    public void testClassType(String name) {
        AttributeDefinition value = TypesUtil.createRequiredAttrDef("name", "type");
        ;
        TypesUtil.createClassTypeDef(name, ImmutableSet.of("super"), value);
    }

    @Test(dataProvider = "classTypeData", expectedExceptions = {IllegalArgumentException.class})
    public void testTraitType(String name) {
        AttributeDefinition value = TypesUtil.createRequiredAttrDef("name", "type");
        ;
        TypesUtil.createTraitTypeDef(name, ImmutableSet.of("super"), value);
    }

    @Test
    public void testValidTypes() {
        AttributeDefinition attribute = TypesUtil.createRequiredAttrDef("name", "type");

        //class with no attributes
        TypesUtil.createClassTypeDef("name", ImmutableSet.of("super"));

        //class with no super types
        TypesUtil.createClassTypeDef("name", ImmutableSet.<String>of(), attribute);

        //trait with no attributes
        TypesUtil.createTraitTypeDef("name", ImmutableSet.of("super"));

        //trait with no super types
        TypesUtil.createTraitTypeDef("name", ImmutableSet.<String>of(), attribute);
    }
}
