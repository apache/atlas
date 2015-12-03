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
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createTraitTypeDef;

public class TraitTest extends HierarchicalTypeTest<TraitType> {


    @BeforeMethod
    public void setup() throws Exception {
        super.setup();
    }

    /*
     * Type Hierarchy is:
     *   A(a,b,c,d)
     *   B(b) extends A
     *   C(c) extends A
     *   D(d) extends B,C
     *
     * - There are a total of 11 fields in an instance of D
     * - an attribute that is hidden by a SubType can referenced by prefixing it with the
     * complete Path.
     *   For e.g. the 'b' attribute in A (that is a superType for B) is hidden the 'b' attribute
     *   in B.
     *   So it is available by the name 'A.B.D.b'
     *
     * - Another way to set attributes is to cast. Casting a 'D' instance of 'B' makes the 'A.B.D
     * .b' attribute
     *   available as 'A.B.b'. Casting one more time to an 'A' makes the 'A.B.b' attribute
     *   available as 'b'.
     */
    @Test
    public void test1() throws AtlasException {
        HierarchicalTypeDefinition A = createTraitTypeDef("A", null, createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE), createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE));
        HierarchicalTypeDefinition B = createTraitTypeDef("B", ImmutableList.<String>of("A"),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));
        HierarchicalTypeDefinition C =
                createTraitTypeDef("C", ImmutableList.<String>of("A"), createOptionalAttrDef("c", DataTypes.BYTE_TYPE));
        HierarchicalTypeDefinition D = createTraitTypeDef("D", ImmutableList.<String>of("B", "C"),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE));

        defineTraits(A, B, C, D);

        TraitType DType = (TraitType) getTypeSystem().getDataType(TraitType.class, "D");

        //        for(String aName : DType.fieldMapping().fields.keySet()) {
        //            System.out.println(String.format("nameToQualifiedName.put(\"%s\", \"%s\");", aName, DType
        // .getQualifiedName(aName)));
        //        }

        Map<String, String> nameToQualifiedName = new HashMap();
        {
            nameToQualifiedName.put("d", "D.d");
            nameToQualifiedName.put("b", "B.b");
            nameToQualifiedName.put("c", "C.c");
            nameToQualifiedName.put("a", "A.a");
            nameToQualifiedName.put("A.B.D.b", "A.B.D.b");
            nameToQualifiedName.put("A.B.D.c", "A.B.D.c");
            nameToQualifiedName.put("A.B.D.d", "A.B.D.d");
            nameToQualifiedName.put("A.C.D.a", "A.C.D.a");
            nameToQualifiedName.put("A.C.D.b", "A.C.D.b");
            nameToQualifiedName.put("A.C.D.c", "A.C.D.c");
            nameToQualifiedName.put("A.C.D.d", "A.C.D.d");
        }

        Struct s1 = new Struct("D");
        s1.set("d", 1);
        s1.set("c", 1);
        s1.set("b", true);
        s1.set("a", 1);
        s1.set("A.B.D.b", true);
        s1.set("A.B.D.c", 2);
        s1.set("A.B.D.d", 2);

        s1.set("A.C.D.a", 3);
        s1.set("A.C.D.b", false);
        s1.set("A.C.D.c", 3);
        s1.set("A.C.D.d", 3);


        ITypedStruct ts = DType.convert(s1, Multiplicity.REQUIRED);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\td : \t1\n" +
                "\tb : \ttrue\n" +
                "\tc : \t1\n" +
                "\ta : \t1\n" +
                "\tA.B.D.b : \ttrue\n" +
                "\tA.B.D.c : \t2\n" +
                "\tA.B.D.d : \t2\n" +
                "\tA.C.D.a : \t3\n" +
                "\tA.C.D.b : \tfalse\n" +
                "\tA.C.D.c : \t3\n" +
                "\tA.C.D.d : \t3\n" +
                "}");

        /*
         * cast to B and set the 'b' attribute on A.
         */
        TraitType BType = (TraitType) getTypeSystem().getDataType(TraitType.class, "B");
        IStruct s2 = DType.castAs(ts, "B");
        s2.set("A.B.b", false);

        Assert.assertEquals(ts.toString(), "{\n" +
                "\td : \t1\n" +
                "\tb : \ttrue\n" +
                "\tc : \t1\n" +
                "\ta : \t1\n" +
                "\tA.B.D.b : \tfalse\n" +
                "\tA.B.D.c : \t2\n" +
                "\tA.B.D.d : \t2\n" +
                "\tA.C.D.a : \t3\n" +
                "\tA.C.D.b : \tfalse\n" +
                "\tA.C.D.c : \t3\n" +
                "\tA.C.D.d : \t3\n" +
                "}");

        /*
         * cast again to A and set the 'b' attribute on A.
         */
        TraitType AType = (TraitType) getTypeSystem().getDataType(TraitType.class, "A");
        IStruct s3 = BType.castAs(s2, "A");
        s3.set("b", true);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\td : \t1\n" +
                "\tb : \ttrue\n" +
                "\tc : \t1\n" +
                "\ta : \t1\n" +
                "\tA.B.D.b : \ttrue\n" +
                "\tA.B.D.c : \t2\n" +
                "\tA.B.D.d : \t2\n" +
                "\tA.C.D.a : \t3\n" +
                "\tA.C.D.b : \tfalse\n" +
                "\tA.C.D.c : \t3\n" +
                "\tA.C.D.d : \t3\n" +
                "}");
    }

    @Test
    public void testRandomOrder() throws AtlasException {
        HierarchicalTypeDefinition A = createTraitTypeDef("A", null, createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE), createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE));
        HierarchicalTypeDefinition B = createTraitTypeDef("B", ImmutableList.<String>of("A"),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));
        HierarchicalTypeDefinition C = createTraitTypeDef("C", ImmutableList.<String>of("A"),
                createOptionalAttrDef("c", DataTypes.BYTE_TYPE));
        HierarchicalTypeDefinition D = createTraitTypeDef("D", ImmutableList.<String>of("B", "C"),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE));

        defineTraits(B, D, A, C);

        TraitType DType = (TraitType) getTypeSystem().getDataType(TraitType.class, "D");

        Struct s1 = new Struct("D");
        s1.set("d", 1);
        s1.set("c", 1);
        s1.set("b", true);
        s1.set("a", 1);
        s1.set("A.B.D.b", true);
        s1.set("A.B.D.c", 2);
        s1.set("A.B.D.d", 2);

        s1.set("A.C.D.a", 3);
        s1.set("A.C.D.b", false);
        s1.set("A.C.D.c", 3);
        s1.set("A.C.D.d", 3);


        ITypedStruct ts = DType.convert(s1, Multiplicity.REQUIRED);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\td : \t1\n" +
                "\tb : \ttrue\n" +
                "\tc : \t1\n" +
                "\ta : \t1\n" +
                "\tA.B.D.b : \ttrue\n" +
                "\tA.B.D.c : \t2\n" +
                "\tA.B.D.d : \t2\n" +
                "\tA.C.D.a : \t3\n" +
                "\tA.C.D.b : \tfalse\n" +
                "\tA.C.D.c : \t3\n" +
                "\tA.C.D.d : \t3\n" +
                "}");
    }

    @Override
    protected HierarchicalTypeDefinition<TraitType> getTypeDefinition(String name, AttributeDefinition... attributes) {
        return new HierarchicalTypeDefinition(TraitType.class, name, null, attributes);
    }

    @Override
    protected HierarchicalTypeDefinition<TraitType> getTypeDefinition(String name, ImmutableList<String> superTypes,
                                                                      AttributeDefinition... attributes) {
        return new HierarchicalTypeDefinition(TraitType.class, name, superTypes, attributes);
    }

    @Override
    protected TypesDef getTypesDef(StructTypeDefinition typeDefinition) {
        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of((HierarchicalTypeDefinition<TraitType>) typeDefinition),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
    }

    @Override
    protected TypesDef getTypesDef(HierarchicalTypeDefinition<TraitType>... typeDefinitions) {
        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.copyOf(typeDefinitions), ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
    }
}

