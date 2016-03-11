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

import com.google.common.collect.ImmutableSet;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Struct;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createTraitTypeDef;

/**
 * Unit tests for type inheritance.
 */
public class TypeInheritanceTest extends BaseTest {

    @BeforeMethod
    public void setup() throws Exception {
        TypeSystem.getInstance().reset();
        super.setup();
    }

    /*
     * Type Hierarchy is:
     *   A(a)
     *   B(b) extends A
     */
    @Test
    public void testSimpleInheritance() throws AtlasException {
        HierarchicalTypeDefinition A = createClassTypeDef("A", null, createRequiredAttrDef("a", DataTypes.INT_TYPE));

        HierarchicalTypeDefinition B =
                createClassTypeDef("B", ImmutableSet.of("A"), createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));

        defineClasses(A, B);

        ClassType BType = getTypeSystem().getDataType(ClassType.class, "B");

        Struct s1 = new Struct("B");
        s1.set("b", true);
        s1.set("a", 1);

        ITypedInstance ts = BType.convert(s1, Multiplicity.REQUIRED);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\tid : (type: B, id: <unassigned>)\n" +
                "\tb : \ttrue\n" +
                "\ta : \t1\n" +
                "}");
    }

    /*
     * Type Hierarchy is:
     *   A(a, b)
     *   B(b) extends A
     */
    @Test
    public void testSimpleInheritanceWithOverrides() throws AtlasException {
        HierarchicalTypeDefinition A = createClassTypeDef("A", null, createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createRequiredAttrDef("b", DataTypes.BOOLEAN_TYPE));

        HierarchicalTypeDefinition B =
                createClassTypeDef("B", ImmutableSet.of("A"), createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));

        defineClasses(A, B);

        ClassType BType = getTypeSystem().getDataType(ClassType.class, "B");

        Struct s1 = new Struct("B");
        s1.set("b", true);
        s1.set("a", 1);
        s1.set("A.B.b", false);

        ITypedInstance ts = BType.convert(s1, Multiplicity.REQUIRED);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\tid : (type: B, id: <unassigned>)\n" +
                "\tb : \ttrue\n" +
                "\ta : \t1\n" +
                "\tA.B.b : \tfalse\n" +
                "}");
    }

    /*
     * Type Hierarchy is:
     *   A(a)
     *   B(b) extends A
     *   C(c) extends B
     *   D(d) extends C
     */
    @Test
    public void testMultiLevelInheritance() throws AtlasException {
        HierarchicalTypeDefinition A = createClassTypeDef("A", null, createRequiredAttrDef("a", DataTypes.INT_TYPE));

        HierarchicalTypeDefinition B =
                createClassTypeDef("B", ImmutableSet.of("A"), createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));

        HierarchicalTypeDefinition C =
                createClassTypeDef("C", ImmutableSet.of("B"), createOptionalAttrDef("c", DataTypes.BYTE_TYPE));

        HierarchicalTypeDefinition D =
                createClassTypeDef("D", ImmutableSet.of("C"), createOptionalAttrDef("d", DataTypes.SHORT_TYPE));

        defineClasses(A, B, C, D);

        ClassType DType = getTypeSystem().getDataType(ClassType.class, "D");

        Struct s1 = new Struct("D");
        s1.set("d", 1);
        s1.set("c", 1);
        s1.set("b", true);
        s1.set("a", 1);

        ITypedInstance ts = DType.convert(s1, Multiplicity.REQUIRED);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\tid : (type: D, id: <unassigned>)\n" +
                "\td : \t1\n" +
                "\tc : \t1\n" +
                "\tb : \ttrue\n" +
                "\ta : \t1\n" +
                "}");
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
     *   So it is availabel by the name 'A.B.D.b'
     *
     * - Another way to set attributes is to cast. Casting a 'D' instance of 'B' makes the 'A.B.D
     * .b' attribute
     *   available as 'A.B.b'. Casting one more time to an 'A' makes the 'A.B.b' attribute
     *   available as 'b'.
     */
    @Test
    public void testDiamondInheritance() throws AtlasException {
        HierarchicalTypeDefinition A = createTraitTypeDef("A", null, createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE), createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE));
        HierarchicalTypeDefinition B =
                createTraitTypeDef("B", ImmutableSet.of("A"), createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));
        HierarchicalTypeDefinition C =
                createTraitTypeDef("C", ImmutableSet.of("A"), createOptionalAttrDef("c", DataTypes.BYTE_TYPE));
        HierarchicalTypeDefinition D =
                createTraitTypeDef("D", ImmutableSet.of("B", "C"), createOptionalAttrDef("d", DataTypes.SHORT_TYPE));

        defineTraits(A, B, C, D);

        TraitType DType = getTypeSystem().getDataType(TraitType.class, "D");

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
        TraitType BType = getTypeSystem().getDataType(TraitType.class, "B");
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
}
