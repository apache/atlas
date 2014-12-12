package org.apache.metadata;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.apache.metadata.types.*;
import org.junit.Before;
import org.junit.Test;

public class TraitTest extends BaseTest {


    @Before
    public void setup() throws MetadataException {
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
     * - an attribute that is hidden by a SubType can referenced by prefixing it with the complete Path.
     *   For e.g. the 'b' attribute in A (that is a superType for B) is hidden the 'b' attribute in B.
     *   So it is availabel by the name 'A.B.D.b'
     *
     * - Another way to set attributes is to cast. Casting a 'D' instance of 'B' makes the 'A.B.D.b' attribute
     *   available as 'A.B.b'. Casting one more time to an 'A' makes the 'A.B.b' attribute available as 'b'.
     */
    @Test
    public void test1() throws MetadataException {
        HierarchicalTypeDefinition A = createTraitTypeDef("A", null,
                createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE),
                createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE));
        HierarchicalTypeDefinition B = createTraitTypeDef("B", ImmutableList.<String>of("A"),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));
        HierarchicalTypeDefinition C = createTraitTypeDef("C", ImmutableList.<String>of("A"),
                createOptionalAttrDef("c", DataTypes.BYTE_TYPE));
        HierarchicalTypeDefinition D = createTraitTypeDef("D", ImmutableList.<String>of("B", "C"),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE));

        defineTraits(A, B, C, D);

        TraitType DType = (TraitType) ms.getTypeSystem().getDataType(TraitType.class, "D");

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
                "\td : 1\n" +
                "\tb : true\n" +
                "\tc : 1\n" +
                "\ta : 1\n" +
                "\tA.B.D.b : true\n" +
                "\tA.B.D.c : 2\n" +
                "\tA.B.D.d : 2\n" +
                "\tA.C.D.a : 3\n" +
                "\tA.C.D.b : false\n" +
                "\tA.C.D.c : 3\n" +
                "\tA.C.D.d : 3\n" +
                "\n" +
                "}\n");

        /*
         * cast to B and set the 'b' attribute on A.
         */
        TraitType BType = (TraitType) ms.getTypeSystem().getDataType(TraitType.class, "B");
        IStruct s2 = DType.castAs(ts, "B");
        s2.set("A.B.b", false);

        Assert.assertEquals(ts.toString(), "{\n" +
                "\td : 1\n" +
                "\tb : true\n" +
                "\tc : 1\n" +
                "\ta : 1\n" +
                "\tA.B.D.b : false\n" +
                "\tA.B.D.c : 2\n" +
                "\tA.B.D.d : 2\n" +
                "\tA.C.D.a : 3\n" +
                "\tA.C.D.b : false\n" +
                "\tA.C.D.c : 3\n" +
                "\tA.C.D.d : 3\n" +
                "\n" +
                "}\n");

        /*
         * cast again to A and set the 'b' attribute on A.
         */
        TraitType AType = (TraitType) ms.getTypeSystem().getDataType(TraitType.class, "A");
        IStruct s3 = BType.castAs(s2, "A");
        s3.set("b", true);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\td : 1\n" +
                "\tb : true\n" +
                "\tc : 1\n" +
                "\ta : 1\n" +
                "\tA.B.D.b : true\n" +
                "\tA.B.D.c : 2\n" +
                "\tA.B.D.d : 2\n" +
                "\tA.C.D.a : 3\n" +
                "\tA.C.D.b : false\n" +
                "\tA.C.D.c : 3\n" +
                "\tA.C.D.d : 3\n" +
                "\n" +
                "}\n");
    }
}

