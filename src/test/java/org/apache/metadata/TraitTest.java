package org.apache.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.metadata.storage.StructInstance;
import org.apache.metadata.types.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

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
        TraitTypeDefinition A = createTraitTypeDef("A", null,
                createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE),
                createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE));
        TraitTypeDefinition B = createTraitTypeDef("B", ImmutableList.<String>of("A"),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));
        TraitTypeDefinition C = createTraitTypeDef("C", ImmutableList.<String>of("A"),
                createOptionalAttrDef("c", DataTypes.BYTE_TYPE));
        TraitTypeDefinition D = createTraitTypeDef("D", ImmutableList.<String>of("B", "C"),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE));

        defineTraits(A, B, C, D);

        TraitType DType = (TraitType) ms.getTypeSystem().getDataType("D");

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


        StructInstance ts = DType.convert(s1, Multiplicity.REQUIRED);
        System.out.println(ts);

        /*
         * cast to B and set the 'b' attribute on A.
         */
        TraitType BType = (TraitType) ms.getTypeSystem().getDataType("B");
        IStruct s2 = DType.castAs(ts, "B");
        s2.set("A.B.b", false);

        System.out.println(ts);

        /*
         * cast again to A and set the 'b' attribute on A.
         */
        TraitType AType = (TraitType) ms.getTypeSystem().getDataType("A");
        IStruct s3 = BType.castAs(s2, "A");
        s3.set("b", true);
        System.out.println(ts);
    }
}

