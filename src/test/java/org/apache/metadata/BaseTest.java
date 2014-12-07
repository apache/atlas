package org.apache.metadata;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.metadata.storage.IRepository;
import org.apache.metadata.storage.memory.MemRepository;
import org.apache.metadata.types.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public abstract class BaseTest {

    protected MetadataService ms;

    public static final String STRUCT_TYPE_1 = "t1";
    public static final String STRUCT_TYPE_2 = "t2";

    @BeforeClass
    public static void setupClass() throws MetadataException {
        TypeSystem ts = new TypeSystem();
        MemRepository mr = new MemRepository();
        MetadataService.setCurrentService(new MetadataService(mr, ts));

        StructType structType = ts.defineStructType(STRUCT_TYPE_1,
                true,
                createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE),
                createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE),
                createOptionalAttrDef("e", DataTypes.INT_TYPE),
                createOptionalAttrDef("f", DataTypes.INT_TYPE),
                createOptionalAttrDef("g", DataTypes.LONG_TYPE),
                createOptionalAttrDef("h", DataTypes.FLOAT_TYPE),
                createOptionalAttrDef("i", DataTypes.DOUBLE_TYPE),
                createOptionalAttrDef("j", DataTypes.BIGINTEGER_TYPE),
                createOptionalAttrDef("k", DataTypes.BIGDECIMAL_TYPE),
                createOptionalAttrDef("l", DataTypes.DATE_TYPE),
                createOptionalAttrDef("m", ts.defineArrayType(DataTypes.INT_TYPE)),
                createOptionalAttrDef("n", ts.defineArrayType(DataTypes.BIGDECIMAL_TYPE)),
                createOptionalAttrDef("o", ts.defineMapType(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)));

        StructType recursiveStructType = ts.defineStructType(STRUCT_TYPE_2,
                true,
                createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("s", STRUCT_TYPE_2));

    }

    public static Struct createStruct(MetadataService ms) throws MetadataException {
        StructType structType = (StructType) ms.getTypeSystem().getDataType(STRUCT_TYPE_1);
        Struct s = new Struct(structType.getName());
        s.set("a", 1);
        s.set("b", true);
        s.set("c", (byte)1);
        s.set("d", (short)2);
        s.set("e", 1);
        s.set("f", 1);
        s.set("g", 1L);
        s.set("h", 1.0f);
        s.set("i", 1.0);
        s.set("j", BigInteger.valueOf(1L));
        s.set("k", new BigDecimal(1));
        s.set("l", new Date(System.currentTimeMillis()));
        s.set("m", Lists.<Integer>asList(Integer.valueOf(1), new Integer[]{Integer.valueOf(1)}));
        s.set("n", Lists.<BigDecimal>asList(BigDecimal.valueOf(1.1), new BigDecimal[] {BigDecimal.valueOf(1.1)}));
        Map<String, Double> hm = Maps.<String, Double>newHashMap();
        hm.put("a", 1.0);
        hm.put("b",2.0);
        s.set("o", hm);
        return s;
    }

    @Before
    public void setup() throws MetadataException {
        ms = MetadataService.getCurrentService();
    }

    public static AttributeDefinition createOptionalAttrDef(String name,
                                                            IDataType dataType
    ) {

        return new AttributeDefinition(name, dataType.getName(), Multiplicity.OPTIONAL, false, null);
    }

    public static AttributeDefinition createOptionalAttrDef(String name,
                                                     String dataType
    ) {
        return new AttributeDefinition(name, dataType, Multiplicity.OPTIONAL, false, null);
    }


    public static AttributeDefinition createRequiredAttrDef(String name,
                                                            IDataType dataType
    ) {

        return new AttributeDefinition(name, dataType.getName(), Multiplicity.REQUIRED, false, null);
    }

    public static AttributeDefinition createRequiredAttrDef(String name,
                                                     String dataType
    ) {

        return new AttributeDefinition(name, dataType, Multiplicity.REQUIRED, false, null);
    }

}
