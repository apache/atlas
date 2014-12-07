package org.apache.metadata;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.metadata.storage.TypedStruct;
import org.apache.metadata.types.*;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public class StructTest extends BaseTest {

    StructType structType;
    StructType recursiveStructType;

    @Before
    public void setup() throws MetadataException {
        super.setup();
        structType = (StructType) ms.getTypeSystem().getDataType(STRUCT_TYPE_1);
        recursiveStructType = (StructType) ms.getTypeSystem().getDataType(STRUCT_TYPE_2);
    }

    @Test
    public void test1() throws MetadataException {
        Struct s = createStruct(ms);
        TypedStruct  ts = structType.convert(s, Multiplicity.REQUIRED);
        System.out.println(ts);
    }

    @Test
    public void testRecursive() throws MetadataException {
        Struct s1 = new Struct(recursiveStructType.getName());
        s1.set("a", 1);
        Struct s2 = new Struct(recursiveStructType.getName());
        s2.set("a", 1);
        s2.set("s", s1);
        TypedStruct  ts = recursiveStructType.convert(s2, Multiplicity.REQUIRED);
        System.out.println(ts);
    }

}
