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

package org.apache.hadoop.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.metadata.json.Serialization$;
import org.apache.hadoop.metadata.types.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public class EnumTest extends BaseTest {

    @Before
    public void setup() throws MetadataException {
        super.setup();
    }

    void defineEnums(TypeSystem ts) throws MetadataException {
        ts.defineEnumType("HiveObjectType",
                new EnumValue("GLOBAL", 1),
                new EnumValue("DATABASE", 2),
                new EnumValue("TABLE", 3),
                new EnumValue("PARTITION", 4),
                new EnumValue("COLUMN", 5));

        ts.defineEnumType("PrincipalType",
                new EnumValue("USER", 1),
                new EnumValue("ROLE", 2),
                new EnumValue("GROUP", 3));

        ts.defineEnumType("TxnState",
                new EnumValue("COMMITTED", 1),
                new EnumValue("ABORTED", 2),
                new EnumValue("OPEN", 3));

        ts.defineEnumType("LockLevel",
                new EnumValue("DB", 1),
                new EnumValue("TABLE", 2),
                new EnumValue("PARTITION", 3));

    }

    protected void fillStruct(Struct s) throws MetadataException {
        s.set("a", 1);
        s.set("b", true);
        s.set("c", (byte) 1);
        s.set("d", (short) 2);
        s.set("e", 1);
        s.set("f", 1);
        s.set("g", 1L);
        s.set("h", 1.0f);
        s.set("i", 1.0);
        s.set("j", BigInteger.valueOf(1L));
        s.set("k", new BigDecimal(1));
        s.set("l", new Date(1418265358440L));
        s.set("m", Lists.<Integer>asList(Integer.valueOf(1), new Integer[]{Integer.valueOf(1)}));
        s.set("n", Lists.<BigDecimal>asList(BigDecimal.valueOf(1.1), new BigDecimal[]{BigDecimal.valueOf(1.1)}));
        Map<String, Double> hm = Maps.<String, Double>newHashMap();
        hm.put("a", 1.0);
        hm.put("b", 2.0);
        s.set("o", hm);
        s.set("enum1", "GLOBAL");
        s.set("enum2", 1);
        s.set("enum3", "COMMITTED");
        s.set("enum4", 3);
    }

    protected Struct createStructWithEnum(String typeName) throws MetadataException {
        Struct s = new Struct(typeName);
        fillStruct(s);
        return s;
    }

    protected Referenceable createInstanceWithEnum(String typeName) throws MetadataException {
        Referenceable r = new Referenceable(typeName);
        fillStruct(r);
        return r;
    }

    protected ClassType defineClassTypeWithEnum(TypeSystem ts) throws MetadataException {
        return ts.defineClassType(createClassTypeDef("t4",
                ImmutableList.<String>of(),
                createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE),
                createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE),
                createOptionalAttrDef("enum1", ts.getDataType(EnumType.class, "HiveObjectType")),
                createOptionalAttrDef("e", DataTypes.INT_TYPE),
                createOptionalAttrDef("f", DataTypes.INT_TYPE),
                createOptionalAttrDef("g", DataTypes.LONG_TYPE),
                createOptionalAttrDef("enum2", ts.getDataType(EnumType.class, "PrincipalType")),
                createOptionalAttrDef("h", DataTypes.FLOAT_TYPE),
                createOptionalAttrDef("i", DataTypes.DOUBLE_TYPE),
                createOptionalAttrDef("j", DataTypes.BIGINTEGER_TYPE),
                createOptionalAttrDef("k", DataTypes.BIGDECIMAL_TYPE),
                createOptionalAttrDef("enum3", ts.getDataType(EnumType.class, "TxnState")),
                createOptionalAttrDef("l", DataTypes.DATE_TYPE),
                createOptionalAttrDef("m", ts.defineArrayType(DataTypes.INT_TYPE)),
                createOptionalAttrDef("n", ts.defineArrayType(DataTypes.BIGDECIMAL_TYPE)),
                createOptionalAttrDef("o", ts.defineMapType(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)),
                createOptionalAttrDef("enum4", ts.getDataType(EnumType.class, "LockLevel"))));
    }

    @Test
    public void testStruct() throws MetadataException {
        TypeSystem ts = getTypeSystem();
        defineEnums(ts);
        StructType structType = ts.defineStructType("t3",
                true,
                createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE),
                createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE),
                createOptionalAttrDef("enum1", ts.getDataType(EnumType.class, "HiveObjectType")),
                createOptionalAttrDef("e", DataTypes.INT_TYPE),
                createOptionalAttrDef("f", DataTypes.INT_TYPE),
                createOptionalAttrDef("g", DataTypes.LONG_TYPE),
                createOptionalAttrDef("enum2", ts.getDataType(EnumType.class, "PrincipalType")),
                createOptionalAttrDef("h", DataTypes.FLOAT_TYPE),
                createOptionalAttrDef("i", DataTypes.DOUBLE_TYPE),
                createOptionalAttrDef("j", DataTypes.BIGINTEGER_TYPE),
                createOptionalAttrDef("k", DataTypes.BIGDECIMAL_TYPE),
                createOptionalAttrDef("enum3", ts.getDataType(EnumType.class, "TxnState")),
                createOptionalAttrDef("l", DataTypes.DATE_TYPE),
                createOptionalAttrDef("m", ts.defineArrayType(DataTypes.INT_TYPE)),
                createOptionalAttrDef("n", ts.defineArrayType(DataTypes.BIGDECIMAL_TYPE)),
                createOptionalAttrDef("o", ts.defineMapType(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)),
                createOptionalAttrDef("enum4", ts.getDataType(EnumType.class, "LockLevel")));

        Struct s = createStructWithEnum("t3");
        ITypedStruct typedS = structType.convert(s, Multiplicity.REQUIRED);
        Assert.assertEquals(typedS.toString(), "{\n" +
                "\ta : \t1\n" +
                "\tb : \ttrue\n" +
                "\tc : \t1\n" +
                "\td : \t2\n" +
                "\tenum1 : \tGLOBAL\n" +
                "\te : \t1\n" +
                "\tf : \t1\n" +
                "\tg : \t1\n" +
                "\tenum2 : \tUSER\n" +
                "\th : \t1.0\n" +
                "\ti : \t1.0\n" +
                "\tj : \t1\n" +
                "\tk : \t1\n" +
                "\tenum3 : \tCOMMITTED\n" +
                "\tl : \t2014-12-10\n" +
                "\tm : \t[1, 1]\n" +
                "\tn : \t[1.1, 1.1]\n" +
                "\to : \t{b=2.0, a=1.0}\n" +
                "\tenum4 : \tPARTITION\n" +
                "}");
    }

    @Test
    public void testClass() throws MetadataException {
        TypeSystem ts = getTypeSystem();
        defineEnums(ts);
        ClassType clsType = defineClassTypeWithEnum(ts);

        IReferenceableInstance r = createInstanceWithEnum("t4");
        ITypedReferenceableInstance typedR = clsType.convert(r, Multiplicity.REQUIRED);
        Assert.assertEquals(typedR.toString(), "{\n" +
                "\tid : (type: t4, id: <unassigned>)\n" +
                "\ta : \t1\n" +
                "\tb : \ttrue\n" +
                "\tc : \t1\n" +
                "\td : \t2\n" +
                "\tenum1 : \tGLOBAL\n" +
                "\te : \t1\n" +
                "\tf : \t1\n" +
                "\tg : \t1\n" +
                "\tenum2 : \tUSER\n" +
                "\th : \t1.0\n" +
                "\ti : \t1.0\n" +
                "\tj : \t1\n" +
                "\tk : \t1\n" +
                "\tenum3 : \tCOMMITTED\n" +
                "\tl : \t2014-12-10\n" +
                "\tm : \t[1, 1]\n" +
                "\tn : \t[1.1, 1.1]\n" +
                "\to : \t{b=2.0, a=1.0}\n" +
                "\tenum4 : \tPARTITION\n" +
                "}");
    }

    @Test
    public void testStorage() throws MetadataException {

        TypeSystem ts = getTypeSystem();
        defineEnums(ts);
        ClassType clsType = defineClassTypeWithEnum(ts);

        getRepository().defineTypes(ImmutableList.of((HierarchicalType)clsType));

        IReferenceableInstance r = createInstanceWithEnum("t4");
        IReferenceableInstance r1 = getRepository().create(r);

        ITypedReferenceableInstance r2 = getRepository().get(r1.getId());
        Assert.assertEquals(r2.toString(), "{\n" +
                "\tid : (type: t4, id: 1)\n" +
                "\ta : \t1\n" +
                "\tb : \ttrue\n" +
                "\tc : \t1\n" +
                "\td : \t0\n" +
                "\tenum1 : \tGLOBAL\n" +
                "\te : \t1\n" +
                "\tf : \t1\n" +
                "\tg : \t1\n" +
                "\tenum2 : \tUSER\n" +
                "\th : \t1.0\n" +
                "\ti : \t1.0\n" +
                "\tj : \t1\n" +
                "\tk : \t1\n" +
                "\tenum3 : \tCOMMITTED\n" +
                "\tl : \t2014-12-10\n" +
                "\tm : \t[1, 1]\n" +
                "\tn : \t[1.1, 1.1]\n" +
                "\to : \t{b=2.0, a=1.0}\n" +
                "\tenum4 : \tPARTITION\n" +
                "}");
    }

    @Test
    public void testJson() throws MetadataException {

        TypeSystem ts = getTypeSystem();
        defineEnums(ts);
        ClassType clsType = defineClassTypeWithEnum(ts);

        getRepository().defineTypes(ImmutableList.of((HierarchicalType)clsType));

        IReferenceableInstance r = createInstanceWithEnum("t4");
        IReferenceableInstance r1 = getRepository().create(r);

        ITypedReferenceableInstance r2 = getRepository().get(r1.getId());
        String jsonStr = Serialization$.MODULE$.toJson(r2);

        IReferenceableInstance r3 = Serialization$.MODULE$.fromJson(jsonStr);
        Assert.assertEquals(r3.toString(), "{\n" +
                "\tid : (type: t4, id: 1)\n" +
                "\ta : \t1\n" +
                "\tb : \ttrue\n" +
                "\tc : \t1\n" +
                "\td : \t0\n" +
                "\tenum1 : \tGLOBAL\n" +
                "\te : \t1\n" +
                "\tf : \t1\n" +
                "\tg : \t1\n" +
                "\tenum2 : \tUSER\n" +
                "\th : \t1.0\n" +
                "\ti : \t1.0\n" +
                "\tj : \t1\n" +
                "\tk : \t1\n" +
                "\tenum3 : \tCOMMITTED\n" +
                "\tl : \t2014-12-11\n" +
                "\tm : \t[1, 1]\n" +
                "\tn : \t[1.100000000000000088817841970012523233890533447265625, 1" +
                ".100000000000000088817841970012523233890533447265625]\n" +
                "\to : \t{b=2.0, a=1.0}\n" +
                "\tenum4 : \tPARTITION\n" +
                "}");
    }
}
