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

package org.apache.atlas.typesystem.persistence;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.utils.SHA256Utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Date;
import java.util.HashSet;

/*
 * @todo handle names prefixed by traitName.
 */
public class ReferenceableInstance extends StructInstance implements ITypedReferenceableInstance {

    private final ImmutableMap<String, ITypedStruct> traits;
    private final ImmutableList<String> traitNames;
    private Id id;
    private AtlasSystemAttributes systemAttributes;

    private static final boolean[]                          EMPTY_BOOL_ARRAY    = new boolean[0];
    private static final ImmutableMap<String, ITypedStruct> EMPTY_TRAITS_MAP    = ImmutableMap.of();
    private static final FieldMapping                       EMPTY_FIELD_MAPPING = new FieldMapping(ImmutableMap.<String, AttributeInfo>of(), ImmutableMap.<String, Integer>of(), ImmutableMap.<String, Integer>of(), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    public ReferenceableInstance(String guid, String typeName) {
        this(new Id(guid, 0, typeName), typeName, null, EMPTY_FIELD_MAPPING, EMPTY_BOOL_ARRAY, EMPTY_BOOL_ARRAY,
             null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, EMPTY_TRAITS_MAP);
    }

    public ReferenceableInstance(Id id, String dataTypeName, AtlasSystemAttributes systemAttributes, FieldMapping fieldMapping, boolean[] nullFlags,
            boolean[] explicitSets, boolean[] bools, byte[] bytes, short[] shorts, int[] ints, long[] longs, float[] floats, double[] doubles,
            BigDecimal[] bigDecimals, BigInteger[] bigIntegers, Date[] dates, String[] strings,
            ImmutableList<Object>[] arrays, ImmutableMap<Object, Object>[] maps, StructInstance[] structs,
            ReferenceableInstance[] referenceableInstances, Id[] ids, ImmutableMap<String, ITypedStruct> traits) {
        super(dataTypeName, fieldMapping, nullFlags, explicitSets, bools, bytes, shorts, ints, longs, floats, doubles, bigDecimals,
                bigIntegers, dates, strings, arrays, maps, structs, referenceableInstances, ids);
        this.id = id;
        this.traits = traits;
        ImmutableList.Builder<String> b = new ImmutableList.Builder<>();
        for (String t : traits.keySet()) {
            b.add(t);
        }
        this.traitNames = b.build();
        if (systemAttributes == null){
            this.systemAttributes = new AtlasSystemAttributes();
        }
        else {
            this.systemAttributes = systemAttributes;
        }
    }

    @Override
    public ImmutableList<String> getTraits() {
        return traitNames;
    }

    @Override
    public Id getId() {
        return id;
    }

    @Override
    public IStruct getTrait(String typeName) {
        return traits.get(typeName);
    }

    @Override
    public AtlasSystemAttributes getSystemAttributes(){
        return systemAttributes;
    }

    /**
     * @nopub
     * @param id
     */
    public void replaceWithNewId(Id id) {
        this.id = id;
    }

    @Override
    public String toString() {
        try {
            StringBuilder buf = new StringBuilder();
            String prefix = "";

            fieldMapping.output(this, buf, prefix, new HashSet<IReferenceableInstance>());
            return buf.toString();

        } catch (AtlasException me) {
            throw new RuntimeException(me);
        }
    }

    @Override
    public String toShortString() {
        String name = null;
        if (fieldMapping().fields.containsKey("name")) {
            try {
                name = getString("name");
            } catch (AtlasException e) {
                //ignore if there is no field name
            }
        }
        return String.format("entity[type=%s guid=%s name=%s]", getTypeName(), getId()._getId(), name);
    }

    @Override
    public String getSignatureHash(MessageDigest digester) throws AtlasException {
        ClassType classType = TypeSystem.getInstance().getDataType(ClassType.class, getTypeName());
        classType.updateSignatureHash(digester, this);
        byte[] digest = digester.digest();
        return SHA256Utils.toString(digest);
    }
}
