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

package org.apache.metadata.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.metadata.IStruct;
import org.apache.metadata.ITypedReferenceableInstance;
import org.apache.metadata.ITypedStruct;
import org.apache.metadata.MetadataException;
import org.apache.metadata.types.FieldMapping;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/*
 * @todo handle names prefixed by traitName.
 */
public class ReferenceableInstance extends StructInstance implements ITypedReferenceableInstance {

    private final Id id;
    private final ImmutableMap<String, ITypedStruct> traits;
    private final ImmutableList<String> traitNames;


    public ReferenceableInstance(Id id, String dataTypeName, FieldMapping fieldMapping, boolean[] nullFlags,
                                 boolean[] bools, byte[] bytes, short[] shorts, int[] ints, long[] longs,
                                 float[] floats, double[] doubles, BigDecimal[] bigDecimals,
                                 BigInteger[] bigIntegers, Date[] dates, String[] strings,
                                 ImmutableList<Object>[] arrays, ImmutableMap<Object, Object>[] maps,
                                 StructInstance[] structs,
                                 ReferenceableInstance[] referenceableInstances,
                                 Id[] ids,
                                 ImmutableMap<String, ITypedStruct> traits) {
        super(dataTypeName, fieldMapping, nullFlags, bools, bytes, shorts, ints, longs, floats, doubles, bigDecimals,
                bigIntegers, dates, strings, arrays, maps, structs, referenceableInstances, ids);
        this.id = id;
        this.traits = traits;
        ImmutableList.Builder<String> b = new ImmutableList.Builder<String>();
        for(String t : traits.keySet()) {
            b.add(t);
        }
        this.traitNames = b.build();
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
    public String toString()  {
        try {
            StringBuilder buf = new StringBuilder();
            String prefix = "";

            fieldMapping.output(this, buf, prefix);
            return buf.toString();

        } catch(MetadataException me) {
            throw new RuntimeException(me);
        }
    }
}
