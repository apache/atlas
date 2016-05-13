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
import org.apache.atlas.typesystem.ITypedStruct;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TraitType extends HierarchicalType<TraitType, IStruct>
        implements IConstructableType<IStruct, ITypedStruct> {

    public final Map<AttributeInfo, List<String>> infoToNameMap;
    private final TypedStructHandler handler;

    TraitType(TypeSystem typeSystem, String name, String description, ImmutableSet<String> superTraits, int numFields) {
        super(typeSystem, TraitType.class, name, description, superTraits, numFields);
        handler = null;
        infoToNameMap = null;
    }

    TraitType(TypeSystem typeSystem, String name, String description, ImmutableSet<String> superTraits, AttributeInfo... fields)
    throws AtlasException {
        super(typeSystem, TraitType.class, name, description, superTraits, fields);
        handler = new TypedStructHandler(this);
        infoToNameMap = TypeUtils.buildAttrInfoToNameMap(fieldMapping);
    }

    @Override
    public DataTypes.TypeCategory getTypeCategory() {
        return DataTypes.TypeCategory.TRAIT;
    }

    @Override
    public ITypedStruct convert(Object val, Multiplicity m) throws AtlasException {
        return handler.convert(val, m);
    }

    public ITypedStruct createInstance() {
        return handler.createInstance();
    }

    @Override
    public void output(IStruct s, Appendable buf, String prefix, Set<IStruct> inProcess) throws AtlasException {
        handler.output(s, buf, prefix, inProcess);
    }

    @Override
    public void updateSignatureHash(MessageDigest digester, Object val) throws AtlasException {
        if( !(val instanceof  ITypedStruct)) {
            throw new IllegalArgumentException("Unexpected value type " + val.getClass().getSimpleName() + ". Expected instance of ITypedStruct");
        }
        digester.update(getName().getBytes(Charset.forName("UTF-8")));

        if(fieldMapping.fields != null && val != null) {
            IStruct typedValue = (IStruct) val;
            for (AttributeInfo aInfo : fieldMapping.fields.values()) {
                Object attrVal = typedValue.get(aInfo.name);
                if(attrVal != null) {
                    aInfo.dataType().updateSignatureHash(digester, attrVal);
                }
            }
        }
    }

    @Override
    public List<String> getNames(AttributeInfo info) {
        return infoToNameMap.get(info);
    }

}
