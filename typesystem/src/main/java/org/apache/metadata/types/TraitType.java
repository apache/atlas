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

package org.apache.metadata.types;

import com.google.common.collect.ImmutableList;
import org.apache.metadata.IStruct;
import org.apache.metadata.ITypedStruct;
import org.apache.metadata.MetadataException;

import java.util.List;
import java.util.Map;

public class TraitType extends HierarchicalType<TraitType, IStruct>
        implements IConstructableType<IStruct, ITypedStruct> {

    private final TypedStructHandler handler;
    public final Map<AttributeInfo, List<String>> infoToNameMap;

    /**
     * Used when creating a TraitType, to support recursive Structs.
     */
    TraitType(TypeSystem typeSystem, String name, ImmutableList<String> superTraits, int numFields) {
        super(typeSystem, TraitType.class, name, superTraits, numFields);
        handler = null;
        infoToNameMap = null;
    }

    TraitType(TypeSystem typeSystem, String name, ImmutableList<String> superTraits, AttributeInfo... fields)
            throws MetadataException {
        super(typeSystem, TraitType.class, name, superTraits, fields);
        handler = new TypedStructHandler(this);
        infoToNameMap = TypeUtils.buildAttrInfoToNameMap(fieldMapping);
    }

    @Override
    public DataTypes.TypeCategory getTypeCategory() {
        return DataTypes.TypeCategory.TRAIT;
    }

    @Override
    public ITypedStruct convert(Object val, Multiplicity m) throws MetadataException {
        return handler.convert(val, m);
    }

    public ITypedStruct createInstance() {
        return handler.createInstance();
    }

    @Override
    public void output(IStruct s, Appendable buf, String prefix) throws MetadataException {
        handler.output(s, buf, prefix);
    }

    @Override
    public List<String> getNames(AttributeInfo info) {
        return infoToNameMap.get(info);
    }

}
