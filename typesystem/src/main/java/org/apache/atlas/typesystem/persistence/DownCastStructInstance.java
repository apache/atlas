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

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.types.DownCastFieldMapping;

import java.util.HashMap;
import java.util.Map;

public class DownCastStructInstance implements IStruct {

    public final String typeName;
    public final DownCastFieldMapping fieldMapping;
    public final IStruct backingInstance;

    public DownCastStructInstance(String typeName, DownCastFieldMapping fieldMapping, IStruct backingInstance) {
        this.typeName = typeName;
        this.fieldMapping = fieldMapping;
        this.backingInstance = backingInstance;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public Object get(String attrName) throws AtlasException {
        return fieldMapping.get(this, attrName);
    }

    @Override
    public void set(String attrName, Object val) throws AtlasException {
        fieldMapping.set(this, attrName, val);
    }

    @Override
    public void setNull(String attrName) throws AtlasException {
        throw new UnsupportedOperationException("unset on attributes are not allowed");
    }

    /*
     * Use only for json serialization
     * @nonpublic
     */
    @Override
    public Map<String, Object> getValuesMap() throws AtlasException {

        Map<String, Object> m = new HashMap<>();
        for (String attr : fieldMapping.fieldNameMap.keySet()) {
            m.put(attr, get(attr));
        }
        return m;
    }

    @Override
    public String toShortString() {
        return toString();
    }
}


