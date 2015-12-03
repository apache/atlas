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

import com.google.common.collect.ImmutableSortedMap;
import org.apache.atlas.AtlasException;

import java.util.Map;

abstract class AbstractDataType<T> implements IDataType<T> {

    protected T convertNull(Multiplicity m) throws AtlasException {
        if (!m.nullAllowed()) {
            throw new ValueConversionException.NullConversionException(m);
        }
        return null;
    }

    @Override
    public void output(T val, Appendable buf, String prefix) throws AtlasException {
        if (val instanceof Map) {
            ImmutableSortedMap immutableSortedMap = ImmutableSortedMap.copyOf((Map) val);
            TypeUtils.outputVal(val == null ? "<null>" : immutableSortedMap.toString(), buf, prefix);
        } else {
            TypeUtils.outputVal(val == null ? "<null>" : val.toString(), buf, prefix);
        }
    }

    /**
     * Validate that current definition can be updated with the new definition
     * @param newType
     */
    @Override
    public void validateUpdate(IDataType newType) throws TypeUpdateException {
        if (!getName().equals(newType.getName()) || !getClass().getName().equals(newType.getClass().getName())) {
            throw new TypeUpdateException(newType);
        }
    }
}

