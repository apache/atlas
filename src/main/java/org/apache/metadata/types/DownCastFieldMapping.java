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

import com.google.common.collect.ImmutableMap;
import org.apache.metadata.MetadataException;
import org.apache.metadata.storage.DownCastStructInstance;
import org.apache.metadata.storage.StructInstance;

public class DownCastFieldMapping {

    public final ImmutableMap<String, String> fieldNameMap;

    protected DownCastFieldMapping(ImmutableMap<String, String> fieldNameMap) {
        this.fieldNameMap = fieldNameMap;
    }

    public void set(DownCastStructInstance s, String attrName, Object val) throws MetadataException {

        String mappedNm = fieldNameMap.get(attrName);
        if ( mappedNm == null ) {
            throw new ValueConversionException(s.getTypeName(), val, "Unknown field " + attrName);
        }

        s.backingInstance.set(mappedNm, val);
    }

    public Object get(DownCastStructInstance s, String attrName) throws MetadataException {

        String mappedNm = fieldNameMap.get(attrName);
        if ( mappedNm == null ) {
            throw new ValueConversionException(
                    String.format("Unknown field %s for Struct %s", attrName, s.getTypeName()));
        }
        return s.backingInstance.get(mappedNm);
    }
}
