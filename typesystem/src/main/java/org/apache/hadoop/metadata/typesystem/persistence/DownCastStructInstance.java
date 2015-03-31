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

package org.apache.hadoop.metadata.typesystem.persistence;

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.IStruct;
import org.apache.hadoop.metadata.typesystem.types.DownCastFieldMapping;

public class DownCastStructInstance implements IStruct {

    public final String typeName;
    public final DownCastFieldMapping fieldMapping;
    public final IStruct backingInstance;

    public DownCastStructInstance(String typeName, DownCastFieldMapping fieldMapping,
                                  IStruct backingInstance) {
        this.typeName = typeName;
        this.fieldMapping = fieldMapping;
        this.backingInstance = backingInstance;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public Object get(String attrName) throws MetadataException {
        return fieldMapping.get(this, attrName);
    }

    @Override
    public void set(String attrName, Object val) throws MetadataException {
        fieldMapping.set(this, attrName, val);
    }
}


