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

package org.apache.hadoop.metadata.storage;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.ObjectGraphWalker;

import java.util.Map;

public class MapIds implements ObjectGraphWalker.NodeProcessor {

    final Map<Id, Id> idToNewIdMap;

    public MapIds(Map<Id, Id> idToNewIdMap) {
        this.idToNewIdMap = idToNewIdMap;
    }

    @Override
    public void processNode(ObjectGraphWalker.Node nd) throws MetadataException {

        IReferenceableInstance ref =  null;
        Id id = null;

        if ( nd.attributeName == null ) {
            ref = (IReferenceableInstance) nd.instance;
            Id newId = idToNewIdMap.get(ref.getId());
            if ( newId != null ) {
                ((ReferenceableInstance)ref).replaceWithNewId(newId);
            }
        } else if ( nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS ) {
            if ( nd.value != null && nd.value instanceof  IReferenceableInstance ) {
                Id oldId = ((IReferenceableInstance)nd.value).getId();
                Id newId = idToNewIdMap.get(oldId);
                /*
                 * Replace Instances with Ids, irrespective of whether they map to newIds or not.
                 */
                newId = newId == null ? oldId : newId;
                nd.instance.set(nd.attributeName, newId);
            }
        } else if ( nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY ) {
            DataTypes.ArrayType aT = (DataTypes.ArrayType) nd.aInfo.dataType();
            Object v = aT.mapIds((ImmutableCollection)nd.value, nd.aInfo.multiplicity, idToNewIdMap);
            nd.instance.set(nd.attributeName, v);
        }  else if ( nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP ) {
            DataTypes.MapType mT = (DataTypes.MapType) nd.aInfo.dataType();
            Object v = mT.mapIds((ImmutableMap)nd.value, nd.aInfo.multiplicity, idToNewIdMap);
            nd.instance.set(nd.attributeName, v);
        }
    }
}
