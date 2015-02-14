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

package org.apache.hadoop.metadata.storage.memory;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.ObjectGraphWalker;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ReplaceIdWithInstance implements ObjectGraphWalker.NodeProcessor {

    final MemRepository repository;
    public final Map<Id, ITypedReferenceableInstance> idToInstanceMap;
    ObjectGraphWalker walker;

    public ReplaceIdWithInstance(MemRepository repository) {
        this.repository = repository;
        idToInstanceMap = new HashMap<Id, ITypedReferenceableInstance>();
    }

    void setWalker(ObjectGraphWalker walker) {
        this.walker = walker;
    }

    @Override
    public void processNode(ObjectGraphWalker.Node nd) throws MetadataException {
        if ( nd.attributeName == null ) {
            return;
        } else if (!nd.aInfo.isComposite || nd.value == null ) {
            return;
        } else if ( nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS ) {

            if ( nd.value != null && nd.value instanceof  Id ) {
                Id id = (Id)nd.value;
                ITypedReferenceableInstance r = getInstance(id);
                nd.instance.set(nd.attributeName, r);
            }
        } else if ( nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY ) {
            DataTypes.ArrayType aT = (DataTypes.ArrayType) nd.aInfo.dataType();
            nd.instance.set(nd.attributeName,
                    convertToInstances((ImmutableCollection)nd.value, nd.aInfo.multiplicity, aT));
        }  else if ( nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP ) {
            DataTypes.MapType mT = (DataTypes.MapType) nd.aInfo.dataType();
            nd.instance.set(nd.attributeName,
                    convertToInstances((ImmutableMap)nd.value, nd.aInfo.multiplicity, mT));
        }
    }

    ImmutableCollection<?> convertToInstances(ImmutableCollection<?> val,
                                                     Multiplicity m, DataTypes.ArrayType arrType)
            throws MetadataException {

        if ( val == null || arrType.getElemType().getTypeCategory() != DataTypes.TypeCategory.CLASS ) {
            return val;
        }

        ImmutableCollection.Builder b = m.isUnique ? ImmutableSet.builder() : ImmutableList.builder();
        Iterator it = val.iterator();
        while(it.hasNext()) {
            Object elem = it.next();
            if ( elem instanceof Id) {
                Id id = (Id) elem;
                elem = getInstance(id);
            }

            b.add(elem);

        }
        return b.build();
    }

    ImmutableMap<?, ?> convertToInstances(ImmutableMap val, Multiplicity m, DataTypes.MapType mapType)
            throws MetadataException {

        if ( val == null || (mapType.getKeyType().getTypeCategory() != DataTypes.TypeCategory.CLASS &&
                mapType.getValueType().getTypeCategory() != DataTypes.TypeCategory.CLASS) ) {
            return val;
        }
        ImmutableMap.Builder b = ImmutableMap.builder();
        Iterator<Map.Entry> it = val.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry elem = it.next();
            Object oldKey = elem.getKey();
            Object oldValue = elem.getValue();
            Object newKey = oldKey;
            Object newValue = oldValue;

            if ( oldKey instanceof Id ) {
                Id id = (Id) elem;
                ITypedReferenceableInstance r = getInstance(id);
            }

            if ( oldValue instanceof Id ) {
                Id id = (Id) elem;
                ITypedReferenceableInstance r = getInstance(id);
            }

            b.put(newKey, newValue);
        }
        return b.build();
    }

    ITypedReferenceableInstance getInstance(Id id) throws MetadataException {

        ITypedReferenceableInstance r = idToInstanceMap.get(id);
        if ( r == null ) {
            r = repository.get(id);
            idToInstanceMap.put(id, r);
            walker.addRoot(r);
        }
        return r;
    }
}
