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

package org.apache.hadoop.metadata.typesystem.types.store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.json.TypesSerialization$;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;

import java.util.HashMap;
import java.util.Map;

public abstract class TypeSystemStore {

    /**
     * Persist the type system under namespace - insert or update
     * @param typeSystem type system to persist
     * @param namespace
     * @throws StorageException
     */
    public synchronized void store(TypeSystem typeSystem, String namespace)
            throws StorageException {
        String json = TypesSerialization$.MODULE$.toJson(typeSystem, typeSystem.getTypeNames());
        publish(namespace, json);
    }

    /**
     * Restore all type definitions
     * @return List of persisted type definitions
     * @throws MetadataException
     */
    public synchronized ImmutableMap<String, TypesDef> restore() throws MetadataException {
        ImmutableList<String> nameSpaces = listNamespaces();
        Map<String, TypesDef> typesDefs = new HashMap<>();

        for (String namespace : nameSpaces) {
            String json = fetch(namespace);
            typesDefs.put(namespace, TypesSerialization$.MODULE$.fromJson(json));
        }
        return ImmutableMap.copyOf(typesDefs);
    }

    /**
     * Restore specified namespace as type definition
     * @param namespace
     * @return type definition
     * @throws MetadataException
     */
    public synchronized TypesDef restore(String namespace) throws MetadataException {
        String json = fetch(namespace);
        return TypesSerialization$.MODULE$.fromJson(json);
    }

    /**
     * Delete the specified namespace
     * @param namespace
     * @throws StorageException
     */
    public abstract void delete(String namespace) throws StorageException;


    //Interfaces for concrete implementations
    protected abstract void publish(String namespace, String json) throws StorageException;

    protected abstract String fetch(String namespace) throws StorageException;

    protected abstract ImmutableList<String> listNamespaces() throws MetadataException;
}
