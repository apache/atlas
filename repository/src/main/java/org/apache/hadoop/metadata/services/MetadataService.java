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

package org.apache.hadoop.metadata.services;

import org.apache.hadoop.metadata.MetadataException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;

/**
 * Metadata service.
 */
public interface MetadataService {

    /**
     * Creates a new type based on the type system to enable adding
     * entities (instances for types).
     *
     * @param typeName       name for this type, must be unique
     * @param typeDefinition definition as json
     * @return a unique id for this type
     */
    JSONObject createType(String typeName,
                          String typeDefinition) throws MetadataException;

    /**
     * Return the definition for the given type.
     *
     * @param typeName name for this type, must be unique
     * @return type definition as JSON
     */
    String getTypeDefinition(String typeName) throws MetadataException;

    /**
     * Return the list of types in the repository.
     *
     * @return list of type names in the repository
     */
    List<String> getTypeNamesList() throws MetadataException;

    /**
     * Creates an entity, instance of the type.
     *
     * @param entityType type
     * @param entityDefinition definition
     * @return guid
     */
    String createEntity(String entityType, String entityDefinition) throws MetadataException;

    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    String getEntityDefinition(String guid) throws MetadataException;

    /**
     * Return the list of entity names for the given type in the repository.
     *
     * @param entityType type
     * @return list of entity names for the given type in the repository
     */
    List<String> getEntityList(String entityType) throws MetadataException;
}
