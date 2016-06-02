/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog;

import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.catalog.exception.ResourceAlreadyExistsException;
import org.apache.atlas.catalog.exception.ResourceNotFoundException;

import java.util.Map;

/**
 * Abstraction for Atlas Type System.
 */
public interface AtlasTypeSystem {

    /**
     * Create a Type in the Atlas Type System if it doesn't already exist.
     * If the type already exists, this method has no affect.
     *
     * @param resourceDefinition  resource definition for type being created
     * @param name                type name
     * @param description         description of the type being created
     *
     * @throws ResourceAlreadyExistsException if entity already exists
     */
    void createClassType(ResourceDefinition resourceDefinition, String name, String description)
            throws ResourceAlreadyExistsException;

    /**
     * Create an entity in the Atlas Type System for the provided request and resource definition.
     * If Type associated with the entity doesn't already exist, it is created.
     *
     * @param definition the definition of the resource for which we are creating the entity
     * @param request    the user request
     *
     * @throws ResourceAlreadyExistsException if type already exists
     */
    void createEntity(ResourceDefinition definition, Request request)
            throws ResourceAlreadyExistsException;

    /**
     * Delete an entity from the Atlas type system.
     *
     * @param definition  definition of the resource being deleted
     * @param request     user request
     *
     * @throws ResourceNotFoundException if the resource to delete doesn't exist
     */
    void deleteEntity(ResourceDefinition definition, Request request) throws ResourceNotFoundException;

    /**
     * Create a trait instance instance in the Atlas Type System.
     *
     * @param resourceDefinition  resource definition for trait type being created
     * @param name                type name
     * @param description         description of the type being created
     *
     * @throws ResourceAlreadyExistsException if type already exists
     */
    void createTraitType(ResourceDefinition resourceDefinition, String name, String description)
            throws ResourceAlreadyExistsException;

    /**
     * Create a trait instance in the Atlas Type System and associate it with the entity identified by the provided guid.
     *
     * @param guid        id of the entity which will be associated with the trait instance
     * @param typeName    type name of the trait
     * @param properties  property map used to populate the trait instance
     *
     * @throws ResourceAlreadyExistsException if trait instance is already associated with the entity
     */
    void createTraitInstance(String guid, String typeName, Map<String, Object> properties)
            throws ResourceAlreadyExistsException;

    /**
     * Delete a tag instance.
     *
     * @param guid       associated entity guid
     * @param traitName  name of the trait to delete
     *
     * @throws ResourceNotFoundException if the specified trait doesn't exist for the specified entity
     */
    void deleteTag(String guid, String traitName) throws ResourceNotFoundException;
}
