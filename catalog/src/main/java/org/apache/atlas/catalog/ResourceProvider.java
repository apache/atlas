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

import org.apache.atlas.catalog.exception.*;

import java.util.Collection;

/**
 * Provider for a resource type.
 */
public interface ResourceProvider {
    /**
     * Get a resource by primary key.
     *
     * @param request  request instance which contains the required id properties and no query string
     * @return  result containing the requested resource; never null
     *
     * @throws ResourceNotFoundException if the requested resource isn't found
     */
    Result getResourceById(Request request) throws ResourceNotFoundException;

    /**
     * Get all resources which match the provider query.
     *
     * @param request request instance which will include a query string and possibly properties
     * @return result containing collection of matching resources. If no resources match
     *         a result is returned with no resources
     *
     * @throws InvalidQueryException     if the user query contains invalid syntax
     * @throws ResourceNotFoundException if a parent resource of the requested resource doesn't exist
     */
    Result getResources(Request request) throws InvalidQueryException, ResourceNotFoundException;

    /**
     * Create a single resource.
     *
     * @param request request instance containing the contents of the resource to create
     *
     * @throws InvalidPayloadException        if the payload or any other part of the user request is invalid
     * @throws ResourceAlreadyExistsException if the resource already exists
     * @throws ResourceNotFoundException      if a parent of the resource to create doesn't exist
     */
    void createResource(Request request)
            throws InvalidPayloadException, ResourceAlreadyExistsException, ResourceNotFoundException;

    //todo: define the behavior for partial success
    /**
     * Create multiple resources.
     *
     * @param request  request instance containing the contents of 1..n resources
     * @return collection of relative urls for the created resources
     *
     * @throws InvalidPayloadException        if the payload or any other part of the user request is invalid
     * @throws ResourceAlreadyExistsException if the resource already exists
     * @throws ResourceNotFoundException      if a parent of the resource to create doesn't exist
     */
    Collection<String> createResources(Request request) throws CatalogException;
}
