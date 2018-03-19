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

import org.apache.atlas.catalog.definition.EntityResourceDefinition;
import org.apache.atlas.catalog.exception.*;
import org.apache.atlas.catalog.query.AtlasQuery;

import java.util.*;

/**
 * Provider for entity resources.
 */
public class EntityResourceProvider extends BaseResourceProvider implements ResourceProvider {

    public EntityResourceProvider(AtlasTypeSystem typeSystem) {
        super(typeSystem, new EntityResourceDefinition());
    }

    @Override
    public Result getResourceById(Request request) throws ResourceNotFoundException {
        AtlasQuery atlasQuery;
        try {
            atlasQuery = queryFactory.createEntityQuery(request);
        } catch (InvalidQueryException e) {
            throw new CatalogRuntimeException("Unable to compile internal Entity query: " + e, e);
        }
        Collection<Map<String, Object>> results = atlasQuery.execute();
        if (results.isEmpty()) {
            throw new ResourceNotFoundException(String.format("Entity '%s' not found.",
                    (String) request.getProperty(resourceDefinition.getIdPropertyName())));
        }
        return new Result(results);
    }

    @Override
    public Result getResources(Request request) throws InvalidQueryException, ResourceNotFoundException {
        AtlasQuery atlasQuery = queryFactory.createEntityQuery(request);
        return new Result(atlasQuery.execute());
    }

    @Override
    public void createResource(Request request)
            throws InvalidPayloadException, ResourceAlreadyExistsException, ResourceNotFoundException {

        // creation of entities is currently unsupported
        throw new UnsupportedOperationException("Creation of entities is not currently supported");
    }

    @Override
    public Collection<String> createResources(Request request) throws InvalidQueryException, ResourceNotFoundException {
        throw new UnsupportedOperationException("Creation of entities is not currently supported");
    }
}
