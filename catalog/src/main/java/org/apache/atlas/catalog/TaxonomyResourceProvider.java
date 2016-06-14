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

import org.apache.atlas.catalog.definition.TaxonomyResourceDefinition;
import org.apache.atlas.catalog.exception.*;
import org.apache.atlas.catalog.query.AtlasQuery;

import java.util.*;

/**
 * Provider for taxonomy resources.
 */
public class TaxonomyResourceProvider extends BaseResourceProvider implements ResourceProvider {
    private final TermResourceProvider termResourceProvider;

    public TaxonomyResourceProvider(AtlasTypeSystem typeSystem) {
        super(typeSystem, new TaxonomyResourceDefinition());
        termResourceProvider = new TermResourceProvider(typeSystem);
    }

    @Override
    public Result getResourceById(Request request) throws ResourceNotFoundException {
        AtlasQuery atlasQuery;
        try {
            atlasQuery = queryFactory.createTaxonomyQuery(request);
        } catch (InvalidQueryException e) {
            throw new CatalogRuntimeException("Unable to compile internal Taxonomy query: " + e, e);
        }
        Collection<Map<String, Object>> results = atlasQuery.execute();
        if (results.isEmpty()) {
            throw new ResourceNotFoundException(String.format("Taxonomy '%s' not found.",
                    request.getProperty(resourceDefinition.getIdPropertyName())));
        }
        return new Result(results);
    }

    @Override
    public Result getResources(Request request) throws InvalidQueryException, ResourceNotFoundException {
        AtlasQuery atlasQuery = queryFactory.createTaxonomyQuery(request);
        return new Result(atlasQuery.execute());
    }

    @Override
    public synchronized void createResource(Request request)
            throws InvalidPayloadException, ResourceAlreadyExistsException {

        resourceDefinition.validateCreatePayload(request);
        ensureTaxonomyDoesntExist(request);
        typeSystem.createEntity(resourceDefinition, request);
    }

    @Override
    public Collection<String> createResources(Request request) throws InvalidQueryException, ResourceNotFoundException {
        throw new UnsupportedOperationException("Creating multiple Taxonomies in a request is not currently supported");
    }

    @Override
    public void deleteResourceById(Request request) throws ResourceNotFoundException, InvalidPayloadException {
        String taxonomyId = getResourceId(request);
        getTermResourceProvider().deleteChildren(taxonomyId, new TermPath(request.<String>getProperty("name")));
        typeSystem.deleteEntity(resourceDefinition, request);
    }

    @Override
    public void updateResourceById(Request request) throws ResourceNotFoundException, InvalidPayloadException {
        resourceDefinition.validateUpdatePayload(request);
        AtlasQuery atlasQuery;
        try {
            atlasQuery = queryFactory.createTaxonomyQuery(request);
        } catch (InvalidQueryException e) {
            throw new CatalogRuntimeException("Unable to compile internal Term query: " + e, e);
        }
        if (atlasQuery.execute(request.getUpdateProperties()).isEmpty()) {
            throw new ResourceNotFoundException(String.format("Taxonomy '%s' not found.",
                    request.getQueryProperties().get("name")));
        }
    }

    private void ensureTaxonomyDoesntExist(Request request) throws ResourceAlreadyExistsException {
        try {
            getResourceById(request);
            throw new ResourceAlreadyExistsException(String.format("Taxonomy '%s' already exists.",
                    request.getProperty("name")));
        } catch (ResourceNotFoundException e) {
            // expected case
        }
    }

    private String getResourceId(Request request) throws ResourceNotFoundException {
        request.addAdditionalSelectProperties(Collections.singleton("id"));
        // will result in expected ResourceNotFoundException if taxonomy doesn't exist
        Result result = getResourceById(request);
        return String.valueOf(result.getPropertyMaps().iterator().next().get("id"));
    }

    protected TermResourceProvider getTermResourceProvider() {
        return termResourceProvider;
    }
}