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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.catalog.definition.TaxonomyResourceDefinition;
import org.apache.atlas.catalog.exception.*;
import org.apache.atlas.catalog.query.AtlasQuery;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Provider for taxonomy resources.
 */
public class TaxonomyResourceProvider extends BaseResourceProvider implements ResourceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(TaxonomyResourceProvider.class);
    public static final String DEFAULT_TAXONOMY_NAME = "Catalog";
    public static final String DEFAULT_TAXONOMY_DESCRIPTION = "Business Catalog";

    public static final String NAMESPACE_ATTRIBUTE_NAME = "taxonomy.namespace";

    // Taxonomy Term type
    public static final String TAXONOMY_TERM_TYPE = "TaxonomyTerm";

    // Taxonomy Namespace
    public static final String TAXONOMY_NS = "atlas.taxonomy";

    private final TermResourceProvider termResourceProvider;

    // This is a cached value to prevent checking for taxonomy objects in every API call.
    // It is updated once per lifetime of the application.
    // TODO: If a taxonomy is deleted outside of this application, this value is not updated
    // TODO: and there is no way in which a taxonomy will be auto-created.
    // TODO: Assumption is that if a taxonomy is deleted externally, it will be created externally as well.
    private static boolean taxonomyAutoInitializationChecked = false;

    public TaxonomyResourceProvider(AtlasTypeSystem typeSystem) {
        super(typeSystem, new TaxonomyResourceDefinition());
        termResourceProvider = new TermResourceProvider(typeSystem);
    }

    @Override
    public Result getResourceById(Request request) throws ResourceNotFoundException {
        synchronized (TaxonomyResourceProvider.class) {
            createDefaultTaxonomyIfNeeded();
        }
        return doGetResourceById(request);
    }

    @Override
    public Result getResources(Request request) throws InvalidQueryException, ResourceNotFoundException {
        synchronized (TaxonomyResourceProvider.class) {
            createDefaultTaxonomyIfNeeded();
        }
        return doGetResources(request);
    }

    @Override
    public void createResource(Request request)
            throws InvalidPayloadException, ResourceAlreadyExistsException {

        // not checking for default taxonomy in create per requirements
        resourceDefinition.validateCreatePayload(request);
        synchronized (TaxonomyResourceProvider.class) {
            ensureTaxonomyDoesntExist(request);
            doCreateResource(request);
        }
    }

    @Override
    public Collection<String> createResources(Request request) throws InvalidQueryException, ResourceNotFoundException {
        throw new UnsupportedOperationException(
                "Creating multiple Taxonomies in a request is not currently supported");
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

        synchronized (TaxonomyResourceProvider.class) {
            createDefaultTaxonomyIfNeeded();
            if (atlasQuery.execute(request.getUpdateProperties()).isEmpty()) {
                throw new ResourceNotFoundException(String.format("Taxonomy '%s' not found.",
                        request.getQueryProperties().get("name")));
            }
        }
    }

    private String getResourceId(Request request) throws ResourceNotFoundException {
        request.addAdditionalSelectProperties(Collections.singleton("id"));
        // will result in expected ResourceNotFoundException if taxonomy doesn't exist
        Result result = getResourceById(request);
        return String.valueOf(result.getPropertyMaps().iterator().next().get("id"));
    }

    //todo: this is currently required because the expected exception isn't thrown by the Atlas repository
    //todo: when an attempt is made to create an entity that already exists
    // must be called from within class monitor
    private void ensureTaxonomyDoesntExist(Request request) throws ResourceAlreadyExistsException {
        try {
            doGetResourceById(request);
            throw new ResourceAlreadyExistsException(String.format("Taxonomy '%s' already exists.",
                    (String) request.getProperty("name")));
        } catch (ResourceNotFoundException e) {
            // expected case
        }
    }

    // must be called from within class monitor
    private Result doGetResourceById(Request request) throws ResourceNotFoundException {
        AtlasQuery atlasQuery;
        try {
            atlasQuery = queryFactory.createTaxonomyQuery(request);
        } catch (InvalidQueryException e) {
            throw new CatalogRuntimeException("Unable to compile internal Taxonomy query: " + e, e);
        }

        Collection<Map<String, Object>> resultSet = atlasQuery.execute();
        if (resultSet.isEmpty()) {
            throw new ResourceNotFoundException(String.format("Taxonomy '%s' not found.",
                    (String) request.getProperty(resourceDefinition.getIdPropertyName())));
        }
        return new Result(resultSet);
    }

    // must be called from within class monitor
    private Result doGetResources(Request request) throws InvalidQueryException, ResourceNotFoundException {
        AtlasQuery atlasQuery = queryFactory.createTaxonomyQuery(request);
        return new Result(atlasQuery.execute());
    }

    // must be called from within class monitor
    private void doCreateResource(Request request) throws ResourceAlreadyExistsException {
        typeSystem.createEntity(resourceDefinition, request);
        taxonomyAutoInitializationChecked = true;
    }

    // must be called from within class monitor
    private void createDefaultTaxonomyIfNeeded() {
        if (! autoInitializationChecked()) {
            try {
                LOG.info("Checking if default taxonomy needs to be created.");
                // if any business taxonomy has been created, don't create one more - hence searching to
                // see if any taxonomy exists.
                if (doGetResources(new CollectionRequest(null, null)).getPropertyMaps().isEmpty()) {
                    LOG.info("No taxonomies found - going to create default taxonomy.");
                    Map<String, Object> requestProperties = new HashMap<>();
                    String defaultTaxonomyName = DEFAULT_TAXONOMY_NAME;
                    try {
                        Configuration configuration = ApplicationProperties.get();
                        defaultTaxonomyName = configuration.getString("atlas.taxonomy.default.name",
                                defaultTaxonomyName);
                    } catch (AtlasException e) {
                        LOG.warn("Unable to read Atlas configuration, will use {} as default taxonomy name",
                                defaultTaxonomyName, e);
                    }
                    requestProperties.put("name", defaultTaxonomyName);
                    requestProperties.put("description", DEFAULT_TAXONOMY_DESCRIPTION);

                    doCreateResource(new InstanceRequest(requestProperties));
                    LOG.info("Successfully created default taxonomy {}.", defaultTaxonomyName);
                } else {
                    taxonomyAutoInitializationChecked = true;
                    LOG.info("Some taxonomy exists, not creating default taxonomy");
                }
            } catch (InvalidQueryException | ResourceNotFoundException e) {
                LOG.error("Unable to query for existing taxonomies due to internal error.", e);
            } catch (ResourceAlreadyExistsException e) {
                LOG.info("Attempted to create default taxonomy and it already exists.");
            }
        }
    }

    protected boolean autoInitializationChecked() {
        return taxonomyAutoInitializationChecked;
    }

    protected TermResourceProvider getTermResourceProvider() {
        return termResourceProvider;
    }
}