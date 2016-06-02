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
import org.apache.atlas.catalog.definition.TermResourceDefinition;
import org.apache.atlas.catalog.exception.*;
import org.apache.atlas.catalog.query.AtlasQuery;

import java.util.*;

/**
 * Provider for Term resources.
 */
public class TermResourceProvider extends BaseResourceProvider implements ResourceProvider {
    private final static ResourceDefinition resourceDefinition = new TermResourceDefinition();
    private ResourceProvider taxonomyResourceProvider;
    private ResourceProvider entityResourceProvider;
    private ResourceProvider entityTagResourceProvider;

    public TermResourceProvider(AtlasTypeSystem typeSystem) {
        super(typeSystem);
    }

    @Override
    public Result getResourceById(Request request) throws ResourceNotFoundException {
        //todo: shouldn't need to add this here
        request.getProperties().put("name", request.<TermPath>getProperty("termPath").getFullyQualifiedName());
        AtlasQuery atlasQuery;
        try {
            atlasQuery = queryFactory.createTermQuery(request);
        } catch (InvalidQueryException e) {
            throw new CatalogRuntimeException("Unable to compile internal Term query: " + e, e);
        }
        Collection<Map<String, Object>> results = atlasQuery.execute();
        if (results.isEmpty()) {
            throw new ResourceNotFoundException(String.format("Term '%s' not found.",
                    request.<TermPath>getProperty("termPath").getFullyQualifiedName()));
        }
        return new Result(results);
    }

    public Result getResources(Request request)
            throws InvalidQueryException, ResourceNotFoundException  {

        TermPath termPath = request.getProperty("termPath");
        String queryString = doQueryStringConversions(termPath, request.getQueryString());
        Request queryRequest = new CollectionRequest(request.getProperties(), queryString);
        AtlasQuery atlasQuery = queryFactory.createTermQuery(queryRequest);
        Collection<Map<String, Object>> result = atlasQuery.execute();
        return new Result(result);
    }

    public void createResource(Request request)
            throws InvalidPayloadException, ResourceAlreadyExistsException, ResourceNotFoundException  {

        TermPath termPath = (TermPath) request.getProperties().remove("termPath");
        String qualifiedTermName = termPath.getFullyQualifiedName();
        request.getProperties().put("name", qualifiedTermName);
        resourceDefinition.validate(request);

        // get taxonomy
        Request taxonomyRequest = new InstanceRequest(
                Collections.<String, Object>singletonMap("name", termPath.getTaxonomyName()));
        taxonomyRequest.addAdditionalSelectProperties(Collections.singleton("id"));
        Result taxonomyResult = getTaxonomyResourceProvider().getResourceById(taxonomyRequest);
        Map<String, Object> taxonomyPropertyMap = taxonomyResult.getPropertyMaps().iterator().next();

        // ensure that parent exists if not a root level term
        if (! termPath.getPath().equals("/")) {
            Map<String, Object> parentProperties = new HashMap<>(request.getProperties());
            parentProperties.put("termPath", termPath.getParent());
            getResourceById(new InstanceRequest(parentProperties));
        }

        typeSystem.createTraitType(resourceDefinition, qualifiedTermName,
                request.<String>getProperty("description"));

        typeSystem.createTraitInstance(String.valueOf(taxonomyPropertyMap.get("id")),
                qualifiedTermName, request.getProperties());
    }

    @Override
    public Collection<String> createResources(Request request) throws InvalidQueryException, ResourceNotFoundException {
        throw new UnsupportedOperationException("Creating multiple Terms in a request is not currently supported");
    }

    @Override
    public void deleteResourceById(Request request) throws ResourceNotFoundException, InvalidPayloadException {
        // will result in expected ResourceNotFoundException if term doesn't exist
        getResourceById(request);

        TermPath termPath = (TermPath) request.getProperties().get("termPath");
        String taxonomyId = getTaxonomyId(termPath);
        deleteChildren(taxonomyId, termPath);
        deleteTerm(taxonomyId, termPath);
    }

    protected void deleteChildren(String taxonomyId, TermPath termPath)
            throws ResourceNotFoundException, InvalidPayloadException {

        TermPath collectionTermPath = new TermPath(termPath.getFullyQualifiedName() + ".");
        Request queryRequest = new CollectionRequest(Collections.<String, Object>singletonMap("termPath",
                collectionTermPath), null);

        AtlasQuery collectionQuery;
        try {
            collectionQuery = queryFactory.createTermQuery(queryRequest);
        } catch (InvalidQueryException e) {
            throw new CatalogRuntimeException("Failed to compile internal predicate: " + e, e);
        }

        Collection<Map<String, Object>> children = collectionQuery.execute();
        for (Map<String, Object> childMap : children) {
            deleteTerm(taxonomyId, new TermPath(String.valueOf(childMap.get("name"))));
        }
    }

    private void deleteTerm(String taxonomyId, TermPath termPath)
            throws ResourceNotFoundException, InvalidPayloadException {

        String  fullyQualifiedName = termPath.getFullyQualifiedName();
        deleteEntityTagsForTerm(fullyQualifiedName);

        // delete term instance associated with the taxonomy
        typeSystem.deleteTag(taxonomyId, fullyQualifiedName);
        //todo: Currently no way to delete type via MetadataService or MetadataRepository
    }

    private void deleteEntityTagsForTerm(String fullyQualifiedName) throws ResourceNotFoundException {
        String  entityQueryStr = String.format("tags/name:%s", fullyQualifiedName);
        Request entityRequest  = new CollectionRequest(Collections.<String, Object>emptyMap(), entityQueryStr);
        Result entityResult;
        try {
            entityResult = getEntityResourceProvider().getResources(entityRequest);
        } catch (InvalidQueryException e) {
            throw new CatalogRuntimeException(String.format(
                    "Failed to compile internal predicate for query '%s': %s", entityQueryStr, e), e);
        }

        for (Map<String, Object> entityResultMap : entityResult.getPropertyMaps()) {
            Map<String, Object> tagRequestProperties = new HashMap<>();
            tagRequestProperties.put("id", String.valueOf(entityResultMap.get("id")));
            tagRequestProperties.put("name", fullyQualifiedName);
            try {
                getEntityTagResourceProvider().deleteResourceById(new InstanceRequest(tagRequestProperties));
            } catch (InvalidPayloadException e) {
                throw new CatalogRuntimeException(
                        "An internal error occurred while trying to delete an entity tag: " + e, e);
            }
        }
    }

    private String getTaxonomyId(TermPath termPath) throws ResourceNotFoundException {
        Request taxonomyRequest = new InstanceRequest(Collections.<String, Object>singletonMap(
                "name", termPath.getTaxonomyName()));
        taxonomyRequest.addAdditionalSelectProperties(Collections.singleton("id"));
        // will result in proper ResourceNotFoundException if taxonomy doesn't exist
        Result taxonomyResult = getTaxonomyResourceProvider().getResourceById(taxonomyRequest);

        Map<String, Object> taxonomyResultMap = taxonomyResult.getPropertyMaps().iterator().next();
        return String.valueOf(taxonomyResultMap.get("id"));
    }

    //todo: add generic support for pre-query modification of expected value
    //todo: similar path parsing code is used in several places in this class
    private String doQueryStringConversions(TermPath termPath, String queryStr) throws InvalidQueryException {
        String hierarchyPathProp = "hierarchy/path";
        // replace "."
        if (queryStr != null && queryStr.contains(String.format("%s:.", hierarchyPathProp))) {
            //todo: regular expression replacement
            queryStr = queryStr.replaceAll(String.format("%s:.", hierarchyPathProp),
                    String.format("%s:%s", hierarchyPathProp, termPath.getPath()));
        }
        return queryStr;
    }

    protected synchronized ResourceProvider getTaxonomyResourceProvider() {
        if (taxonomyResourceProvider == null) {
            taxonomyResourceProvider = new TaxonomyResourceProvider(typeSystem);
        }
        return taxonomyResourceProvider;
    }

    protected synchronized ResourceProvider getEntityResourceProvider() {
        if (entityResourceProvider == null) {
            entityResourceProvider = new EntityResourceProvider(typeSystem);
        }
        return entityResourceProvider;
    }

    protected synchronized ResourceProvider getEntityTagResourceProvider() {
        if (entityTagResourceProvider == null) {
            entityTagResourceProvider = new EntityTagResourceProvider(typeSystem);
        }
        return entityTagResourceProvider;
    }
}







