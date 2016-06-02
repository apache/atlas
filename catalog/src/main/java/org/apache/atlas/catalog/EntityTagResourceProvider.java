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

import org.apache.atlas.catalog.definition.EntityTagResourceDefinition;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.catalog.exception.*;
import org.apache.atlas.catalog.query.AtlasQuery;

import java.util.*;

/**
 * Provider for entity tag resources.
 */
public class EntityTagResourceProvider extends BaseResourceProvider implements ResourceProvider {
    private final static ResourceDefinition resourceDefinition = new EntityTagResourceDefinition();
    private TermResourceProvider termResourceProvider;

    public EntityTagResourceProvider(AtlasTypeSystem typeSystem) {
        super(typeSystem);

    }

    @Override
    public Result getResourceById(Request request) throws ResourceNotFoundException {
        AtlasQuery atlasQuery;
        try {
            atlasQuery = queryFactory.createEntityTagQuery(request);
        } catch (InvalidQueryException e) {
            throw new CatalogRuntimeException("Unable to compile internal Entity Tag query: " + e, e);
        }
        Collection<Map<String, Object>> results = atlasQuery.execute();
        if (results.isEmpty()) {
            throw new ResourceNotFoundException(String.format("Tag '%s' not found.",
                    request.getProperty(resourceDefinition.getIdPropertyName())));
        }

        return new Result(results);
    }

    @Override
    public Result getResources(Request request) throws InvalidQueryException, ResourceNotFoundException {
        AtlasQuery atlasQuery = queryFactory.createEntityTagQuery(request);
        return new Result(atlasQuery.execute());
    }

    @Override
    public void createResource(Request request)
            throws InvalidPayloadException, ResourceAlreadyExistsException, ResourceNotFoundException {

        String entityId = String.valueOf(request.getProperties().remove("id"));
        resourceDefinition.validate(request);
        Result termResult = getTermQueryResult(request.<String>getProperty("name"));
        Map<String, Object> termProperties = termResult.getPropertyMaps().iterator().next();
        //todo: use constant for property name
        if (String.valueOf(termProperties.get("available_as_tag")).equals("false")) {
            throw new InvalidPayloadException(
                    "Attempted to tag an entity with a term which is not available to be tagged");
        }
        tagEntities(Collections.singleton(entityId), termProperties);
    }

    //todo: response for case mixed case where some subset of creations fail
    @Override
    public Collection<String> createResources(Request request)
            throws InvalidQueryException, ResourceNotFoundException, ResourceAlreadyExistsException {

        Collection<String> relativeUrls = new ArrayList<>();
        AtlasQuery atlasQuery = queryFactory.createEntityQuery(request);
        Collection<String> guids = new ArrayList<>();
        for (Map<String, Object> entityMap: atlasQuery.execute()) {
            guids.add(String.valueOf(entityMap.get("id")));
        }

        Collection<Map<String, String>> tagMaps = request.getProperty("tags");
        for (Map<String, String> tagMap : tagMaps) {
            Result termResult = getTermQueryResult(tagMap.get("name"));
            relativeUrls.addAll(tagEntities(guids, termResult.getPropertyMaps().iterator().next()));
        }
        return relativeUrls;
    }

    @Override
    public void deleteResourceById(Request request) throws ResourceNotFoundException, InvalidPayloadException {
        typeSystem.deleteTag(request.<String>getProperty("id"), request.<String>getProperty("name"));
    }

    private Result getTermQueryResult(String termName) throws ResourceNotFoundException {
        Request tagRequest = new InstanceRequest(
                Collections.<String, Object>singletonMap("termPath", new TermPath(termName)));

        tagRequest.addAdditionalSelectProperties(Collections.singleton("type"));
        return getTermResourceProvider().getResourceById(tagRequest);
    }

    private Collection<String> tagEntities(Collection<String> entityGuids, Map<String, Object> termProperties)
            throws ResourceAlreadyExistsException {

        Collection<String> relativeUrls = new ArrayList<>();
        for (String guid : entityGuids) {
            //createTermEdge(entity, Collections.singleton(termVertex));
            // copy term properties from trait associated with taxonomy to be set
            // on trait associated with new entity (basically clone at time of tag event)
            //todo: any changes to 'singleton' trait won't be reflected in new trait
            //todo: iterate over properties in term definition instead of hard coding here
            Map<String, Object> properties = new HashMap<>();
            String termName = String.valueOf(termProperties.get("name"));
            properties.put("name", termName);
            properties.put("description", termProperties.get("description"));

            typeSystem.createTraitInstance(guid, termName, properties);
            //todo: *** shouldn't know anything about href structure in this class ***
            relativeUrls.add(String.format("v1/entities/%s/tags/%s", guid, termName));
        }
        return relativeUrls;
    }

    protected synchronized ResourceProvider getTermResourceProvider() {
        if (termResourceProvider == null) {
            termResourceProvider = new TermResourceProvider(typeSystem);
        }
        return termResourceProvider;
    }
}
