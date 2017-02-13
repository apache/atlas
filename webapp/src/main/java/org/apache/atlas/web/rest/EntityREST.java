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
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.ClassificationAssociateRequest;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v1.EntityStream;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.converters.AtlasInstanceConverter.toAtlasBaseException;

/**
 * REST for a single entity
 */
@Path("v2/entity")
@Singleton
public class EntityREST {

    public static final String PREFIX_ATTR = "attr:";

    private final AtlasTypeRegistry         typeRegistry;
    private final AtlasInstanceConverter restAdapters;
    private final MetadataService           metadataService;
    private final AtlasEntityStore          entitiesStore;

    @Inject
    public EntityREST(AtlasTypeRegistry typeRegistry, AtlasInstanceConverter instanceConverter,
                      MetadataService metadataService, AtlasEntityStore entitiesStore) {
        this.typeRegistry    = typeRegistry;
        this.restAdapters    = instanceConverter;
        this.metadataService = metadataService;
        this.entitiesStore   = entitiesStore;
    }

    /**
     * Fetch complete definition of an entity given its GUID.
     * @param guid GUID for the entity
     * @return AtlasEntity
     * @throws AtlasBaseException
     */
    @GET
    @Path("/guid/{guid}")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntityWithExtInfo getById(@PathParam("guid") String guid) throws AtlasBaseException {
        return entitiesStore.getById(guid);
    }

    /**
     * Fetch complete definition of an entity given its type and unique attribute.
     * @param typeName
     * @return AtlasEntityWithExtInfo
     * @throws AtlasBaseException
     */
    @GET
    @Path("/uniqueAttribute/type/{typeName}")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntityWithExtInfo getByUniqueAttributes(@PathParam("typeName") String typeName,
                                                        @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        AtlasEntityType     entityType = ensureEntityType(typeName);
        Map<String, Object> attributes = getAttributes(servletRequest);

        validateUniqueAttribute(entityType, attributes);

        return entitiesStore.getByUniqueAttributes(entityType, attributes);
    }

    /**
     * Create new entity or update existing entity in Atlas.
     * Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
     * @param entity
     * @return EntityMutationResponse
     * @throws AtlasBaseException
     */
    @POST
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse createOrUpdate(AtlasEntityWithExtInfo entity) throws AtlasBaseException {
        return entitiesStore.createOrUpdate(new AtlasEntityStream(entity), false);
    }

    /*******
     * Entity Partial Update - Allows a subset of attributes to be updated on
     * an entity which is identified by its type and unique attribute  eg: Referenceable.qualifiedName.
     * Null updates are not possible
     *******/
    @PUT
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Path("/uniqueAttribute/type/{typeName}")
    public EntityMutationResponse partialUpdateByUniqueAttributes(@PathParam("typeName") String typeName,
                                                                  @Context HttpServletRequest servletRequest,
                                                                  AtlasEntity entity) throws Exception {
        AtlasEntityType     entityType       = ensureEntityType(typeName);
        Map<String, Object> uniqueAttributes = getAttributes(servletRequest);

        validateUniqueAttribute(entityType, uniqueAttributes);

        return entitiesStore.updateByUniqueAttributes(entityType, uniqueAttributes, entity);
    }

    /**
     * Delete an entity identified by its GUID.
     * @param  guid GUID for the entity
     * @return EntityMutationResponse
     */
    @DELETE
    @Path("guid/{guid}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse deleteByGuid(@PathParam("guid") final String guid) throws AtlasBaseException {
        return entitiesStore.deleteById(guid);
    }

    /**
     * Delete an entity identified by its type and unique attributes.
     * @param  typeName - entity type to be deleted
     * @param  servletRequest - request containing unique attributes/values
     * @return EntityMutationResponse
     */
    @DELETE
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Path("/uniqueAttribute/type/{typeName}")
    public EntityMutationResponse deleteByUniqueAttribute(@PathParam("typeName") String typeName,
                                                          @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        AtlasEntityType     entityType = ensureEntityType(typeName);
        Map<String, Object> attributes = getAttributes(servletRequest);

        return entitiesStore.deleteByUniqueAttributes(entityType, attributes);
    }

    /**
     * Gets the list of classifications for a given entity represented by a guid.
     * @param guid globally unique identifier for the entity
     * @return a list of classifications for the given entity guid
     */
    @GET
    @Path("/guid/{guid}/classification/{classificationName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasClassification getClassification(@PathParam("guid") String guid, @PathParam("classificationName") String typeName) throws AtlasBaseException {

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        ensureClassificationType(typeName);

        try {
            IStruct trait = metadataService.getTraitDefinition(guid, typeName);
            return restAdapters.getClassification(trait);

        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }

    /**
     * Gets the list of classifications for a given entity represented by a guid.
     * @param guid globally unique identifier for the entity
     * @return a list of classifications for the given entity guid
     */
    @GET
    @Path("/guid/{guid}/classifications")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasClassification.AtlasClassifications getClassifications(@PathParam("guid") String guid) throws AtlasBaseException {

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        AtlasClassification.AtlasClassifications clss = new AtlasClassification.AtlasClassifications();

        try {
            List<AtlasClassification> clsList = new ArrayList<>();
            for ( String traitName : metadataService.getTraitNames(guid) ) {
                IStruct trait = metadataService.getTraitDefinition(guid, traitName);
                AtlasClassification cls = restAdapters.getClassification(trait);
                clsList.add(cls);
            }

            clss.setList(clsList);

        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
        return clss;
    }

    /**
     * Adds classifications to an existing entity represented by a guid.
     * @param guid globally unique identifier for the entity
     */
    @POST
    @Path("/guid/{guid}/classifications")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void addClassifications(@PathParam("guid") final String guid, List<AtlasClassification> classifications) throws AtlasBaseException {

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        for (AtlasClassification classification:  classifications) {
            final ITypedStruct trait = restAdapters.getTrait(classification);
            try {
                metadataService.addTrait(guid, trait);
            } catch (IllegalArgumentException e) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, e);
            } catch (AtlasException e) {
                throw toAtlasBaseException(e);
            }
        }
    }

    /**
     * Update classification(s) for an entity represented by a guid.
     * Classifications are identified by their guid or name
     * @param guid globally unique identifier for the entity
     */
    @PUT
    @Path("/guid/{guid}/classifications")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void updateClassifications(@PathParam("guid") final String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        //Not supported in old API

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }
    }

    /**
     * Deletes a given classification from an existing entity represented by a guid.
     * @param guid      globally unique identifier for the entity
     * @param typeName name of the trait
     */
    @DELETE
    @Path("/guid/{guid}/classification/{classificationName}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteClassification(@PathParam("guid") String guid,
        @PathParam("classificationName") String typeName) throws AtlasBaseException {

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        ensureClassificationType(typeName);

        try {
            metadataService.deleteTrait(guid, typeName);
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }

    /******************************************************************/
    /** Bulk API operations                                          **/
    /******************************************************************/

    /**
     * Bulk API to retrieve list of entities identified by its GUIDs.
     */
    @GET
    @Path("/bulk")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntitiesWithExtInfo getByGuids(@QueryParam("guid") List<String> guids) throws AtlasBaseException {

        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guids);
        }

        AtlasEntitiesWithExtInfo entities = entitiesStore.getByIds(guids);

        return entities;
    }

    /**
     * Bulk API to create new entities or update existing entities in Atlas.
     * Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
     */
    @POST
    @Path("/bulk")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse createOrUpdate(AtlasEntitiesWithExtInfo entities) throws AtlasBaseException {

        EntityStream entityStream = new AtlasEntityStream(entities);

        return entitiesStore.createOrUpdate(entityStream, false);
    }

    /**
     * Bulk API to delete list of entities identified by its GUIDs
     */
    @DELETE
    @Path("/bulk")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse deleteByGuids(@QueryParam("guid") final List<String> guids) throws AtlasBaseException {
        return entitiesStore.deleteByIds(guids);
    }

    /**
     * Bulk API to associate a tag to multiple entities
     */
    @POST
    @Path("/bulk/classification")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void addClassification(ClassificationAssociateRequest request) throws AtlasBaseException {
        AtlasClassification classification = request == null ? null : request.getClassification();
        List<String>        entityGuids    = request == null ? null : request.getEntityGuids();

        if (classification == null || org.apache.commons.lang.StringUtils.isEmpty(classification.getTypeName())) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no classification");
        }

        if (CollectionUtils.isEmpty(entityGuids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "empty entity list");
        }

        final ITypedStruct trait = restAdapters.getTrait(classification);

        try {
            metadataService.addTrait(entityGuids, trait);
        } catch (IllegalArgumentException e) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, e);
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }

    private AtlasEntityType ensureEntityType(String typeName) throws AtlasBaseException {
        AtlasEntityType ret = typeRegistry.getEntityTypeByName(typeName);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), typeName);
        }

        return ret;
    }

    private AtlasClassificationType ensureClassificationType(String typeName) throws AtlasBaseException {
        AtlasClassificationType ret = typeRegistry.getClassificationTypeByName(typeName);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.CLASSIFICATION.name(), typeName);
        }

        return ret;
    }

    private Map<String, Object> getAttributes(HttpServletRequest request) {
        Map<String, Object> attributes = new HashMap<>();

        if (MapUtils.isNotEmpty(request.getParameterMap())) {
            for (Map.Entry<String, String[]> e : request.getParameterMap().entrySet()) {
                String key = e.getKey();

                if (key != null && key.startsWith(PREFIX_ATTR)) {
                    String[] values = e.getValue();
                    String   value  = values != null && values.length > 0 ? values[0] : null;

                    attributes.put(key.substring(PREFIX_ATTR.length()), value);
                }
            }
        }

        return attributes;
    }

    /**
     * Validate that each attribute given is an unique attribute
     * @param entityType the entity type
     * @param attributes attributes
     */
    private void validateUniqueAttribute(AtlasEntityType entityType, Map<String, Object> attributes) throws AtlasBaseException {
        if (MapUtils.isEmpty(attributes)) {
            throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID, entityType.getTypeName(), "");
        }

        for (String attributeName : attributes.keySet()) {
            AtlasAttributeDef attribute = entityType.getAttributeDef(attributeName);

            if (attribute == null || !attribute.getIsUnique()) {
                throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID, entityType.getTypeName(), attributeName);
            }
        }
    }
}
