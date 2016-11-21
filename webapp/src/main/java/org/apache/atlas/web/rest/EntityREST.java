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

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.web.adapters.AtlasInstanceRestAdapters;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.web.adapters.AtlasInstanceRestAdapters.toAtlasBaseException;
import static org.apache.atlas.web.adapters.AtlasInstanceRestAdapters.toEntityMutationResponse;

/**
 * REST for a single entity
 */
@Path("v2/entity")
@Singleton
public class EntityREST {

    private static final Logger LOG = LoggerFactory.getLogger(EntityREST.class);

    private final AtlasTypeRegistry typeRegistry;

    private final AtlasInstanceRestAdapters restAdapters;

    private final MetadataService metadataService;

    @Inject
    public EntityREST(AtlasTypeRegistry typeRegistry, AtlasInstanceRestAdapters restAdapters, MetadataService metadataService) {
        this.typeRegistry = typeRegistry;
        this.restAdapters = restAdapters;
        this.metadataService = metadataService;
    }

    /**
     * Create or Update an entity if it  already exists
     *
     * @param entity The updated entity
     * @return
     */
    @POST
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse createOrUpdate(final AtlasEntity entity) throws AtlasBaseException {
        EntityMutationResponse response = null;
        ITypedReferenceableInstance[] entitiesInOldFormat = restAdapters.getITypedReferenceables(new ArrayList<AtlasEntity>() {{ add(entity); }});

        try {
            final AtlasClient.EntityResult result = metadataService.updateEntities(entitiesInOldFormat);
            response = toEntityMutationResponse(result);
        } catch (AtlasException e) {
            LOG.error("Exception while getting a typed reference for the entity ", e);
            throw AtlasInstanceRestAdapters.toAtlasBaseException(e);
        }
        return response;
    }

    /**
     * Complete Update of an entity identified by its GUID
     *
     * @param guid
     * @param entity The updated entity
     * @return
     */
    @PUT
    @Path("guid/{guid}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse updateByGuid(@PathParam("guid") String guid, AtlasEntity entity, @DefaultValue("false") @QueryParam("partialUpdate") boolean partialUpdate) throws AtlasBaseException {
        return createOrUpdate(entity);
    }


    /**
     * Fetch the complete definition of an entity given its GUID.
     *
     * @param guid GUID for the entity
     */
    @GET
    @Path("/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntity getById(@PathParam("guid") String guid) throws AtlasBaseException {
        try {
            ITypedReferenceableInstance ref = metadataService.getEntityDefinition(guid);
            return restAdapters.getAtlasEntity(ref);
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }

    /**
     * Fetch the complete definition of an entity given its GUID including its associations
     * like classifications, terms etc.
     *
     * @param guid GUID for the entity
     */
    @GET
    @Path("/guid/{guid}/associations")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntityWithAssociations getWithAssociationsByGuid(@PathParam("guid") String guid) throws AtlasBaseException {
        try {
            ITypedReferenceableInstance ref = metadataService.getEntityDefinition(guid);
            return restAdapters.getAtlasEntity(ref);
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }



    /**
     * Delete an entity identified by its GUID
     *
     * @param guid
     * @return
     */
    @DELETE
    @Path("guid/{guid}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse deleteByGuid(@PathParam("guid") final String guid) throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }
        try {
            AtlasClient.EntityResult result = metadataService.deleteEntities(new ArrayList<String>() {{ add(guid); }});
            return toEntityMutationResponse(result);
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }


    /*******
     * Entity Partial Update - Allows a subset of attributes to be updated on
     * an entity which is identified by its type and unique attribute  eg: Referenceable.qualifiedName.
     * Null updates are not possible
     *******/

    @Deprecated
    @PUT
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Path("/uniqueAttribute/type/{typeName}/attribute/{attrName}")
    public EntityMutationResponse partialUpdateByUniqueAttribute(@PathParam("typeName") String entityType,
        @PathParam("attrName") String attribute,
        @QueryParam("value") String value, AtlasEntity entity) throws Exception {

        AtlasEntityType type = (AtlasEntityType) validateType(entityType, TypeCategory.ENTITY);
        validateUniqueAttribute(type, attribute);

        Referenceable ref = restAdapters.getReferenceable(entity);
        AtlasClient.EntityResult result = metadataService.updateEntityByUniqueAttribute(entityType, attribute, value, ref);
        return toEntityMutationResponse(result);
    }

    @Deprecated
    @DELETE
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Path("/uniqueAttribute/type/{typeName}/attribute/{attrName}")
    public EntityMutationResponse deleteByUniqueAttribute(@PathParam("typeName") String entityType,
        @PathParam("attrName") String attribute,
        @QueryParam("value") String value) throws Exception {

        AtlasEntityType type = (AtlasEntityType) validateType(entityType, TypeCategory.ENTITY);
        validateUniqueAttribute(type, attribute);

        final AtlasClient.EntityResult result = metadataService.deleteEntityByUniqueAttribute(entityType, attribute, value);
        return toEntityMutationResponse(result);
    }

    /**
     * Fetch the complete definition of an entity
     * which is identified by its type and unique attribute  eg: Referenceable.qualifiedName.
     */
    @Deprecated
    @GET
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Path("/uniqueAttribute/type/{typeName}/attribute/{attrName}")
    public AtlasEntity getByUniqueAttribute(@PathParam("typeName") String entityType,
        @PathParam("attrName") String attribute,
        @QueryParam("value") String value) throws AtlasBaseException {

        AtlasEntityType type = (AtlasEntityType) validateType(entityType, TypeCategory.ENTITY);
        validateUniqueAttribute(type, attribute);

        try {
            final ITypedReferenceableInstance entityDefinitionReference = metadataService.getEntityDefinitionReference(entityType, attribute, value);
            return restAdapters.getAtlasEntity(entityDefinitionReference);
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }


    /**
     * Gets the list of classifications for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of classifications for the given entity guid
     */
    @GET
    @Path("/guid/{guid}/classification/{classificationName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasClassification getClassification(@PathParam("guid") String guid, @PathParam("classificationName") String classificationName) throws AtlasBaseException {

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        validateType(classificationName, TypeCategory.CLASSIFICATION);

        try {
            IStruct trait = metadataService.getTraitDefinition(guid, classificationName);
            return restAdapters.getClassification(trait);

        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }


    /**
     * Gets the list of classifications for a given entity represented by a guid.
     *
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
     * Classification management
     */

    /**
     * Adds classifications to an existing entity represented by a guid.
     *
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
     *
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
     *
     * @param guid      globally unique identifier for the entity
     * @param classificationName name of the trait
     */
    @DELETE
    @Path("/guid/{guid}/classification/{classificationName}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteClassification(@PathParam("guid") String guid,
        @PathParam("classificationName") String classificationName) throws AtlasBaseException {

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        validateType(classificationName, TypeCategory.CLASSIFICATION);

        try {
            metadataService.deleteTrait(guid, classificationName);
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }

    private AtlasType validateType(String entityType, TypeCategory expectedCategory) throws AtlasBaseException {
        if ( StringUtils.isEmpty(entityType) ) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, entityType);
        }

        AtlasType type = typeRegistry.getType(entityType);
        if (type.getTypeCategory() != expectedCategory) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, type.getTypeCategory().name(), expectedCategory.name());
        }

        return type;
    }

    /**
     * Validate that attribute is unique attribute
     * @param entityType     the entity type
     * @param attributeName  the name of the attribute
     */
    private void validateUniqueAttribute(AtlasEntityType entityType, String attributeName) throws AtlasBaseException {
        AtlasStructDef.AtlasAttributeDef attribute = entityType.getAttributeDef(attributeName);
        if (attribute != null && !attribute.getIsUnique()) {
            throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID, entityType.getTypeName(), attributeName);
        }
    }
}
