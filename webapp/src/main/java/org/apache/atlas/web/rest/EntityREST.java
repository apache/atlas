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
import org.apache.atlas.CreateUpdateEntitiesResult;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.web.adapters.AtlasFormatConverter;
import org.apache.atlas.web.adapters.AtlasInstanceRestAdapters;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.web.adapters.AtlasInstanceRestAdapters.toAtlasBaseException;
import static org.apache.atlas.web.adapters.AtlasInstanceRestAdapters.toEntityMutationResponse;

/**
 * REST for a single entity
 */
@Path("v2/entity")
@Singleton
public class EntityREST {

    private static final Logger LOG = LoggerFactory.getLogger(EntityREST.class);

    public static final String PREFIX_ATTR = "attr:";

    private final AtlasTypeRegistry         typeRegistry;
    private final AtlasInstanceRestAdapters restAdapters;
    private final MetadataService           metadataService;
    private final AtlasEntityStore          entitiesStore;

    @Inject
    public EntityREST(AtlasTypeRegistry typeRegistry, AtlasInstanceRestAdapters restAdapters, MetadataService metadataService, AtlasEntityStore entitiesStore) {
        this.typeRegistry    = typeRegistry;
        this.restAdapters    = restAdapters;
        this.metadataService = metadataService;
        this.entitiesStore   = entitiesStore;
    }

    /**
     * Fetch the complete definition of an entity given its GUID.
     *
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
     * Get entity information using entity type and unique attribute.
     * @param typeName
     * @return
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

        validateAttributes(entityType, attributes);

        return entitiesStore.getByUniqueAttribute(entityType, attributes);
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

        AtlasFormatConverter.ConverterContext ctx = new AtlasFormatConverter.ConverterContext();
        ctx.addEntity(entity);
        Referenceable ref = restAdapters.getReferenceable(entity, ctx);
        CreateUpdateEntitiesResult result = metadataService.updateEntityByUniqueAttribute(entityType, attribute, value, ref);
        return toEntityMutationResponse(result);
    }

    @Deprecated
    @DELETE
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Path("/uniqueAttribute/type/{typeName}/attribute/{attrName}")
    public EntityMutationResponse deleteByUniqueAttribute(@PathParam("typeName") String typeName,
        @PathParam("attrName") String attribute,
        @QueryParam("value") String value) throws Exception {

        AtlasEntityType entityType = ensureEntityType(typeName);

        validateUniqueAttribute(entityType, attribute);

        final AtlasClient.EntityResult result = metadataService.deleteEntityByUniqueAttribute(typeName, attribute, value);
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
    public List<AtlasEntity> getByUniqueAttribute(@PathParam("typeName") String typeName,
        @PathParam("attrName") String attribute,
        @QueryParam("value") String value) throws AtlasBaseException {

        List<AtlasEntity> entityList = new ArrayList<>();
        AtlasEntityType                   entityType = ensureEntityType(typeName);

        validateUniqueAttribute(entityType, attribute);

        try {
            final ITypedReferenceableInstance entityDefinitionReference = metadataService.getEntityDefinitionReference(typeName, attribute, value);
            Map<String, AtlasEntity> entityRet = restAdapters.getAtlasEntity(entityDefinitionReference);
            entityList.addAll(entityRet.values());
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }

        return entityList;
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

    /**
     * Deprecated method - not used
     * Validate that attribute is unique attribute
     * @param entityType     the entity type
     * @param attributeName  the name of the attribute
     */
    private void validateUniqueAttribute(AtlasEntityType entityType, String attributeName) throws AtlasBaseException {
        AtlasAttributeDef attribute = entityType.getAttributeDef(attributeName);

        if (attribute == null || !attribute.getIsUnique()) {
            throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID, entityType.getTypeName(), attributeName);
        }
    }

    private List<AtlasEntity> getOrderedEntityList(Map<String, AtlasEntity> entities, String firstItemGuid) {
        List<AtlasEntity> ret = new ArrayList<>(entities.size());

        for (AtlasEntity entity : entities.values()) {
            if (StringUtils.equals(entity.getGuid(), firstItemGuid)) {
                ret.add(0, entity);
            } else {
                ret.add(entity);
            }
        }

        return ret;
    }

    private void validateAttributes(AtlasEntityType entityType, Map<String, Object> attributes)
            throws AtlasBaseException {

        if (MapUtils.isEmpty(attributes)) {
            throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID, entityType.getTypeName(), "");
        }

        for (String attrName : attributes.keySet()) {
            AtlasAttributeDef attrDef = entityType.getAttributeDef(attrName);

            if (attrDef == null || !attrDef.getIsUnique()) {
                throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID, entityType.getTypeName(), attrName);
            }
        }
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
}
