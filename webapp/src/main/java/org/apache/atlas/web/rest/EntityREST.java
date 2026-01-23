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

import com.google.common.collect.Lists;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.authorize.*;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.bulkimport.BulkImportResponse;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.audit.AuditSearchParams;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.audit.ESBasedAuditRepository;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.*;
import org.apache.atlas.repository.store.graph.v2.repair.AtlasRepairAttributeService;
import org.apache.atlas.repository.store.graph.v2.tags.PaginatedVertexIdResult;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.FileUtils;
import org.apache.atlas.util.RepairIndex;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.DEPRECATED_API;
import static org.apache.atlas.authorize.AtlasPrivilege.*;


/**
 * REST for a single entity
 */
@Path("entity")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class EntityREST {
    private static final Logger LOG      = LoggerFactory.getLogger(EntityREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.EntityREST");

    public static final String PREFIX_ATTR  = "attr:";
    public static final String PREFIX_ATTR_ = "attr_";
    private static final int HUNDRED_THOUSAND = 100000;
    private static final int TWO_MILLION = HUNDRED_THOUSAND * 10 * 2;
    private static  final int  ENTITIES_ALLOWED_IN_BULK = AtlasConfiguration.ATLAS_BULK_API_MAX_ENTITIES_ALLOWED.getInt();
    private static final Set<String> ATTRS_WITH_TWO_MILLION_LIMIT = Arrays.stream(AtlasConfiguration.ATLAS_ENTITIES_ATTRIBUTE_ALLOWED_LARGE_ATTRIBUTES
            .getStringArray())
            .collect(Collectors.toSet());


    private final AtlasTypeRegistry      typeRegistry;
    private final AtlasEntityStore       entitiesStore;
    private final ESBasedAuditRepository  esBasedAuditRepository;
    private final EntityGraphRetriever entityGraphRetriever;
    private final EntityMutationService entityMutationService;
    private final AtlasRepairAttributeService repairAttributeService;
    private final RepairIndex repairIndex;

    @Inject
    public EntityREST(AtlasTypeRegistry typeRegistry, AtlasEntityStore entitiesStore, ESBasedAuditRepository  esBasedAuditRepository, EntityGraphRetriever retriever, EntityMutationService entityMutationService, AtlasRepairAttributeService repairAttributeService, RepairIndex repairIndex) {
        this.typeRegistry      = typeRegistry;
        this.entitiesStore     = entitiesStore;
        this.esBasedAuditRepository = esBasedAuditRepository;
        this.entityGraphRetriever = retriever;
        this.entityMutationService = entityMutationService;
        this.repairAttributeService = repairAttributeService;
        this.repairIndex = repairIndex;
    }

    /**
     * Fetch complete definition of an entity given its GUID.
     * @param guid GUID for the entity
     * @return AtlasEntity
     * @throws AtlasBaseException
     */
    @GET
    @Path("/guid/{guid}")
    @Timed
    public AtlasEntityWithExtInfo getById(@PathParam("guid") String guid, @QueryParam("minExtInfo") @DefaultValue("false") boolean minExtInfo, @QueryParam("ignoreRelationships") @DefaultValue("false") boolean ignoreRelationships) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getById(" + guid + ", " + minExtInfo + " )");
            }

            return entitiesStore.getById(guid, minExtInfo, ignoreRelationships);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk API to create new entities or updates existing entities in Atlas.
     * Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
     */
    @POST
    @Path("/evaluator")
    public List<AtlasEvaluatePolicyResponse> evaluatePolicies(List<AtlasEvaluatePolicyRequest> entities) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        List<AtlasEvaluatePolicyResponse> response = new ArrayList();

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.evaluatePolicies()");
            }
            response = entitiesStore.evaluatePolicies(entities);
        } finally {
            AtlasPerfTracer.log(perf);
        }

        return response;
    }

    /**
     * API to get accessors info such as roles/groups/users who can perform specific action
     */
    @POST
    @Path("/accessors")
    public List<AtlasAccessorResponse> getAccessors(List<AtlasAccessorRequest> atlasAccessorRequestList) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        List<AtlasAccessorResponse> ret;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getAccessorss()");
        }

        try {
            validateAccessorRequest(atlasAccessorRequestList);

            ret = entitiesStore.getAccessors(atlasAccessorRequestList);

        } finally {
            AtlasPerfTracer.log(perf);
        }
        return ret;
    }


    /**
     * Get entity header given its GUID.
     * @param guid GUID for the entity
     * @return AtlasEntity
     * @throws AtlasBaseException
     */
    @GET
    @Path("/guid/{guid}/header")
    @Timed
    public AtlasEntityHeader getHeaderById(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getHeaderById(" + guid + ")");
            }

            return entitiesStore.getHeaderById(guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Fetch AtlasEntityHeader given its type and unique attribute.
     *
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     *
     * attr:<attrName>=<attrValue>
     *
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     *
     * The REST request would look something like this
     *
     * GET /v2/entity/uniqueAttribute/type/aType/header?attr:aTypeAttribute=someValue
     *
     * @param typeName
     * @return AtlasEntityHeader
     * @throws AtlasBaseException
     */
    @GET
    @Path("/uniqueAttribute/type/{typeName}/header")
    @Timed
    public AtlasEntityHeader getEntityHeaderByUniqueAttributes(@PathParam("typeName") String typeName,
                                                               @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            Map<String, Object> attributes = getAttributes(servletRequest);
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getEntityHeaderByUniqueAttributes(" + typeName + "," + attributes + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            validateUniqueAttribute(entityType, attributes);

            return entitiesStore.getEntityHeaderByUniqueAttributes(entityType, attributes);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Fetch complete definition of an entity given its type and unique attribute.
     *
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     *
     * attr:<attrName>=<attrValue>
     *
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     *
     * The REST request would look something like this
     *
     * GET /v2/entity/uniqueAttribute/type/aType?attr:aTypeAttribute=someValue
     *
     * @param typeName
     * @param minExtInfo
     * @param ignoreRelationships
     * @return AtlasEntityWithExtInfo
     * @throws AtlasBaseException
     */
    @GET
    @Path("/uniqueAttribute/type/{typeName}")
    @Timed
    public AtlasEntityWithExtInfo getByUniqueAttributes(@PathParam("typeName") String typeName, @QueryParam("minExtInfo") @DefaultValue("false") boolean minExtInfo,
                                                        @QueryParam("ignoreRelationships") @DefaultValue("false") boolean ignoreRelationships, @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            Map<String, Object> attributes = getAttributes(servletRequest);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getByUniqueAttributes(" + typeName + "," + attributes + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            validateUniqueAttribute(entityType, attributes);

            return entitiesStore.getByUniqueAttributes(entityType, attributes, minExtInfo, ignoreRelationships);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /*******
     * Entity Partial Update - Allows a subset of attributes to be updated on
     * an entity which is identified by its type and unique attribute  eg: Referenceable.qualifiedName.
     * Null updates are not possible
     *
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     *
     * attr:<attrName>=<attrValue>
     *
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     *
     * The REST request would look something like this
     *
     * PUT /v2/entity/uniqueAttribute/type/aType?attr:aTypeAttribute=someValue
     *
     *******/
    @PUT
    @Path("/uniqueAttribute/type/{typeName}")
    @Timed
    public EntityMutationResponse partialUpdateEntityByUniqueAttrs(@PathParam("typeName") String typeName,
                                                                   @Context HttpServletRequest servletRequest,
                                                                   AtlasEntityWithExtInfo entityInfo) throws Exception {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            Map<String, Object> uniqueAttributes = getAttributes(servletRequest);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.partialUpdateEntityByUniqueAttrs(" + typeName + "," + uniqueAttributes + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            validateUniqueAttribute(entityType, uniqueAttributes);

            return entityMutationService.updateByUniqueAttributes(entityType, uniqueAttributes, entityInfo);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete an entity identified by its type and unique attributes.
     *
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     *
     * attr:<attrName>=<attrValue>
     *
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     *
     * The REST request would look something like this
     *
     * DELETE /v2/entity/uniqueAttribute/type/aType?attr:aTypeAttribute=someValue
     *
     * @param  typeName - entity type to be deleted
     * @param  servletRequest - request containing unique attributes/values
     * @return EntityMutationResponse
     */
    @DELETE
    @Path("/uniqueAttribute/type/{typeName}")
    @Timed
    public EntityMutationResponse deleteByUniqueAttribute(@PathParam("typeName") String typeName,
                                                          @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            Map<String, Object> attributes = getAttributes(servletRequest);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteByUniqueAttribute(" + typeName + "," + attributes + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            return entityMutationService.deleteByUniqueAttributes(entityType, attributes);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Create new entity or update existing entity in Atlas.
     * Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
     * @param entity
     * @return EntityMutationResponse
     * @throws AtlasBaseException
     */
    @POST
    @Timed
    public EntityMutationResponse createOrUpdate(AtlasEntityWithExtInfo entity,
                                                 @QueryParam("replaceClassifications") @DefaultValue("false") boolean replaceClassifications,
                                                 @QueryParam("replaceBusinessAttributes") @DefaultValue("false") boolean replaceBusinessAttributes,
                                                 @QueryParam("overwriteBusinessAttributes") @DefaultValue("false") boolean isOverwriteBusinessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.createOrUpdate()");
            }
            validateAttributeLength(Lists.newArrayList(entity.getEntity()));

            BulkRequestContext context = new BulkRequestContext.Builder()
                    .setReplaceClassifications(replaceClassifications)
                    .setReplaceBusinessAttributes(replaceBusinessAttributes)
                    .setOverwriteBusinessAttributes(isOverwriteBusinessAttributes)
                    .build();
            return entityMutationService.createOrUpdate(new AtlasEntityStream(entity), context);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /*******
     * Entity Partial Update - Add/Update entity attribute identified by its GUID.
     * Supports only uprimitive attribute type and entity references.
     * does not support updation of complex types like arrays, maps
     * Null updates are not possible
     *******/
    @PUT
    @Path("/guid/{guid}")
    @Timed
    public EntityMutationResponse partialUpdateEntityAttrByGuid(@PathParam("guid") String guid,
                                                                @QueryParam("name") String attrName,
                                                                Object attrValue) throws Exception {
        Servlets.validateQueryParamLength("guid", guid);
        Servlets.validateQueryParamLength("name", attrName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.partialUpdateEntityAttrByGuid(" + guid + "," + attrName + ")");
            }

            return entitiesStore.updateEntityAttributeByGuid(guid, attrName, attrValue);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete an entity identified by its GUID.
     * @param  guid GUID for the entity
     * @return EntityMutationResponse
     */
    @DELETE
    @Path("/guid/{guid}")
    @Timed
    public EntityMutationResponse deleteByGuid(@PathParam("guid") final String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteByGuid(" + guid + ")");
            }

            return entityMutationService.deleteById(guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Gets the list of classifications for a given entity represented by a guid.
     * @param guid globally unique identifier for the entity
     * @return classification for the given entity guid
     */
    @GET
    @Path("/guid/{guid}/classification/{classificationName}")
    @Timed
    public AtlasClassification getClassification(@PathParam("guid") String guid, @PathParam("classificationName") final String classificationName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);
        Servlets.validateQueryParamLength("classificationName", classificationName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getClassification(" + guid + "," + classificationName + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            ensureClassificationType(classificationName);
            return entitiesStore.getClassification(guid, classificationName);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Gets the list of classifications for a given entity represented by a guid.
     * @param guid globally unique identifier for the entity
     * @return a list of classifications for the given entity guid
     */
    @GET
    @Path("/guid/{guid}/classifications")
    @Timed
    public AtlasClassification.AtlasClassifications getClassifications(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getClassifications(" + guid + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            return new AtlasClassification.AtlasClassifications(entitiesStore.getClassifications(guid));
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Adds classification to the entity identified by its type and unique attributes.
     * @param typeName
     */
    @POST
    @Path("/uniqueAttribute/type/{typeName}/classifications")
    @Timed
    public void addClassificationsByUniqueAttribute(@PathParam("typeName") String typeName, @Context HttpServletRequest servletRequest, List<AtlasClassification> classifications) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addClassificationsByUniqueAttribute(" + typeName + ")");
            }

            AtlasEntityType     entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String              guid       = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entityMutationService.addClassifications(guid, classifications);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Adds classifications to an existing entity represented by a guid.
     * @param guid globally unique identifier for the entity
     */
    @POST
    @Path("/guid/{guid}/classifications")
    @Timed
    public void addClassifications(@PathParam("guid") final String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addClassifications(" + guid + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            entityMutationService.addClassifications(guid, classifications);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Updates classification on an entity identified by its type and unique attributes.
     * @param  typeName
     */
    @PUT
    @Path("/uniqueAttribute/type/{typeName}/classifications")
    @Timed
    public void updateClassificationsByUniqueAttribute(@PathParam("typeName") String typeName, @Context HttpServletRequest servletRequest, List<AtlasClassification> classifications) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.updateClassificationsByUniqueAttribute(" + typeName + ")");
            }

            AtlasEntityType     entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String              guid       = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entityMutationService.updateClassifications(guid, classifications);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Updates classifications to an existing entity represented by a guid.
     * @param  guid globally unique identifier for the entity
     * @return classification for the given entity guid
     */
    @PUT
    @Path("/guid/{guid}/classifications")
    @Timed
    public void updateClassifications(@PathParam("guid") final String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.updateClassifications(" + guid + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            entityMutationService.updateClassifications(guid, classifications);

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Deletes a given classification from an entity identified by its type and unique attributes.
     * @param typeName
     * @param classificationName name of the classification
     */
    @DELETE
    @Path("/uniqueAttribute/type/{typeName}/classification/{classificationName}")
    @Timed
    public void deleteClassificationByUniqueAttribute(@PathParam("typeName") String typeName, @Context HttpServletRequest servletRequest,@PathParam("classificationName") String classificationName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        Servlets.validateQueryParamLength("classificationName", classificationName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteClassificationByUniqueAttribute(" + typeName + ")");
            }

            AtlasEntityType     entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String              guid       = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entityMutationService.deleteClassification(guid, classificationName);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Deletes a given classification from an existing entity represented by a guid.
     * @param guid      globally unique identifier for the entity
     * @param classificationName name of the classifcation
     */
    @DELETE
    @Path("/guid/{guid}/classification/{classificationName}")
    @Timed
    public void deleteClassification(@PathParam("guid") String guid,
                                     @PathParam("classificationName") final String classificationName,
                                     @QueryParam("associatedEntityGuid") final String associatedEntityGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);
        Servlets.validateQueryParamLength("classificationName", classificationName);
        Servlets.validateQueryParamLength("associatedEntityGuid", associatedEntityGuid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteClassification(" + guid + "," + classificationName + "," + associatedEntityGuid + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            ensureClassificationType(classificationName);

            entityMutationService.deleteClassification(guid, classificationName, associatedEntityGuid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /******************************************************************/
    /** Bulk API operations                                          **/
    /******************************************************************/

    /**
     * Bulk API to retrieve list of entities identified by its unique attributes.
     *
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     *
     * typeName=<typeName>&attr_1:<attrName>=<attrValue>&attr_2:<attrName>=<attrValue>&attr_3:<attrName>=<attrValue>
     *
     * NOTE: The attrName should be an unique attribute for the given entity-type
     *
     * The REST request would look something like this
     *
     * GET /v2/entity/bulk/uniqueAttribute/type/hive_db?attr_0:qualifiedName=db1@cl1&attr_2:qualifiedName=db2@cl1
     *
     * @param typeName
     * @param minExtInfo
     * @param ignoreRelationships
     * @return AtlasEntitiesWithExtInfo
     * @throws AtlasBaseException
     */
    @GET
    @Path("/bulk/uniqueAttribute/type/{typeName}")
    @Timed
    public AtlasEntitiesWithExtInfo getEntitiesByUniqueAttributes(@PathParam("typeName") String typeName,
                                                                  @QueryParam("minExtInfo") @DefaultValue("false") boolean minExtInfo,
                                                                  @QueryParam("ignoreRelationships") @DefaultValue("false") boolean ignoreRelationships,
                                                                  @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            List<Map<String, Object>> uniqAttributesList = getAttributesList(servletRequest);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getEntitiesByUniqueAttributes(" + typeName + "," + uniqAttributesList + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            for (Map<String, Object> uniqAttributes : uniqAttributesList) {
                validateUniqueAttribute(entityType, uniqAttributes);
            }

            return entitiesStore.getEntitiesByUniqueAttributes(entityType, uniqAttributesList, minExtInfo, ignoreRelationships);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk API to retrieve list of entities identified by its GUIDs.
     */
    @GET
    @Path("/bulk")
    @Timed
    public AtlasEntitiesWithExtInfo getByGuids(@QueryParam("guid") List<String> guids, @QueryParam("minExtInfo") @DefaultValue("false") boolean minExtInfo, @QueryParam("ignoreRelationships") @DefaultValue("false") boolean ignoreRelationships) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(guids)) {
            for (String guid : guids) {
                Servlets.validateQueryParamLength("guid", guid);
            }
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getByGuids(" + guids + ")");
            }

            if (CollectionUtils.isEmpty(guids)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guids);
            }

            return entitiesStore.getByIds(guids, minExtInfo, ignoreRelationships);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk API to create new entities or updates existing entities in Atlas.
     * Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
     */
    @POST
    @Path("/bulk")
    @Timed
    public EntityMutationResponse createOrUpdate(AtlasEntitiesWithExtInfo entities,
                                                 @QueryParam("replaceClassifications") @DefaultValue("false") boolean replaceClassifications,
                                                 @QueryParam("replaceTags") @DefaultValue("false") boolean replaceTags,
                                                 @QueryParam("appendTags") @DefaultValue("false") boolean appendTags,
                                                 @QueryParam("replaceBusinessAttributes") @DefaultValue("false") boolean replaceBusinessAttributes,
                                                 @QueryParam("overwriteBusinessAttributes") @DefaultValue("false") boolean isOverwriteBusinessAttributes,
                                                 @QueryParam("skipProcessEdgeRestoration") @DefaultValue("false") boolean skipProcessEdgeRestoration
    ) throws AtlasBaseException {

        if (Stream.of(replaceClassifications, replaceTags, appendTags).filter(flag -> flag).count() > 1) {
            throw new AtlasBaseException(BAD_REQUEST, "Only one of [replaceClassifications, replaceTags, appendTags] can be true");
        }

        AtlasPerfTracer perf = null;
        RequestContext.get().setSkipProcessEdgeRestoration(skipProcessEdgeRestoration);
        try {

            if (CollectionUtils.isEmpty(entities.getEntities())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no entities to create/update.");
            }
            int entitiesCount = entities.getEntities().size();

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.createOrUpdate(entityCount=" + entitiesCount+ ")");
            }

            if( entitiesCount > ENTITIES_ALLOWED_IN_BULK) {
                RequestContext.get().endMetricRecord(RequestContext.get().startMetricRecord("requestThrottledDueToBulkEntityOperation"));
                throw new AtlasBaseException(AtlasErrorCode.EXCEEDED_MAX_ENTITIES_ALLOWED, String.valueOf(ENTITIES_ALLOWED_IN_BULK));
            }

            validateAttributeLength(entities.getEntities());

            EntityStream entityStream = new AtlasEntityStream(entities);

            BulkRequestContext context = new BulkRequestContext.Builder()
                    .setReplaceClassifications(replaceClassifications)
                    .setReplaceTags(replaceTags)
                    .setAppendTags(appendTags)
                    .setReplaceBusinessAttributes(replaceBusinessAttributes)
                    .setOverwriteBusinessAttributes(isOverwriteBusinessAttributes)
                    .build();
            return entityMutationService.createOrUpdate(entityStream, context);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }


    public static void validateAttributeLength(final List<AtlasEntity> entities) throws AtlasBaseException {
        List<String> errorMessages = new ArrayList<>();

        for (final AtlasEntity atlasEntity : entities) {
            for (Map.Entry<String, Object> attribute : atlasEntity.getAttributes().entrySet()) {

                if (attribute.getValue() instanceof String && ((String) attribute.getValue()).length() > HUNDRED_THOUSAND) {

                    if (ATTRS_WITH_TWO_MILLION_LIMIT.contains(attribute.getKey())) {
                        if (((String) attribute.getValue()).length() > TWO_MILLION) {
                            errorMessages.add("Attribute " + attribute.getKey() + " exceeds limit of " + TWO_MILLION + " characters");
                        }
                    } else {
                        errorMessages.add("Attribute " + attribute.getKey() + " exceeds limit of " + HUNDRED_THOUSAND + " characters");
                    }
                }
            }

            if (errorMessages.size() > 0) {
                throw new AtlasBaseException(AtlasType.toJson(errorMessages));
            }
        }
    }

    /**
     * Bulk API to delete list of entities identified by its GUIDs
     */
    @DELETE
    @Path("/bulk")
    @Timed
    public EntityMutationResponse deleteByGuids(@QueryParam("guid") final List<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(guids)) {
            for (String guid : guids) {
                Servlets.validateQueryParamLength("guid", guid);
            }
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteByGuids(" + guids  + ")");
            }

            return entityMutationService.deleteByIds(guids);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @DELETE
    @Path("/bulk/uniqueAttribute")
    @Timed
    public EntityMutationResponse bulkDeleteByUniqueAttribute(List<AtlasObjectId> objectIds, @QueryParam("skipHasLineageCalculation") @DefaultValue("false") boolean skipHasLineageCalculation) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        RequestContext.get().setSkipHasLineageCalculation(skipHasLineageCalculation);
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.bulkDeleteByUniqueAttribute(" + objectIds.size() + ")");
            }

            return entityMutationService.deleteByUniqueAttributes(objectIds);

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }


    /**
     * Bulk API to restore list of entities identified by its GUIDs
     */
    @POST
    @Path("/restore/bulk")
    public EntityMutationResponse restoreByGuids(@QueryParam("guid") final List<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(guids)) {
            for (String guid : guids) {
                Servlets.validateQueryParamLength("guid", guid);
            }
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.restoreByGuids(" + guids  + ")");
            }

            return entityMutationService.restoreByIds(guids);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk API to associate a tag to multiple entities
     */
    @POST
    @Path("/bulk/classification")
    @Timed
    public void addClassification(ClassificationAssociateRequest request) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addClassification(" + request  + ")");
            }

            AtlasClassification       classification           = request == null ? null : request.getClassification();
            List<String>              entityGuids              = request == null ? null : request.getEntityGuids();
            List<Map<String, Object>> entitiesUniqueAttributes = request == null ? null : request.getEntitiesUniqueAttributes();
            String                    entityTypeName           = request == null ? null : request.getEntityTypeName();

            if (classification == null || StringUtils.isEmpty(classification.getTypeName())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no classification");
            }

            if (hasNoGUIDAndTypeNameAttributes(request)) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Need either list of GUIDs or entity type and list of qualified Names");
            }

            if (CollectionUtils.isNotEmpty(entitiesUniqueAttributes) && entityTypeName != null) {
                AtlasEntityType entityType = ensureEntityType(entityTypeName);

                if (CollectionUtils.isEmpty(entityGuids)) {
                    entityGuids = new ArrayList<>();
                }

                for (Map<String, Object> eachEntityAttributes : entitiesUniqueAttributes) {
                    try {
                        String guid = entitiesStore.getGuidByUniqueAttributes(entityType, eachEntityAttributes);

                        if (guid != null) {
                            entityGuids.add(guid);
                        }
                    } catch (AtlasBaseException e) {
                        if (RequestContext.get().isSkipFailedEntities()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("getByIds(): ignoring failure for entity with unique attributes {} and typeName {}: error code={}, message={}", eachEntityAttributes, entityTypeName, e.getAtlasErrorCode(), e.getMessage());
                            }

                            continue;
                        }

                        throw e;
                    }
                }

                if (CollectionUtils.isEmpty(entityGuids)) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "No guid found for given entity Type Name and list of attributes");
                }
            }

            entityMutationService.addClassification(entityGuids, classification);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk API to associate tags to multiple entities
     */
    @POST
    @Path("/bulk/classification/displayName")
    @Timed
    public void addClassificationByDisplayName(List<AtlasClassification> classificationList) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addClassificationByDisplayName()");
            }

            if (CollectionUtils.isEmpty(classificationList)) {
                throw new AtlasBaseException(BAD_REQUEST, "classification list should be specified");
            }

            Map<String, List<AtlasClassification>> entityGuidClassificationMap = new HashMap<>();
            for (AtlasClassification classification : classificationList) {

                if (StringUtils.isEmpty(classification.getTypeName())) {

                    try {
                        AtlasType type = typeRegistry.getClassificationTypeByDisplayName(classification.getDisplayName());
                        if (type == null) {
                            throw new AtlasBaseException("Classification type not found for displayName {}" + classification.getDisplayName());
                        }
                        classification.setTypeName(type.getTypeName());
                    } catch (NoSuchElementException exception) {
                        throw new AtlasBaseException("No Classification type fount for displayName " + classification.getDisplayName());
                    }
                }

                if (entityGuidClassificationMap.containsKey(classification.getEntityGuid())) {
                    List<AtlasClassification> classifications = entityGuidClassificationMap.get(classification.getEntityGuid());
                    classifications.add(classification);
                } else {
                    List<AtlasClassification> classifications = new ArrayList<>();
                    classifications.add(classification);
                    entityGuidClassificationMap.put(classification.getEntityGuid(), classifications);
                }
            }

            for (Map.Entry<String, List<AtlasClassification>> x : entityGuidClassificationMap.entrySet()) {
                entityMutationService.addClassifications(x.getKey(), x.getValue());
            }

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("{guid}/audit")
    @Timed
    public List<EntityAuditEventV2> getAuditEvents(@PathParam("guid") String guid, @QueryParam("startKey") String startKey,
                                                   @QueryParam("auditAction") EntityAuditActionV2 auditAction,
                                                   @QueryParam("count") @DefaultValue("100") short count,
                                                   @QueryParam("offset") @DefaultValue("-1") int offset,
                                                   @QueryParam("sortBy") String sortBy,
                                                   @QueryParam("sortOrder") String sortOrder) throws AtlasBaseException {
        throw new AtlasBaseException(DEPRECATED_API, "/entity/auditSearch");
    }

    @POST
    @Path("{guid}/auditSearch")
    @Timed
    public EntityAuditSearchResult searchAuditEventsByGuid(AuditSearchParams parameters, @PathParam("guid") String guid) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getAuditEvents(" + guid +  ")");
            }

            // Enforces authorization for entity-read
            try {
                entitiesStore.getHeaderById(guid);
            } catch (AtlasBaseException e) {
                if (e.getAtlasErrorCode() == AtlasErrorCode.INSTANCE_GUID_NOT_FOUND) {
                    AtlasEntityHeader entityHeader = getEntityHeaderFromPurgedAudit(guid);

                    AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, ENTITY_READ, entityHeader), "read entity audit: guid=", guid);
                } else {
                    throw e;
                }
            }

            String dslString = parameters.getQueryStringForGuid(guid);

            EntityAuditSearchResult ret = esBasedAuditRepository.searchEvents(dslString);

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("auditSearch")
    @Timed
    public EntityAuditSearchResult searchAuditEvents(AuditSearchParams parameters) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            String dslString = parameters.getQueryString();

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.searchAuditEvents(" + dslString + ")");
            }

            EntityAuditSearchResult ret = esBasedAuditRepository.searchEvents(dslString);

            scrubAndSetEntityAudits(ret, parameters.getSuppressLogs(), parameters.getAttributes());

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private void scrubAndSetEntityAudits(EntityAuditSearchResult result, boolean suppressLogs, Set<String> attributes) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("scrubEntityAudits");
        for (EntityAuditEventV2 event : result.getEntityAudits()) {
            try {
                AtlasSearchResult ret = new AtlasSearchResult();
                AtlasEntityWithExtInfo entityWithExtInfo = entitiesStore.getByIdWithoutAuthorization(event.getEntityId());
                AtlasEntityHeader entityHeader = new AtlasEntityHeader(entityWithExtInfo.getEntity());
                ret.addEntity(entityHeader);
                AtlasSearchResultScrubRequest request = new AtlasSearchResultScrubRequest(typeRegistry, ret);
                AtlasAuthorizationUtils.scrubSearchResults(request, suppressLogs);
                if(entityHeader.getScrubbed()!= null && entityHeader.getScrubbed()){
                    event.setDetail(null);
                }
                Map<String, Object> entityAttrs = entityHeader.getAttributes();
                if(attributes == null) entityAttrs.clear();
                else entityAttrs.keySet().retainAll(attributes);

                event.setEntityDetail(entityHeader);

            } catch (AtlasBaseException e) {
                if (e.getAtlasErrorCode() == AtlasErrorCode.INSTANCE_GUID_NOT_FOUND) {
                    try {
                        AtlasEntityHeader entityHeader = event.getEntityHeader();
                        AtlasSearchResult ret = new AtlasSearchResult();
                        ret.addEntity(entityHeader);
                        AtlasSearchResultScrubRequest request = new AtlasSearchResultScrubRequest(typeRegistry, ret);
                        AtlasAuthorizationUtils.scrubSearchResults(request, suppressLogs);
                        if(entityHeader.getScrubbed()!= null && entityHeader.getScrubbed()){
                            event.setDetail(null);
                        }
                    } catch (AtlasBaseException abe) {
                        throw abe;
                    }
                }
            }
        }
        RequestContext.get().endMetricRecord(metric);
    }

    @GET
    @Path("bulk/headers")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public AtlasEntityHeaders getEntityHeaders(@QueryParam("tagUpdateStartTime") long tagUpdateStartTime) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            long  tagUpdateEndTime = System.currentTimeMillis();

            if (tagUpdateStartTime > tagUpdateEndTime) {
                throw new AtlasBaseException(BAD_REQUEST, "fromTimestamp should be less than toTimestamp");
            }

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getEntityHeaders(" + tagUpdateStartTime + ", " + tagUpdateEndTime + ")");
            }

            ClassificationAssociator.Retriever associator = new ClassificationAssociator.Retriever(typeRegistry, esBasedAuditRepository, entityGraphRetriever);
            return associator.get(tagUpdateStartTime, tagUpdateEndTime);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("bulk/setClassifications")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void setClassifications(AtlasEntityHeaders entityHeaders, @QueryParam("overrideClassifications") @DefaultValue("true") boolean overrideClassifications) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.setClassifications(" + overrideClassifications +")");
            }

            entityMutationService.setClassifications(entityHeaders, overrideClassifications);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("repairClassificationsMappings/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void repairClassifications(@PathParam("guid") String guid) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairClassifications()");
            }

            entityMutationService.repairClassificationMappings(Collections.singletonList(guid));
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("bulk/repairClassificationsMappings")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public Map<String, String> repairClassificationsMappings(Set<String> guids) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairClassificationsMappings(" + guids.size() + ")");
            }

            return entityMutationService.repairClassificationMappings(new ArrayList<>(guids));
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("/guid/{guid}/businessmetadata")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void addOrUpdateBusinessAttributes(@PathParam("guid") final String guid, @QueryParam("isOverwrite") @DefaultValue("false") boolean isOverwrite, Map<String, Map<String, Object>> businessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addOrUpdateBusinessAttributes(" + guid + ", isOverwrite=" + isOverwrite + ")");
            }

            entitiesStore.addOrUpdateBusinessAttributes(guid, businessAttributes, isOverwrite);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("/guid/{guid}/businessmetadata/displayName")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void addOrUpdateBusinessAttributesByDisplayName(@PathParam("guid") final String guid, @QueryParam("isOverwrite") @DefaultValue("false") boolean isOverwrite, Map<String, Map<String, Object>> businessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addOrUpdateBusinessAttributesByDisplayName(" + guid + ", isOverwrite=" + isOverwrite + ")");
            }

            entitiesStore.addOrUpdateBusinessAttributesByDisplayName(guid, businessAttributes, isOverwrite);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @DELETE
    @Path("/guid/{guid}/businessmetadata")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void removeBusinessAttributes(@PathParam("guid") final String guid, Map<String, Map<String, Object>> businessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.removeBusinessAttributes(" + guid + ")");
            }

            entitiesStore.removeBusinessAttributes(guid, businessAttributes);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("/guid/{guid}/businessmetadata/{bmName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void addOrUpdateBusinessAttributes(@PathParam("guid") final String guid, @PathParam("bmName") final String bmName, Map<String, Object> businessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addOrUpdateBusinessAttributes(" + guid + ", " + bmName + ")");
            }

            entitiesStore.addOrUpdateBusinessAttributes(guid, Collections.singletonMap(bmName, businessAttributes), false);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @DELETE
    @Path("/guid/{guid}/businessmetadata/{bmName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void removeBusinessAttributes(@PathParam("guid") final String guid, @PathParam("bmName") final String bmName, Map<String, Object> businessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.removeBusinessAttributes(" + guid + ", " + bmName + ")");
            }

            entitiesStore.removeBusinessAttributes(guid, Collections.singletonMap(bmName, businessAttributes));
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Set labels to a given entity
     * @param guid - Unique entity identifier
     * @param labels - set of labels to be set to the entity
     * @throws AtlasBaseException
     */
    @POST
    @Path("/guid/{guid}/labels")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void setLabels(@PathParam("guid") final String guid, Set<String> labels) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.setLabels()");
            }

            entitiesStore.setLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * delete given labels to a given entity
     * @param guid - Unique entity identifier
     * @throws AtlasBaseException
     */
    @DELETE
    @Path("/guid/{guid}/labels")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void removeLabels(@PathParam("guid") final String guid, Set<String> labels) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteLabels()");
            }

            entitiesStore.removeLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * add given labels to a given entity
     * @param guid - Unique entity identifier
     * @throws AtlasBaseException
     */
    @PUT
    @Path("/guid/{guid}/labels")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void addLabels(@PathParam("guid") final String guid, Set<String> labels) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addLabels()");
            }

            entitiesStore.addLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("/uniqueAttribute/type/{typeName}/labels")
    @Timed
    public void setLabels(@PathParam("typeName") String typeName, Set<String> labels,
                          @Context HttpServletRequest servletRequest) throws AtlasBaseException {

        Servlets.validateQueryParamLength("typeName", typeName);
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.setLabels(" + typeName + ")");
            }

            AtlasEntityType     entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String              guid       = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entitiesStore.setLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @PUT
    @Path("/uniqueAttribute/type/{typeName}/labels")
    @Timed
    public void addLabels(@PathParam("typeName") String typeName, Set<String> labels,
                          @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addLabels(" + typeName + ")");
            }

            AtlasEntityType     entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String              guid       = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entitiesStore.addLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @DELETE
    @Path("/uniqueAttribute/type/{typeName}/labels")
    @Timed
    public void removeLabels(@PathParam("typeName") String typeName, Set<String> labels,
                             @Context HttpServletRequest servletRequest) throws AtlasBaseException {

        Servlets.validateQueryParamLength("typeName", typeName);
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.removeLabels(" + typeName + ")");
            }

            AtlasEntityType     entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String              guid       = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entitiesStore.removeLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
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

    // attr:qualifiedName=db1@cl1 ==> { qualifiedName:db1@cl1 }
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

    // attr_1:qualifiedName=db1@cl1&attr_2:qualifiedName=db2@cl1 ==> [ { qualifiedName:db1@cl1 }, { qualifiedName:db2@cl1 } ]
    private List<Map<String, Object>> getAttributesList(HttpServletRequest request) {
        Map<String, Map<String, Object>> ret = new HashMap<>();

        if (MapUtils.isNotEmpty(request.getParameterMap())) {
            for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
                String key = entry.getKey();

                if (key == null || !key.startsWith(PREFIX_ATTR_)) {
                    continue;
                }

                int      sepPos = key.indexOf(':', PREFIX_ATTR_.length());
                String[] values = entry.getValue();
                String   value  = values != null && values.length > 0 ? values[0] : null;

                if (sepPos == -1 || value == null) {
                    continue;
                }

                String              attrName   = key.substring(sepPos + 1);
                String              listIdx    = key.substring(PREFIX_ATTR_.length(), sepPos);
                Map<String, Object> attributes = ret.get(listIdx);

                if (attributes == null) {
                    attributes = new HashMap<>();

                    ret.put(listIdx, attributes);
                }

                attributes.put(attrName, value);
            }
        }

        return new ArrayList<>(ret.values());
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

    /**
     * Get the sample Template for uploading/creating bulk BusinessMetaData
     *
     * @return Template File
     * @throws AtlasBaseException
     * @HTTP 400 If the provided fileType is not supported
     */
    @GET
    @Path("/businessmetadata/import/template")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response produceTemplate() {
        return Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                outputStream.write(FileUtils.getBusinessMetadataHeaders().getBytes());
            }
        }).header("Content-Disposition", "attachment; filename=\"template_business_metadata\"").build();
    }

    /**
     * Upload the file for creating Business Metadata in BULK
     *
     * @param uploadedInputStream InputStream of file
     * @param fileDetail          FormDataContentDisposition metadata of file
     * @return
     * @throws AtlasBaseException
     * @HTTP 200 If Business Metadata creation was successful
     * @HTTP 400 If Business Metadata definition has invalid or missing information
     * @HTTP 409 If Business Metadata already exists (duplicate qualifiedName)
     */
    @POST
    @Path("/businessmetadata/import")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Timed
    public BulkImportResponse importBMAttributes(@FormDataParam("file") InputStream uploadedInputStream,
                                                 @FormDataParam("file") FormDataContentDisposition fileDetail) throws AtlasBaseException {

        return entitiesStore.bulkCreateOrUpdateBusinessAttributes(uploadedInputStream, fileDetail.getFileName());
    }

    private AtlasEntityHeader getEntityHeaderFromPurgedAudit(String guid) throws AtlasBaseException {
        List<EntityAuditEventV2> auditEvents = esBasedAuditRepository.listEventsV2(guid, EntityAuditActionV2.ENTITY_PURGE, null, (short)1);
        AtlasEntityHeader        ret         = CollectionUtils.isNotEmpty(auditEvents) ? auditEvents.get(0).getEntityHeader() : null;

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        return ret;
    }

    /**
     * Reindexs all the mixed indices.
     */
    @POST
    @Path("/repairindex")
    @Timed
    public void repairIndex() throws AtlasBaseException {

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairIndex");
            }
            entitiesStore.repairIndex();
        } finally {
            AtlasPerfTracer.log(perf);
        }

    }

    /**
     * repairHasLineage API to correct haslineage attribute of entity.
     */
    @POST
    @Path("/repairhaslineage")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    public void repairHasLineage(AtlasHasLineageRequests request) throws AtlasBaseException {

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairHasLineage(" + request + ")");
            }

            entitiesStore.repairHasLineage(request);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * repairHasLineageByIds API to correct hasLineage attribute for entities by vertex IDs.
     * This endpoint accepts a list of vertex IDs and repairs the hasLineage flag for both
     * Process and Asset entities based on their current lineage state.
     * 
     * @param typeByVertexId Map of vertex IDs to repair
     * @throws AtlasBaseException if repair operation fails
     */
    @POST
    @Path("/repairhaslineagebyids")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public void repairHasLineageByIds(Map<String, String> typeByVertexId) throws AtlasBaseException {
        
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairHasLineageByIds(typeByVertexId count=" +
                        (typeByVertexId != null ? typeByVertexId.size() : 0) + ")");
            }

            if (typeByVertexId == null || typeByVertexId.isEmpty()) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "typeByVertexId map cannot be empty");
            }

            entitiesStore.repairHasLineageByIds(typeByVertexId);


        } catch (AtlasBaseException e) {
            LOG.error("Failed to repair hasLineage by IDs", e);
            throw e;
        } catch (Exception e) {
            LOG.error("Unexpected error during repairHasLineageByIds", e);
            throw new AtlasBaseException(e);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private boolean hasNoGUIDAndTypeNameAttributes(ClassificationAssociateRequest request) {
        return (request == null || (CollectionUtils.isEmpty(request.getEntityGuids()) &&
                (CollectionUtils.isEmpty(request.getEntitiesUniqueAttributes()) || request.getEntityTypeName() == null)));
    }

    private void validateAccessorRequest(List<AtlasAccessorRequest> atlasAccessorRequestList) throws AtlasBaseException {

        if (CollectionUtils.isEmpty(atlasAccessorRequestList)) {
            throw new AtlasBaseException(BAD_REQUEST, "Requires list of AtlasAccessor");
        } else {

            for (AtlasAccessorRequest accessorRequest : atlasAccessorRequestList) {
                try {
                    if (StringUtils.isEmpty(accessorRequest.getAction())) {
                        throw new AtlasBaseException(BAD_REQUEST, "Requires action parameter");
                    }

                    AtlasPrivilege action = null;
                    try {
                        action = AtlasPrivilege.valueOf(accessorRequest.getAction());
                    } catch (IllegalArgumentException el) {
                        throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid action provided " + accessorRequest.getAction());
                    }

                    switch (action) {
                        case ENTITY_READ:
                        case ENTITY_CREATE:
                        case ENTITY_UPDATE:
                        case ENTITY_DELETE:
                            validateEntityForAccessors(accessorRequest.getGuid(), accessorRequest.getQualifiedName(), accessorRequest.getTypeName());
                            break;

                        case ENTITY_READ_CLASSIFICATION:
                        case ENTITY_ADD_CLASSIFICATION:
                        case ENTITY_UPDATE_CLASSIFICATION:
                        case ENTITY_REMOVE_CLASSIFICATION:
                            if (StringUtils.isEmpty(accessorRequest.getClassification())) {
                                throw new AtlasBaseException(BAD_REQUEST, "Requires classification");
                            }
                            validateEntityForAccessors(accessorRequest.getGuid(), accessorRequest.getQualifiedName(), accessorRequest.getTypeName());
                            break;

                        case ENTITY_ADD_LABEL:
                        case ENTITY_REMOVE_LABEL:
                            if (StringUtils.isEmpty(accessorRequest.getLabel())) {
                                throw new AtlasBaseException(BAD_REQUEST, "Requires label");
                            }
                            validateEntityForAccessors(accessorRequest.getGuid(), accessorRequest.getQualifiedName(), accessorRequest.getTypeName());
                            break;

                        case ENTITY_UPDATE_BUSINESS_METADATA:
                            if (StringUtils.isEmpty(accessorRequest.getBusinessMetadata())) {
                                throw new AtlasBaseException(BAD_REQUEST, "Requires businessMetadata");
                            }
                            validateEntityForAccessors(accessorRequest.getGuid(), accessorRequest.getQualifiedName(), accessorRequest.getTypeName());
                            break;


                        case RELATIONSHIP_ADD:
                        case RELATIONSHIP_UPDATE:
                        case RELATIONSHIP_REMOVE:
                            if (StringUtils.isEmpty(accessorRequest.getRelationshipTypeName())) {
                                throw new AtlasBaseException(BAD_REQUEST, "Requires relationshipTypeName");
                            }

                            validateEntityForAccessors(accessorRequest.getEntityGuidEnd1(), accessorRequest.getEntityQualifiedNameEnd1(), accessorRequest.getEntityTypeEnd1());
                            validateEntityForAccessors(accessorRequest.getEntityGuidEnd2(), accessorRequest.getEntityQualifiedNameEnd2(), accessorRequest.getEntityTypeEnd2());
                            break;


                        case TYPE_READ:
                        case TYPE_CREATE:
                        case TYPE_UPDATE:
                        case TYPE_DELETE:
                            if (StringUtils.isEmpty(accessorRequest.getTypeName())) {
                                throw new AtlasBaseException(BAD_REQUEST, "Requires typeName of the asset");
                            }
                            break;

                        default:
                            throw new AtlasBaseException(BAD_REQUEST, "Please add validation support for action {}", accessorRequest.getAction());

                    }

                } catch (AtlasBaseException e) {
                    e.getErrorDetailsMap().put("accessorRequest", AtlasType.toJson(accessorRequest));
                    throw e;
                }
            }
        }
    }

    private void validateEntityForAccessors(String guid, String qualifiedName, String typeName) throws AtlasBaseException {
        if (StringUtils.isEmpty(typeName) && StringUtils.isNotEmpty(qualifiedName)) {
            throw new AtlasBaseException(BAD_REQUEST, "Requires typeName of the asset with qualifiedName");
        }

        if (StringUtils.isEmpty(guid) && StringUtils.isEmpty(qualifiedName)) {
            throw new AtlasBaseException(BAD_REQUEST, "Requires either qualifiedName or GUID of the asset");
        }
    }

    /**
     * Repair index for the entity GUID.
     * @param guid GUID for the entity
     * @return AtlasEntity
     * @throws AtlasBaseException
     */
    @POST
    @Path("/guid/{guid}/repairindex")
    public void repairEntityIndex(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_REPAIR_INDEX), "Admin Repair Index");

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairEntityIndex(" + guid + ")");
            }

            AtlasEntityWithExtInfo entity = entitiesStore.getById(guid);
            Map<String, AtlasEntity> referredEntities = entity.getReferredEntities();
            repairIndex.restoreSelective(guid, referredEntities);
        } catch (Exception e) {
            LOG.error("Exception while repairEntityIndex ", e);
            throw new AtlasBaseException(e);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("/guid/bulk/repairindex")
    public void repairEntityIndexBulk(Set<String> guids) throws AtlasBaseException {

        AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_REPAIR_INDEX), "Admin Repair Index");

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairEntityIndexBulk(" + guids.size() + ")");
            }
            repairIndex.restoreByIds(guids);
        } catch (Exception e) {
            LOG.error("Exception while repairEntityIndexBulk ", e);
            throw new AtlasBaseException(e);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Repair attributes for the entity GUID.
     * @param guids
     * @throws AtlasBaseException
     */

    @POST
    @Path("/guid/bulk/repairattributes")
    public void repairEntityAttributesBulk(Set<String> guids, @QueryParam("repairType") String repairType, @QueryParam("repairAttributeName") String repairAttributeName) throws AtlasBaseException {

        Servlets.validateQueryParamLength("repairType", repairType);
        Servlets.validateQueryParamLength("repairAttributeName", repairAttributeName);

        AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_REPAIR_INDEX), "Admin Repair Attributes");

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairEntityAttributesBulk(" + guids.size() + ")");
            }

            repairAttributeService.repairAttributes(repairAttributeName, repairType, guids);

        } catch (Exception e) {
            LOG.error("Exception while repairEntityAttributesBulk ", e);
            throw new AtlasBaseException(e);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("/repairindex/{typename}")
    public void repairIndexByTypeName(@PathParam("typename") String typename, @QueryParam("delay") @DefaultValue("0") int delay, @QueryParam("limit") @DefaultValue("1000") int limit, @QueryParam("offset") @DefaultValue("0") int offset, @QueryParam("batchSize") @DefaultValue("1000") int batchSize) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typename", typename);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairAllEntitiesIndex");
            }

            AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_REPAIR_INDEX), "Admin Repair Index");

            LOG.info("Repairing index for entities in " + typename);

            long startTime = System.currentTimeMillis();

            List<String> entityGUIDs = entitiesStore.getEntityGUIDS(typename);

            LOG.info("Entities to repair in " + typename + " " + entityGUIDs.size());

            if (entityGUIDs.size() > offset + limit) {
                entityGUIDs = entityGUIDs.subList(offset, offset + limit);
                LOG.info("Updated - Entities to repair in " + typename + " " + entityGUIDs.size());
            } else if (entityGUIDs.size() > offset) {
                entityGUIDs = entityGUIDs.subList(offset, entityGUIDs.size());
                LOG.info("Updated - Entities to repair in " + typename + " " + entityGUIDs.size());
            } else {
                LOG.info("No entities to repair");
                return;
            }

            List<List<String>> entityGUIDsChunked = Lists.partition(entityGUIDs, batchSize);
            for (List<String> guids : entityGUIDsChunked) {
                Set<String> guidsToReIndex = new HashSet<>(guids);
                repairIndex.restoreByIds(guidsToReIndex);
                if (guids.size() >= batchSize && delay > 0) {
                    try {
                        LOG.info("Sleep for " + delay + " ms");
                        Thread.sleep(delay);
                    } catch(InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        LOG.info("Thread interrupted at " + typename);
                    }
                }
                LOG.info("Repaired index for " + guids.size() + " entities of type" + typename);
            }
            LOG.info("Repaired index for entities for typeName " + typename + " in " + (System.currentTimeMillis() - startTime) + " ms");

        } catch (Exception e) {
            LOG.error("Exception while repairIndexByTypeName ", e);
            throw new AtlasBaseException(e);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("/repairAllClassifications")
    public void repairAllClassifications(@QueryParam("delay") @DefaultValue("0") int delay, @QueryParam("batchSize") @DefaultValue("1000") int batchSize, @QueryParam("fetchSize") @DefaultValue("5000") int fetchSize) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairAllClassifications");
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_REPAIR_INDEX), "Admin Repair Classifications");
        try {
            Set<Long> vertexIds = entitiesStore.getVertexIdFromTags(fetchSize);
            entityMutationService.repairClassificationMappingsByVertexIds(vertexIds, batchSize, delay);

        } catch (Exception e) {
            LOG.error("Exception while repairAllClassifications", e);
            throw new AtlasBaseException(e);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("/repair/accesscontrolAlias/{guid}")
    @Timed
    public void repairAccessControlAlias(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;


        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.repairAccessControlAlias");
            }

            entitiesStore.repairAccesscontrolAlias(guid);

            LOG.info("Repaired access control alias for entity with guid {}", guid);

        } finally {
            AtlasPerfTracer.log(perf);
        }


    }
}