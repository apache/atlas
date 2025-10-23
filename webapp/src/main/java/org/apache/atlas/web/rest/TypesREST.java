/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.rest;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.*;
import org.apache.atlas.repository.graph.TypeCacheRefresher;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.http.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import static org.apache.atlas.AtlasErrorCode.APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.ATTRIBUTE_DELETION_NOT_SUPPORTED;
import static org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer.TYPEDEF_LATEST_VERSION;
import static org.apache.atlas.web.filters.AuditFilter.X_ATLAN_CLIENT_ORIGIN;

/**
 * REST interface for CRUD operations on type definitions
 */
@Path("types")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class TypesREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.TypesREST");
    private static final Logger LOG = LoggerFactory.getLogger(TypesREST.class);
    // private static final String ATLAS_TYPEDEF_LOCK = "atlas:type-def:lock";

    private final AtlasTypeDefStore typeDefStore;
    private final RedisService redisService;
    private final TypeCacheRefresher typeCacheRefresher;
    private String typeDefLock;

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;

    @Inject
    public TypesREST(AtlasTypeDefStore typeDefStore, RedisService redisService, Configuration configuration, TypeCacheRefresher typeCacheRefresher) {
        this.typeDefStore = typeDefStore;
        this.redisService = redisService;
        this.typeCacheRefresher = typeCacheRefresher;
        this.typeDefLock = configuration.getString(ApplicationProperties.TYPEDEF_LOCK_NAME, "atlas:type-def:lock");
        LOG.info("TypeDef lock name: {}", this.typeDefLock);
    }

    /**
     * Get type definition by it's name
     * @param name Type name
     * @return Type definition
     * @throws AtlasBaseException
     * @HTTP 200 Successful lookup by name
     * @HTTP 404 Failed lookup by name
     */
    @GET
    @Path("/typedef/name/{name}")
    @Timed
    public AtlasBaseTypeDef getTypeDefByName(@PathParam("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasBaseTypeDef ret = typeDefStore.getByName(name);

        return ret;
    }

    /**
     * @param guid GUID of the type
     * @return Type definition
     * @throws AtlasBaseException
     * @HTTP 200 Successful lookup
     * @HTTP 404 Failed lookup
     */
    @GET
    @Path("/typedef/guid/{guid}")
    @Timed
    public AtlasBaseTypeDef getTypeDefByGuid(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasBaseTypeDef ret = typeDefStore.getByGuid(guid);

        return ret;
    }

    /**
     * Bulk retrieval API for all type definitions returned as a list of minimal information header
     * @return List of AtlasTypeDefHeader {@link AtlasTypeDefHeader}
     * @throws AtlasBaseException
     * @HTTP 200 Returns a list of {@link AtlasTypeDefHeader} matching the search criteria
     * or an empty list if no match.
     */
    @GET
    @Path("/typedefs/headers")
    @Timed
    public List<AtlasTypeDefHeader> getTypeDefHeaders(@Context HttpServletRequest httpServletRequest) throws AtlasBaseException {
        SearchFilter searchFilter = getSearchFilter(httpServletRequest);

        AtlasTypesDef searchTypesDef = typeDefStore.searchTypesDef(searchFilter);

        return AtlasTypeUtil.toTypeDefHeader(searchTypesDef);
    }

    /**
     * Bulk retrieval API for retrieving all type definitions in Atlas
     * @return A composite wrapper object with lists of all type definitions
     * @throws Exception
     * @HTTP 200 {@link AtlasTypesDef} with type definitions matching the search criteria or else returns empty list of type definitions
     */
    @GET
    @Path("/typedefs")
    @Timed
    public AtlasTypesDef getAllTypeDefs(@Context HttpServletRequest httpServletRequest) throws AtlasBaseException {
        SearchFilter searchFilter = getSearchFilter(httpServletRequest);

        AtlasTypesDef typesDef = typeDefStore.searchTypesDef(searchFilter);

        return typesDef;
    }

    /**
     * Get the enum definition by it's name (unique)
     * @param name enum name
     * @return enum definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the enum definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GET
    @Path("/enumdef/name/{name}")
    @Timed
    public AtlasEnumDef getEnumDefByName(@PathParam("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasEnumDef ret = typeDefStore.getEnumDefByName(name);

        return ret;
    }

    /**
     * Get the enum definition for the given guid
     * @param guid enum guid
     * @return enum definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the enum definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GET
    @Path("/enumdef/guid/{guid}")
    @Timed
    public AtlasEnumDef getEnumDefByGuid(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasEnumDef ret = typeDefStore.getEnumDefByGuid(guid);

        return ret;
    }


    /**
     * Get the struct definition by it's name (unique)
     * @param name struct name
     * @return struct definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the struct definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GET
    @Path("/structdef/name/{name}")
    @Timed
    public AtlasStructDef getStructDefByName(@PathParam("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasStructDef ret = typeDefStore.getStructDefByName(name);

        return ret;
    }

    /**
     * Get the struct definition for the given guid
     * @param guid struct guid
     * @return struct definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the struct definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GET
    @Path("/structdef/guid/{guid}")
    @Timed
    public AtlasStructDef getStructDefByGuid(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasStructDef ret = typeDefStore.getStructDefByGuid(guid);

        return ret;
    }

    /**
     * Get the classification definition by it's name (unique)
     * @param name classification name
     * @return classification definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the classification definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GET
    @Path("/classificationdef/name/{name}")
    @Timed
    public AtlasClassificationDef getClassificationDefByName(@PathParam("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasClassificationDef ret = typeDefStore.getClassificationDefByName(name);

        return ret;
    }

    /**
     * Get the classification definition for the given guid
     * @param guid classification guid
     * @return classification definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the classification definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GET
    @Path("/classificationdef/guid/{guid}")
    @Timed
    public AtlasClassificationDef getClassificationDefByGuid(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasClassificationDef ret = typeDefStore.getClassificationDefByGuid(guid);

        return ret;
    }

    /**
     * Get the entity definition by it's name (unique)
     * @param name entity name
     * @return Entity definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the entity definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GET
    @Path("/entitydef/name/{name}")
    @Timed
    public AtlasEntityDef getEntityDefByName(@PathParam("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasEntityDef ret = typeDefStore.getEntityDefByName(name);

        return ret;
    }

    /**
     * Get the Entity definition for the given guid
     * @param guid entity guid
     * @return Entity definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the entity definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GET
    @Path("/entitydef/guid/{guid}")
    @Timed
    public AtlasEntityDef getEntityDefByGuid(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasEntityDef ret = typeDefStore.getEntityDefByGuid(guid);

        return ret;
    }
    /**
     * Get the relationship definition by it's name (unique)
     * @param name relationship name
     * @return relationship definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the relationship definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GET
    @Path("/relationshipdef/name/{name}")
    @Timed
    public AtlasRelationshipDef getRelationshipDefByName(@PathParam("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasRelationshipDef ret = typeDefStore.getRelationshipDefByName(name);

        return ret;
    }

    /**
     * Get the relationship definition for the given guid
     * @param guid relationship guid
     * @return relationship definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the relationship definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GET
    @Path("/relationshipdef/guid/{guid}")
    @Timed
    public AtlasRelationshipDef getRelationshipDefByGuid(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasRelationshipDef ret = typeDefStore.getRelationshipDefByGuid(guid);

        return ret;
    }

    /**
     * Get the businessMetadata definition for the given guid
     * @param guid businessMetadata guid
     * @return businessMetadata definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the businessMetadata definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GET
    @Path("/businessmetadatadef/guid/{guid}")
    @Timed
    public AtlasBusinessMetadataDef getBusinessMetadataDefByGuid(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasBusinessMetadataDef ret = typeDefStore.getBusinessMetadataDefByGuid(guid);

        return ret;
    }

    /**
     * Get the businessMetadata definition by it's name (unique)
     * @param name businessMetadata name
     * @return businessMetadata definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the businessMetadata definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GET
    @Path("/businessmetadatadef/name/{name}")
    @Timed
    public AtlasBusinessMetadataDef getBusinessMetadataDefByName(@PathParam("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasBusinessMetadataDef ret = typeDefStore.getBusinessMetadataDefByName(name);

        return ret;
    }

    private void attemptAcquiringLock() throws AtlasBaseException {
        final String traceId = RequestContext.get().getTraceId();
        try {
            if (!redisService.acquireDistributedLock(typeDefLock)) {
                LOG.info("Lock is already acquired. Returning now :: traceId {}", traceId);
                throw new AtlasBaseException(AtlasErrorCode.FAILED_TO_OBTAIN_TYPE_UPDATE_LOCK);
            }
            LOG.info("successfully acquired lock :: traceId {}", traceId);
        } catch (AtlasBaseException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Error while acquiring lock on type-defs :: traceId " + traceId + " ." + e.getMessage(), e);
            throw new AtlasBaseException("Error while acquiring a lock on type-defs");
        }
    }

    private Lock attemptAcquiringLockV2() throws AtlasBaseException {
        final String traceId = RequestContext.get().getTraceId();
        Lock lock = null;
        try {

            lock = redisService.acquireDistributedLockV2(typeDefLock);
            if (lock == null) {
                LOG.info("Lock is already acquired. for key {} : Returning now :: traceId {}", typeDefLock, traceId);
                throw new AtlasBaseException(AtlasErrorCode.FAILED_TO_OBTAIN_TYPE_UPDATE_LOCK);
            }
            LOG.info("Successfully acquired lock with key {} :: traceId {}", typeDefLock, traceId);
            return lock;
        } catch (AtlasBaseException e) {
            redisService.releaseDistributedLockV2(lock, typeDefLock);
            throw e;
        } catch (Exception e) {
            LOG.error("Error while acquiring lock on type-defs :: traceId " + traceId + " ." + e.getMessage(), e);
            throw new AtlasBaseException("Error while acquiring a lock on type-defs", e);
        }
    }

    /* Bulk API operation */

    /**
     * Bulk create APIs for all atlas type definitions, only new definitions will be created.
     * Any changes to the existing definitions will be discarded
     * @param typesDef A composite wrapper object with corresponding lists of the type definition
     * @param allowDuplicateDisplayName
     * @return A composite wrapper object with lists of type definitions that were successfully
     * created
     * @throws Exception
     * @HTTP 200 On successful update of requested type definitions
     * @HTTP 400 On validation failure for any type definitions
     */
    @POST
    @Path("/typedefs")
    @Timed
    public AtlasTypesDef createAtlasTypeDefs(final AtlasTypesDef typesDef, @QueryParam("allowDuplicateDisplayName") @DefaultValue("false") boolean allowDuplicateDisplayName) throws AtlasBaseException {
        Lock lock = null;
        AtlasPerfTracer perf = null;
        validateBuiltInTypeNames(typesDef);
        RequestContext.get().setTraceId(UUID.randomUUID().toString());
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.createAtlasTypeDefs(" +
                        AtlasTypeUtil.toDebugString(typesDef) + ")");
            }
            lock = attemptAcquiringLockV2();
            RequestContext.get().setAllowDuplicateDisplayName(allowDuplicateDisplayName);
            typesDef.getBusinessMetadataDefs().forEach(AtlasBusinessMetadataDef::setRandomNameForEntityAndAttributeDefs);
            typesDef.getClassificationDefs().forEach(AtlasClassificationDef::setRandomNameForEntityAndAttributeDefs);
            AtlasTypesDef atlasTypesDef = createTypeDefsWithRetry(typesDef);
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentTypedefInternalVersion(latestVersion);
            return atlasTypesDef;
        } catch (AtlasBaseException atlasBaseException) {
            LOG.error("TypesREST.createAtlasTypeDefs:: " + atlasBaseException.getMessage(), atlasBaseException);
            throw atlasBaseException;
        } catch (Exception e) {
            LOG.error("TypesREST.createAtlasTypeDefs:: " + e.getMessage(), e);
            throw new AtlasBaseException("Error while creating a type definition");
        }
        finally {
            if (lock != null) {
                redisService.releaseDistributedLockV2(lock, typeDefLock);
            }
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk update API for all types, changes detected in the type definitions would be persisted
     * @param typesDef A composite object that captures all type definition changes
     * @return A composite object with lists of type definitions that were updated
     * @throws Exception
     * @HTTP 200 On successful update of requested type definitions
     * @HTTP 400 On validation failure for any type definitions
     */
    @PUT
    @Path("/typedefs")
    @Experimental
    @Timed
    public AtlasTypesDef updateAtlasTypeDefs(final AtlasTypesDef typesDef, @QueryParam("patch") final boolean patch,
                                             @QueryParam("allowDuplicateDisplayName") @DefaultValue("false") boolean allowDuplicateDisplayName) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        validateBuiltInTypeNames(typesDef);
        validateTypeNameExists(typesDef);
        RequestContext.get().setTraceId(UUID.randomUUID().toString());
        Lock lock = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.updateAtlasTypeDefs(" +
                        AtlasTypeUtil.toDebugString(typesDef) + ")");
            }
            lock = attemptAcquiringLockV2();
            for (AtlasBusinessMetadataDef mb : typesDef.getBusinessMetadataDefs()) {
                AtlasBusinessMetadataDef existingMB;
                try{
                    existingMB = typeDefStore.getBusinessMetadataDefByGuid(mb.getGuid());
                }catch (AtlasBaseException e){
                    //do nothing -- this BM is ew
                    existingMB = null;
                }
                mb.setRandomNameForNewAttributeDefs(existingMB);
            }
            for (AtlasClassificationDef classificationDef : typesDef.getClassificationDefs()) {
                AtlasClassificationDef existingClassificationDef;
                try{
                    existingClassificationDef = typeDefStore.getClassificationDefByGuid(classificationDef.getGuid());
                }catch (AtlasBaseException e){
                    //do nothing -- this classification is ew
                    existingClassificationDef = null;
                }
                classificationDef.setRandomNameForNewAttributeDefs(existingClassificationDef);
            }
            RequestContext.get().setInTypePatching(patch);
            RequestContext.get().setAllowDuplicateDisplayName(allowDuplicateDisplayName);
            LOG.info("TypesRest.updateAtlasTypeDefs:: Typedef patch enabled:" + patch);
            AtlasTypesDef atlasTypesDef = updateTypeDefsWithRetry(typesDef);
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentTypedefInternalVersion(latestVersion);
            LOG.info("TypesRest.updateAtlasTypeDefs:: Done");
            return atlasTypesDef;
        } catch (AtlasBaseException atlasBaseException) {
            LOG.error("TypesREST.updateAtlasTypeDefs:: " + atlasBaseException.getMessage(), atlasBaseException);
            throw atlasBaseException;
        } catch (Exception e) {
            LOG.error("TypesREST.updateAtlasTypeDefs:: " + e.getMessage(), e);
            throw new AtlasBaseException("Error while updating a type definition");
        } finally {
            if (lock != null) {
                redisService.releaseDistributedLockV2(lock, typeDefLock);
            }
            RequestContext.clear();
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk delete API for all types
     * @param typesDef A composite object that captures all types to be deleted
     * @throws Exception
     * @HTTP 204 On successful deletion of the requested type definitions
     * @HTTP 400 On validation failure for any type definitions
     */
    @DELETE
    @Path("/typedefs")
    @Experimental
    @Timed
    public void deleteAtlasTypeDefs(final AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        Lock lock = null;
        RequestContext.get().setTraceId(UUID.randomUUID().toString());
        validateBuiltInTypeNames(typesDef);
        validateTypeNameExists(typesDef);
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.deleteAtlasTypeDefs(" +
                        AtlasTypeUtil.toDebugString(typesDef) + ")");
            }
            lock = attemptAcquiringLockV2();
            deleteTypeDefsWithRetry(typesDef);
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentTypedefInternalVersion(latestVersion);
        } catch (AtlasBaseException atlasBaseException) {
            LOG.error("TypesREST.deleteAtlasTypeDefs:: " + atlasBaseException.getMessage(), atlasBaseException);
            throw atlasBaseException;
        } catch (Exception e) {
            LOG.error("TypesREST.deleteAtlasTypeDefs:: " + e.getMessage(), e);
            throw new AtlasBaseException("Error while deleting a type definition");
        } finally {
            if (lock != null) {
                redisService.releaseDistributedLockV2(lock, typeDefLock);
            }
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete API for type identified by its name.
     * @param typeName Name of the type to be deleted.
     * @throws AtlasBaseException
     * @HTTP 204 On successful deletion of the requested type definitions
     * @HTTP 400 On validation failure for any type definitions
     */
    @DELETE
    @Path("/typedef/name/{typeName}")
    @Timed
    public void deleteAtlasTypeByName(@PathParam("typeName") final String typeName) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        Lock lock = null;
        RequestContext.get().setTraceId(UUID.randomUUID().toString());
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.deleteAtlasTypeByName(" + typeName + ")");
            }
            lock = attemptAcquiringLockV2();
            deleteTypeByNameWithRetry(typeName);
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentTypedefInternalVersion(latestVersion);
        } catch (AtlasBaseException atlasBaseException) {
            LOG.error("TypesREST.deleteAtlasTypeByName:: " + atlasBaseException.getMessage(), atlasBaseException);
            throw atlasBaseException;
        } catch (Exception e) {
            LOG.error("TypesREST.deleteAtlasTypeByName:: " + e.getMessage(), e);
            throw new AtlasBaseException("Error while deleting a type definition");
        } finally {
            if (lock != null) {
                redisService.releaseDistributedLockV2(lock, typeDefLock);
            }
            AtlasPerfTracer.log(perf);
        }
    }

    private void validateBuiltInTypeNames(AtlasTypesDef typesDef) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(typesDef.getEnumDefs())) {
            for (AtlasBaseTypeDef typeDef : typesDef.getEnumDefs())
                if (typeDefStore.hasBuiltInTypeName(typeDef))
                    throw new AtlasBaseException(AtlasErrorCode.FORBIDDEN_TYPENAME, typeDef.getName());
        }
        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            for (AtlasBaseTypeDef typeDef : typesDef.getEntityDefs())
                if (typeDefStore.hasBuiltInTypeName(typeDef))
                    throw new AtlasBaseException(AtlasErrorCode.FORBIDDEN_TYPENAME, typeDef.getName());
        }
        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            for (AtlasBaseTypeDef typeDef : typesDef.getStructDefs())
                if (typeDefStore.hasBuiltInTypeName(typeDef))
                    throw new AtlasBaseException(AtlasErrorCode.FORBIDDEN_TYPENAME, typeDef.getName());
        }
    }

    private void validateTypeNames(AtlasTypesDef typesDef) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(typesDef.getEnumDefs())) {
            for (AtlasBaseTypeDef typeDef : typesDef.getEnumDefs()) {
                typeDefStore.getByName(typeDef.getName());
            }
        }
        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            for (AtlasBaseTypeDef typeDef : typesDef.getEntityDefs()) {
                typeDefStore.getByName(typeDef.getName());
            }
        }
        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            for (AtlasBaseTypeDef typeDef : typesDef.getStructDefs()) {
                typeDefStore.getByName(typeDef.getName());
            }
        }
        if (CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())) {
            for (AtlasBaseTypeDef typeDef : typesDef.getClassificationDefs()) {
                typeDefStore.getByName(typeDef.getName());
            }
        }
        if (CollectionUtils.isNotEmpty(typesDef.getBusinessMetadataDefs())) {
            for (AtlasBaseTypeDef typeDef : typesDef.getBusinessMetadataDefs()) {
                typeDefStore.getByName(typeDef.getName());
            }
        }
    }

    private void validateTypeNameExists(AtlasTypesDef typesDef) throws AtlasBaseException {
        try {
            validateTypeNames(typesDef);
        } catch (AtlasBaseException e) {
            if(AtlasErrorCode.TYPE_NAME_NOT_FOUND.equals(e.getAtlasErrorCode())) {
                typeDefStore.init();
                validateTypeNames(typesDef);
            }
        }
    }

    /**
     * Populate a SearchFilter on the basis of the Query Parameters
     * @return
     */
    private SearchFilter getSearchFilter(HttpServletRequest httpServletRequest) {
        SearchFilter ret    = new SearchFilter();
        Set<String>  keySet = httpServletRequest.getParameterMap().keySet();

        for (String k : keySet) {
            String key   = String.valueOf(k);

            if (key.equalsIgnoreCase("type")) {
                String[] values = httpServletRequest.getParameterValues(k);
                ret.setParam(key, Arrays.asList(values));

            } else {
                String value = String.valueOf(httpServletRequest.getParameter(k));

                if (key.equalsIgnoreCase("excludeInternalTypesAndReferences") && value.equalsIgnoreCase("true")) {
                    FilterUtil.addParamsToHideInternalType(ret);
                } else {
                    ret.setParam(key, value);
                }
            }

        }

        return ret;
    }

    private void refreshAllHostCache(AtlasTypesDef typesDef, String action) throws AtlasBaseException {
        try {
            typeCacheRefresher.refreshAllHostCache(typesDef, action);
        } catch (Exception e) {
            LOG.error("Error while refreshing all host cache", e);
            throw new AtlasBaseException(AtlasErrorCode.FAILED_TO_REFRESH_TYPE_CACHE, e.getMessage());
        }
    }

    @PreDestroy
    public void cleanUp() {
        this.redisService.releaseDistributedLock(typeDefLock);
    }

    private AtlasTypesDef createTypeDefsWithRetry(AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasBaseException lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Enable patching for cache conflicts (Atlas might confuse CREATE with UPDATE)
                RequestContext.get().setInTypePatching(true);

                // Perform the creation
                AtlasTypesDef result = typeDefStore.createTypesDef(typesDef);
                refreshAllHostCache(result, "CREATE");
                LOG.info("Successfully created typedefs on attempt {}", attempt);
                return result;

            } catch (AtlasBaseException e) {
                lastException = e;

                if (attempt == MAX_RETRIES) {
                    LOG.error("Failed to create typedefs after {} attempts", MAX_RETRIES, e);
                    throw e;
                }

                if (isRetryable(e)) {
                    LOG.warn("Creation attempt {} failed with retryable error, retrying in {}ms",
                            attempt, RETRY_DELAY_MS, e);
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.warn("Creation retry interrupted, failing operation");
                        throw e;
                    }
                } else {
                    LOG.error("Non-retryable error occurred during creation", e);
                    throw e;
                }
            } finally {
                // Reset type patching mode
                RequestContext.get().setInTypePatching(false);
            }
        }

        throw lastException; // Should never reach here, but for completeness
    }

    private AtlasTypesDef updateTypeDefsWithRetry(AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasBaseException lastException = null;
        boolean originalPatchingState = RequestContext.get().isInTypePatching();

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Enable patching for retries to handle cache sync conflicts
                if (attempt > 1) {
                    RequestContext.get().setInTypePatching(true);
                }

                AtlasTypesDef result = typeDefStore.updateTypesDef(typesDef);
                refreshAllHostCache(result, "UPDATE");
                LOG.info("Successfully updated typedefs on attempt {}", attempt);
                return result;

            } catch (AtlasBaseException e) {
                lastException = e;

                if (attempt == MAX_RETRIES) {
                    LOG.error("Failed to update typedefs after {} attempts", MAX_RETRIES, e);
                    throw e;
                }

                if (isRetryable(e)) {
                    LOG.warn("Attempt {} failed with retryable error, retrying in {}ms",
                            attempt, RETRY_DELAY_MS, e);
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.warn("Retry interrupted, failing operation");
                        throw e;
                    }
                } else {
                    LOG.error("Non-retryable error occurred", e);
                    throw e;
                }
            } finally {
                // Always restore original patching state
                RequestContext.get().setInTypePatching(originalPatchingState);
            }
        }

        throw lastException; // Should never reach here, but for completeness
    }

    private void deleteTypeDefsWithRetry(AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasBaseException lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Perform the deletion
                typeDefStore.deleteTypesDef(typesDef);
                refreshAllHostCache(typesDef, "DELETE");
                LOG.info("Successfully deleted typedefs on attempt {}", attempt);
                return;

            } catch (AtlasBaseException e) {
                lastException = e;

                if (attempt == MAX_RETRIES) {
                    LOG.error("Failed to delete typedefs after {} attempts", MAX_RETRIES, e);
                    throw e;
                }

                if (isRetryable(e)) {
                    LOG.warn("Deletion attempt {} failed with retryable error, retrying in {}ms",
                            attempt, RETRY_DELAY_MS, e);
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.warn("Deletion retry interrupted, failing operation");
                        throw e;
                    }
                } else {
                    LOG.error("Non-retryable error occurred during deletion", e);
                    throw e;
                }
            }
        }

        throw lastException; // Should never reach here, but for completeness
    }

    private void deleteTypeByNameWithRetry(String typeName) throws AtlasBaseException {
        AtlasBaseException lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Perform the deletion
                AtlasTypesDef typesDef = typeDefStore.deleteTypeByName(typeName);
                refreshAllHostCache(typesDef, "DELETE");
                LOG.info("Successfully deleted typedef '{}' on attempt {}", typeName, attempt);
                return;

            } catch (AtlasBaseException e) {
                lastException = e;

                if (attempt == MAX_RETRIES) {
                    LOG.error("Failed to delete typedef '{}' after {} attempts", typeName, MAX_RETRIES, e);
                    throw e;
                }

                if (isRetryable(e)) {
                    LOG.warn("Deletion attempt {} for '{}' failed with retryable error, retrying in {}ms",
                            attempt, typeName, RETRY_DELAY_MS, e);
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.warn("Deletion retry interrupted, failing operation");
                        throw e;
                    }
                } else {
                    LOG.error("Non-retryable error occurred during deletion of '{}'", typeName, e);
                    throw e;
                }
            }
        }

        throw lastException; // Should never reach here, but for completeness
    }

    private boolean isRetryable(AtlasBaseException e) {
        return APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED.equals(e.getAtlasErrorCode()) ||
                ATTRIBUTE_DELETION_NOT_SUPPORTED.equals(e.getAtlasErrorCode());
    }
}
