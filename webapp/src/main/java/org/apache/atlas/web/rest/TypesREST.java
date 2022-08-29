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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.*;
import org.apache.atlas.repository.graph.TypeCacheRefresher;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.service.CuratorFactory;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.http.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
import java.util.concurrent.TimeUnit;

/**
 * REST interface for CRUD operations on type definitions
 */
@Path("v2/types")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class TypesREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.TypesREST");
    private static final Logger LOG = LoggerFactory.getLogger(TypesREST.class);
    private static final String TYPE_DEF_LOCK = "/type-def-lock";
    private final String zkRoot;


    private final AtlasTypeDefStore typeDefStore;
    private final CuratorFactory curatorFactory;
    private final TypeCacheRefresher typeCacheRefresher;
    private final boolean isActiveActiveHAEnabled;

    @Inject
    public TypesREST(AtlasTypeDefStore typeDefStore, CuratorFactory curatorFactory, Configuration configuration, TypeCacheRefresher typeCacheRefresher) {
        this.typeDefStore = typeDefStore;
        this.curatorFactory = curatorFactory;
        this.typeCacheRefresher = typeCacheRefresher;
        this.zkRoot = HAConfiguration.getZookeeperProperties(configuration).getZkRoot();
        this.isActiveActiveHAEnabled = HAConfiguration.isActiveActiveHAEnabled(configuration);
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

    private InterProcessMutex attemptAcquiringLock() throws AtlasBaseException {
        if (!isActiveActiveHAEnabled)
            return null;

        final String traceId = RequestContext.get().getTraceId();
        final InterProcessMutex lock = curatorFactory.lockInstance(zkRoot, TYPE_DEF_LOCK);
        try {
            if (!lock.acquire(1, TimeUnit.MILLISECONDS)) {
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
        return lock;
    }

    private void releaseLock(InterProcessMutex lock) throws AtlasBaseException {
        if (lock == null)
            return;
        try {
            if(lock.isOwnedByCurrentThread()) {
                final String traceId = RequestContext.get().getTraceId();
                LOG.info("About to release type-def lock :: traceId {}", traceId);
                lock.release();
                LOG.info("successfully released type-def lock :: traceId {}", traceId);
            }
        } catch (Exception e) {
          throw new AtlasBaseException(e.getMessage(),e);
        }
    }

    /* Bulk API operation */

    /**
     * Bulk create APIs for all atlas type definitions, only new definitions will be created.
     * Any changes to the existing definitions will be discarded
     * @param typesDef A composite wrapper object with corresponding lists of the type definition
     * @return A composite wrapper object with lists of type definitions that were successfully
     * created
     * @throws Exception
     * @HTTP 200 On successful update of requested type definitions
     * @HTTP 400 On validation failure for any type definitions
     */
    @POST
    @Path("/typedefs")
    @Timed
    public AtlasTypesDef createAtlasTypeDefs(final AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        InterProcessMutex lock = null;
        RequestContext.get().setTraceId(UUID.randomUUID().toString());
        try {
            typeCacheRefresher.verifyCacheRefresherHealth();
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.createAtlasTypeDefs(" +
                                                               AtlasTypeUtil.toDebugString(typesDef) + ")");
            }
            lock = attemptAcquiringLock();
            typesDef.getBusinessMetadataDefs().forEach(AtlasBusinessMetadataDef::setRandomNameForEntityAndAttributeDefs);
            typesDef.getClassificationDefs().forEach(AtlasClassificationDef::setRandomNameForEntityAndAttributeDefs);
            AtlasTypesDef atlasTypesDef = typeDefStore.createTypesDef(typesDef);
            typeCacheRefresher.refreshAllHostCache();
            return atlasTypesDef;
        } catch (AtlasBaseException atlasBaseException) {
            LOG.error("TypesREST.createAtlasTypeDefs:: " + atlasBaseException.getMessage(), atlasBaseException);
            throw atlasBaseException;
        } catch (Exception e) {
            LOG.error("TypesREST.createAtlasTypeDefs:: " + e.getMessage(), e);
            throw new AtlasBaseException("Error while creating a type definition");
        }
        finally {
            releaseLock(lock);
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
    public AtlasTypesDef updateAtlasTypeDefs(final AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        InterProcessMutex lock = null;
        RequestContext.get().setTraceId(UUID.randomUUID().toString());
        try {
            typeCacheRefresher.verifyCacheRefresherHealth();
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.updateAtlasTypeDefs(" +
                                                               AtlasTypeUtil.toDebugString(typesDef) + ")");
            }
            lock = attemptAcquiringLock();

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
            AtlasTypesDef atlasTypesDef = typeDefStore.updateTypesDef(typesDef);
            typeCacheRefresher.refreshAllHostCache();
            return atlasTypesDef;
        } catch (AtlasBaseException atlasBaseException) {
            LOG.error("TypesREST.updateAtlasTypeDefs:: " + atlasBaseException.getMessage(), atlasBaseException);
            throw atlasBaseException;
        } catch (Exception e) {
            LOG.error("TypesREST.updateAtlasTypeDefs:: " + e.getMessage(), e);
            throw new AtlasBaseException("Error while updating a type definition");
        } finally {
            releaseLock(lock);
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
        InterProcessMutex lock = null;
        RequestContext.get().setTraceId(UUID.randomUUID().toString());
        try {
            typeCacheRefresher.verifyCacheRefresherHealth();
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.deleteAtlasTypeDefs(" +
                                                               AtlasTypeUtil.toDebugString(typesDef) + ")");
            }
            lock = attemptAcquiringLock();
            typeDefStore.deleteTypesDef(typesDef);
            typeCacheRefresher.refreshAllHostCache();
        } catch (AtlasBaseException atlasBaseException) {
            LOG.error("TypesREST.deleteAtlasTypeDefs:: " + atlasBaseException.getMessage(), atlasBaseException);
            throw atlasBaseException;
        } catch (Exception e) {
            LOG.error("TypesREST.deleteAtlasTypeDefs:: " + e.getMessage(), e);
            throw new AtlasBaseException("Error while deleting a type definition");
        } finally {
            releaseLock(lock);
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
        InterProcessMutex lock = null;
        RequestContext.get().setTraceId(UUID.randomUUID().toString());
        try {
            typeCacheRefresher.verifyCacheRefresherHealth();
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.deleteAtlasTypeByName(" + typeName + ")");
            }
            lock = attemptAcquiringLock();
            typeDefStore.deleteTypeByName(typeName);
            typeCacheRefresher.refreshAllHostCache();
        } catch (AtlasBaseException atlasBaseException) {
            LOG.error("TypesREST.deleteAtlasTypeByName:: " + atlasBaseException.getMessage(), atlasBaseException);
            throw atlasBaseException;
        } catch (Exception e) {
            LOG.error("TypesREST.deleteAtlasTypeByName:: " + e.getMessage(), e);
            throw new AtlasBaseException("Error while deleting a type definition");
        } finally {
            releaseLock(lock);
            AtlasPerfTracer.log(perf);
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
}
