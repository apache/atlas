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

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.web.util.Servlets;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * REST for a single entity
 */
@Path("v2/entity")
@Singleton
public class EntityRest {

    /**
     * Create or Update an entity if it  already exists
     *
     * @param entity The updated entity
     * @return
     */
    @POST
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse createOrUpdate(AtlasEntity entity) {
        return null;
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
    public EntityMutationResponse updateByGuid(@PathParam("guid") String guid, AtlasEntity entity, @DefaultValue("false") @QueryParam("partialUpdate") boolean partialUpdate) {
        return null;
    }


    /**
     * Fetch the complete definition of an entity given its GUID.
     *
     * @param guid GUID for the entity
     */
    @GET
    @Path("/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntity getByGuid(@PathParam("guid") String guid) {
        return null;
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
    public EntityMutationResponse deleteByGuid(@PathParam("guid") String guid) {
        return null;
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
        return null;
    }

    @Deprecated
    @DELETE
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Path("/uniqueAttribute/type/{typeName}/attribute/{attrName}")
    public EntityMutationResponse deleteByUniqueAttribute(@PathParam("typeName") String entityType,
        @PathParam("attrName") String attribute,
        @QueryParam("value") String value) throws Exception {
        return null;
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
        @QueryParam("value") String value) {
        return null;
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
    public AtlasClassification.AtlasClassifications getClassification(@PathParam("guid") String guid, @PathParam("classificationName") String classificationName) {
        return null;
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
    public AtlasClassification.AtlasClassifications getClassifications(@PathParam("guid") String guid) {
        return null;
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
    public void addClassifications(@PathParam("guid") final String guid, List<AtlasClassification> classifications) {
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
    public void updateClassifications(@PathParam("guid") final String guid, List<AtlasClassification> classifications) {
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
        @PathParam("classificationName") String classificationName) {
    }
}
