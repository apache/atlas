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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.apache.atlas.api.PList;
import org.apache.atlas.api.SearchFilter;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("types")
public class TypesREST {
    private static final Logger LOG = LoggerFactory.getLogger(TypesREST.class);

    @POST
    @Path("/enumdef")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEnumDef createEnumDef(AtlasEnumDef enumDef) throws Exception {
        AtlasEnumDef ret = null;

        // TODO: ret = store.createEnumDef()

        return ret;
    }

    @GET
    @Path("/enumdef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEnumDef getEnumDefByName(@PathParam("name") String name) throws Exception {
        AtlasEnumDef ret = null;

        // TODO: ret = store.getEnumDefByName(name)

        return ret;
    }

    @GET
    @Path("/enumdef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEnumDef getEnumDefByGuid(@PathParam("guid") String guid) throws Exception {
        AtlasEnumDef ret = null;

        // TODO: ret = store.getEnumDefByGuid(guid)

        return ret;
    }

    @PUT
    @Path("/enumdef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEnumDef updateEnumDefByName(@PathParam("name") String name, AtlasEnumDef enumDef) throws Exception {
        AtlasEnumDef ret = null;

        // TODO: ret = store.updateEnumDefByName(name, enumDef)

        return ret;
    }

    @PUT
    @Path("/enumdef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEnumDef updateEnumDefByGuid(@PathParam("guid") String guid, AtlasEnumDef enumDef) throws Exception {
        AtlasEnumDef ret = null;

        // TODO: ret = store.updateEnumDefByGuid(guid, enumDef)

        return ret;
    }

    @DELETE
    @Path("/enumdef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteEnumDefByName(@PathParam("name") String name) throws Exception {
        // TODO: store.deleteEnumDefByName(name)
    }

    @DELETE
    @Path("/enumdef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteEnumDefByGuid(@PathParam("guid") String guid) throws Exception {
        // TODO: store.deleteEnumDefByGuid(guid)
    }

    @GET
    @Path("/enumdef")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public PList<AtlasEnumDef> searchEnumDefs(@Context HttpServletRequest request) throws Exception {
        PList<AtlasEnumDef> ret = null;

        // TODO: SearchFilter filter = getSearchFilter(request);
        // TODO: ret = store.searchEnumDefs(filter);

        return ret;
    }


    @POST
    @Path("/structdef")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasStructDef createStructDef(AtlasStructDef structDef) throws Exception {
        AtlasStructDef ret = null;

        // TODO: ret = store.createStructDef()

        return ret;
    }

    @GET
    @Path("/structdef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasStructDef getStructDefByName(@PathParam("name") String name) throws Exception {
        AtlasStructDef ret = null;

        // TODO: ret = store.getStructDefByName(name)

        return ret;
    }

    @GET
    @Path("/structdef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasStructDef getStructDefByGuid(@PathParam("guid") String guid) throws Exception {
        AtlasStructDef ret = null;

        // TODO: ret = store.getStructDefByGuid(guid)

        return ret;
    }

    @PUT
    @Path("/structdef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasStructDef updateStructDefByName(@PathParam("name") String name, AtlasStructDef structDef) throws Exception {
        AtlasStructDef ret = null;

        // TODO: ret = store.updateStructDefByName(name, structDef)

        return ret;
    }

    @PUT
    @Path("/structdef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasStructDef updateStructDefByGuid(@PathParam("guid") String guid, AtlasStructDef structDef) throws Exception {
        AtlasStructDef ret = null;

        // TODO: ret = store.updateStructDefByGuid(guid, structDef)

        return ret;
    }

    @DELETE
    @Path("/structdef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteStructDefByName(@PathParam("name") String name) throws Exception {
        // TODO: store.deleteStructDefByName(name)
    }

    @DELETE
    @Path("/structdef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteStructDefByGuid(@PathParam("guid") String guid) throws Exception {
        // TODO: store.deleteStructDefByGuid(guid)
    }

    @GET
    @Path("/structdef")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public PList<AtlasStructDef> searchStructDefs(@Context HttpServletRequest request) throws Exception {
        PList<AtlasStructDef> ret = null;

        // TODO: SearchFilter filter = getSearchFilter(request);
        // TODO: ret = store.searchStructDefs(filter);

        return ret;
    }


    @POST
    @Path("/classificationdef")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasClassificationDef createClassificationDef(AtlasClassificationDef classificationDef) throws Exception {
        AtlasClassificationDef ret = null;

        // TODO: ret = store.createClassificationDef()

        return ret;
    }

    @GET
    @Path("/classificationdef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasClassificationDef getClassificationDefByName(@PathParam("name") String name) throws Exception {
        AtlasClassificationDef ret = null;

        // TODO: ret = store.getClassificationDefByName(name)

        return ret;
    }

    @GET
    @Path("/classificationdef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasClassificationDef getClassificationDefByGuid(@PathParam("guid") String guid) throws Exception {
        AtlasClassificationDef ret = null;

        // TODO: ret = store.getClassificationDefByGuid(guid)

        return ret;
    }

    @PUT
    @Path("/classificationdef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasClassificationDef updateClassificationDefByName(@PathParam("name") String name, AtlasClassificationDef classificationDef) throws Exception {
        AtlasClassificationDef ret = null;

        // TODO: ret = store.updateClassificationDefByName(name, classificationDef)

        return ret;
    }

    @PUT
    @Path("/classificationdef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasClassificationDef updateClassificationDefByGuid(@PathParam("guid") String guid, AtlasClassificationDef classificationDef) throws Exception {
        AtlasClassificationDef ret = null;

        // TODO: ret = store.updateClassificationDefByGuid(guid, classificationDef)

        return ret;
    }

    @DELETE
    @Path("/classificationdef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteClassificationDefByName(@PathParam("name") String name) throws Exception {
        // TODO: store.deleteClassificationDefByName(name)
    }

    @DELETE
    @Path("/classificationdef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteClassificationDefByGuid(@PathParam("guid") String guid) throws Exception {
        // TODO: store.deleteClassificationDefByGuid(guid)
    }

    @GET
    @Path("/classificationdef")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public PList<AtlasClassificationDef> searchClassificationDefs(SearchFilter filter) throws Exception {
        PList<AtlasClassificationDef> ret = null;

        // TODO: SearchFilter filter = getSearchFilter(request);
        // TODO: ret = store.searchClassificationDefs(filter);

        return ret;
    }


    @POST
    @Path("/entitydef")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntityDef createEntityDef(AtlasEntityDef entityDef) throws Exception {
        AtlasEntityDef ret = null;

        // TODO: ret = store.createEntityDef()

        return ret;
    }

    @GET
    @Path("/entitydef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntityDef getEntityDefByName(@PathParam("name") String name) throws Exception {
        AtlasEntityDef ret = null;

        // TODO: ret = store.getEntityDefByName(name)

        return ret;
    }

    @GET
    @Path("/entitydef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntityDef getEntityDefByIdByGuid(@PathParam("guid") String guid) throws Exception {
        AtlasEntityDef ret = null;

        // TODO: ret = store.getEntityDefByGuid(guid)

        return ret;
    }

    @PUT
    @Path("/entitydef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntityDef updateEntityDefByName(@PathParam("name") String name, AtlasEntityDef entityDef) throws Exception {
        AtlasEntityDef ret = null;

        // TODO: ret = store.updateEntityDefByName(name, entityDef)

        return ret;
    }

    @PUT
    @Path("/entitydef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntityDef updateEntityDefByGuid(@PathParam("guid") String guid, AtlasEntityDef entityDef) throws Exception {
        AtlasEntityDef ret = null;

        // TODO: ret = store.updateEntityDefByGuid(guid, entityDef)

        return ret;
    }

    @DELETE
    @Path("/entitydef/name/{name}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteEntityDef(@PathParam("name") String name) throws Exception {
        // TODO: store.deleteEntityDefByName(name)
    }

    @DELETE
    @Path("/entitydef/guid/{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteEntityDefByGuid(@PathParam("guid") String guid) throws Exception {
        // TODO: store.deleteEntityDefByGuid(guid)
    }

    @GET
    @Path("/entitydef")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public PList<AtlasEntityDef> searchEntityDefs(SearchFilter filter) throws Exception {
        PList<AtlasEntityDef> ret = null;

        // TODO: SearchFilter filter = getSearchFilter(request);
        // TODO: ret = store.searchEntityDefs(filter);

        return ret;
    }
}
