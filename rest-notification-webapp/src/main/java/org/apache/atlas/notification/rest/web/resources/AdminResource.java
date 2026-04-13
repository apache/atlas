/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.notification.rest.web.resources;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.notification.rest.Servlets;
import org.apache.atlas.notification.rest.web.service.ServiceState;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

/**
 * Jersey Resource for admin operations.
 */
@Path("api/atlas/admin")
@Singleton
@Service
@EnableScheduling
public class AdminResource {
    private static final Logger LOG = LoggerFactory.getLogger(AdminResource.class);


    private static Configuration atlasProperties;
    private final  ServiceState  serviceState;

    static {
        try {
            atlasProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }
    }

    @Inject
    public AdminResource(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    /**
     * Fetches the thread stack dump for this application.
     *
     * @return json representing the thread stack dump.
     */
    @GET
    @Path("stack")
    @Produces(MediaType.TEXT_PLAIN)
    public String getThreadDump() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getThreadDump()");
        }

        ThreadGroup topThreadGroup = Thread.currentThread().getThreadGroup();

        while (topThreadGroup.getParent() != null) {
            topThreadGroup = topThreadGroup.getParent();
        }
        Thread[] threads = new Thread[topThreadGroup.activeCount()];

        int nr = topThreadGroup.enumerate(threads);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < nr; i++) {
            builder.append(threads[i].getName()).append("\nState: ").
                    append(threads[i].getState()).append("\n");
            String stackTrace = StringUtils.join(threads[i].getStackTrace(), "\n");
            builder.append(stackTrace);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getThreadDump()");
        }

        return builder.toString();
    }


    @GET
    @Path("status")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getStatus() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getStatus()");
        }

        Map<String, Object> responseData = new HashMap() {{
            put(AtlasClient.STATUS, serviceState.getState().toString());
        }};

        Response response = Response.ok(AtlasJson.toV1Json(responseData)).build();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getStatus()");
        }

        return response;
    }


}
