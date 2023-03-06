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

import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.TaskSearchParams;
import org.apache.atlas.model.tasks.TaskSearchResult;
import org.apache.atlas.tasks.TaskService;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

/**
 * REST interface for CRUD operations on tasks
 */
@Path("task")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class TaskREST {
    private static final Logger LOG      = LoggerFactory.getLogger(TaskREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.TaskREST");

    private TaskService taskService;

    @Inject
    public TaskREST(TaskService taskService) {
        this.taskService = taskService;
    }

    @POST
    @Path("search")
    @Timed
    public TaskSearchResult getTasks(TaskSearchParams parameters) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getTasks");
            }

            TaskSearchResult ret = taskService.getTasks(parameters);

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @PUT
    @Path("retry/{guid}")
    @Timed
    public HttpStatus retryTask(@PathParam("guid") final String guid) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.retryTask");
            }

            taskService.retryTask(guid);

            return HttpStatus.OK;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}
