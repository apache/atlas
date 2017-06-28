/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.odf.admin.rest.resources;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.atlas.odf.api.engine.SystemHealth;
import org.apache.atlas.odf.api.utils.ODFLogConfig;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.admin.log.LoggingHandler;
import org.apache.atlas.odf.admin.rest.RestUtils;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.engine.ODFEngineOptions;
import org.apache.atlas.odf.api.engine.ODFStatus;
import org.apache.atlas.odf.api.engine.ODFVersion;
import org.apache.atlas.odf.api.engine.ServiceRuntimesInfo;
import org.apache.atlas.odf.json.JSONUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/engine")
@Api(value = "/engine", description = "Monitor and control the ODF engine", produces = MediaType.APPLICATION_JSON)
public class EngineResource {
	final static LoggingHandler REST_LOG_HANDLER = new LoggingHandler();

	static {
		//initialize log config and log handler to cache logs
		ODFLogConfig.run();
		Logger rootLogger = Logger.getLogger("org.apache.atlas.odf");
		REST_LOG_HANDLER.setLevel(Level.ALL);
		rootLogger.addHandler(REST_LOG_HANDLER);
	}

	private Logger logger = Logger.getLogger(EngineResource.class.getName());

	@POST
	@Path("shutdown")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Shutdown ODF engine", httpMethod = "POST", notes = "Shutdown ODF engine, purge all scheduled analysis requests from the queues, and cancel all running analysis requests (for debugging purposes only)", response = Response.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response shutdown(@ApiParam(value = "Engine options", defaultValue = "false", required = true) ODFEngineOptions engineOptions) {
		logger.entering(EngineResource.class.getName(), "shutdown");
		logger.log(Level.INFO, "Restart option is ", engineOptions.isRestart());
		new ODFFactory().create().getEngineManager().shutdown(engineOptions);
		return Response.ok().build();
	}

	@GET
	@Path("health")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get health status", httpMethod = "GET", notes = "Check the health status of ODF", response = SystemHealth.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response healthCheck() {
		logger.entering(EngineResource.class.getName(), "healthCheck");
		SystemHealth health = new ODFFactory().create().getEngineManager().checkHealthStatus();
		Status status = Status.OK;
		try {
			return Response.status(status).entity(JSONUtils.toJSON(health)).type(MediaType.APPLICATION_JSON).build();
		} catch (JSONException e) {
			e.printStackTrace();
			logger.info("Parse exception " + e);
			return RestUtils.createErrorResponse(e);
		}
	}

	@GET
	@Path("runtimes")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get info about the available runtimes", httpMethod = "GET", notes = "Get information about all runtimes running discovery services", response = SystemHealth.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response getRuntimesInfo() {
		logger.entering(EngineResource.class.getName(), "getRuntimesInfo");
		ServiceRuntimesInfo sri = new ODFFactory().create().getEngineManager().getRuntimesInfo();
		Status status = Status.OK;
		try {
			return Response.status(status).entity(JSONUtils.toJSON(sri)).type(MediaType.APPLICATION_JSON).build();
		} catch (JSONException e) {
			e.printStackTrace();
			logger.info("Parse exception " + e);
			return RestUtils.createErrorResponse(e);
		} finally {
			logger.exiting(EngineResource.class.getName(), "getRuntimesInfo");
		}
	}

	@GET
	@Path("status")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get current status", httpMethod = "GET", notes = "Retrieve status of the messaging subsystem and the internal thread manager", response = ODFStatus.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response getStatus() throws IOException {
		logger.entering(EngineResource.class.getName(), "getStatus");
		try {
			ODFStatus odfStatus = new ODFFactory().create().getEngineManager().getStatus();
			return Response.status(Status.OK).entity(JSONUtils.toJSON(odfStatus)).type(MediaType.APPLICATION_JSON).build();
		} catch (Exception exc) {
			logger.log(Level.INFO, "An exception occurred while getting the request status", exc);
			return RestUtils.createErrorResponse(exc);
		}
	}

	@GET
	@Path("log")
	@Produces(MediaType.TEXT_PLAIN)
	@ApiOperation(value = "Get current application log", httpMethod = "GET", notes = "Retrieve logs of the ODF instance", response = ODFStatus.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response getLog(@QueryParam("numberOfLogs") Integer numberOfLogs, @QueryParam("logLevel") String logLevel) throws IOException {
		logger.entering(EngineResource.class.getName(), "getLog");
		try {
			Level level = Level.ALL;
			if (logLevel != null) {
				level = Level.parse(logLevel);
			}
			return Response.status(Status.OK).entity(REST_LOG_HANDLER.getFormattedCachedLog(numberOfLogs, level)).type(MediaType.TEXT_PLAIN).build();
		} catch (Exception exc) {
			logger.log(Level.INFO, "An exception occurred while getting the ODF log", exc);
			return RestUtils.createErrorResponse(exc);
		}
	}

	@GET
	@Path("version")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get the ODF build version", httpMethod = "GET", notes = "The version is of the form versionnumber-buildid, e.g., 0.1.0-154", response = ODFVersion.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response getVersion() {
		try {
			ODFVersion version = new ODFFactory().create().getEngineManager().getVersion();
			Status status = Status.OK;
			return Response.status(status).entity(JSONUtils.toJSON(version)).type(MediaType.APPLICATION_JSON).build();
		} catch (Exception exc) {
			logger.log(Level.INFO, "An exception occurred while getting the version", exc);
			return RestUtils.createErrorResponse(exc);
		}

	}
}
