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

import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.atlas.odf.api.analysis.AnalysisCancelResult;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.admin.rest.RestUtils;
import org.apache.atlas.odf.api.analysis.AnalysisRequestSummary;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackers;
import org.apache.atlas.odf.api.ODFFactory;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/analyses")
@Api(value = "/analyses", description = "Create and view analysis requests", produces = MediaType.APPLICATION_JSON)
public class AnalysesResource {
	private Logger logger = Logger.getLogger(AnalysesResource.class.getName());

	@GET
	@Path("/stats")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get analysis request statistics", httpMethod = "GET", notes = "Return number of successfull and failing analysis requests", response = AnalysisRequestSummary.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getStats() {
		try {
			return Response.ok(JSONUtils.toJSON(new ODFFactory().create().getAnalysisManager().getAnalysisStats())).build();
		} catch (JSONException e) {
			e.printStackTrace();
			logger.info("Parse exception " + e);
			return RestUtils.createErrorResponse(e);
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get list of analysis requests", httpMethod = "GET", notes = "Retrieve list of recent analysis requests (from latest to oldest)", responseContainer="List", response = AnalysisRequestTrackers.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getAnalysisRequests(
			@ApiParam(value = "Starting offset (use 0 to start with the latest request).", required = false)
			@DefaultValue("0") @QueryParam("offset") int offset,
			@ApiParam(value = "Maximum number of analysis requests to be returned (use -1 to retrieve all requests).", required = false)
			@DefaultValue("10") @QueryParam("limit") int limit) {
		try {
			String result = JSONUtils.toJSON(new ODFFactory().create().getAnalysisManager().getAnalysisRequests(offset, limit));
			return Response.ok(result).build();
		} catch (Exception exc) {
			throw new RuntimeException(exc);
		}
	}

	@GET
	@Path("/{requestId}")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get analysis request status", httpMethod = "GET", notes = "Show status of a specific analysis request", response = AnalysisRequestStatus.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getAnalysisStatus(
			@ApiParam(value = "ID of the analysis request", required = true)
			@PathParam("requestId") String requestId) {
		logger.entering(AnalysesResource.class.getName(), "getAnalysisStatus");
		AnalysisRequestStatus analysisRequestStatus = new ODFFactory().create().getAnalysisManager().getAnalysisRequestStatus(requestId);
		try {
			return Response.ok(JSONUtils.toJSON(analysisRequestStatus)).build();
		} catch (JSONException e) {
			e.printStackTrace();
			logger.info("Parse exception " + e);
			return RestUtils.createErrorResponse(e);
		}
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Run analysis", httpMethod = "POST", notes = "Create and run new analysis request", response = AnalysisResponse.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response startAnalysis(@ApiParam(value = "Analysis request to be started", required = true) AnalysisRequest request) {
		logger.entering(AnalysesResource.class.getName(), "startAnalysis");
		try {
			AnalysisResponse analysisResponse = new ODFFactory().create().getAnalysisManager().runAnalysis(request);
			return Response.ok(JSONUtils.toJSON(analysisResponse)).build();
		} catch (JSONException e) {
			e.printStackTrace();
			logger.info("Parse exception " + e);
			return RestUtils.createErrorResponse(e);
		}
	}

	@POST
	@Path("/{requestId}/cancel")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Cancel analysis request", httpMethod = "POST", notes = "Cancel a queued analysis request that has not been started yet", response = Response.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request - The request with the provided id could not be found"),
			@ApiResponse(code = 403, message = "Forbidden - The status of the analysis request does not allow for cancellation")
	})
	public Response cancelAnalysisRequest(@ApiParam(value = "ID of the analysis request", required = true) @PathParam("requestId") String requestId) {
		logger.entering(AnalysesResource.class.getName(), "cancelAnalysisRequest");
		AnalysisCancelResult result = new ODFFactory().create().getAnalysisManager().cancelAnalysisRequest(requestId);
		if (result.getState() == AnalysisCancelResult.State.NOT_FOUND) {
			return Response.status(Status.BAD_REQUEST).build();
		} else if (result.getState() == AnalysisCancelResult.State.INVALID_STATE) {
			return Response.status(Status.FORBIDDEN).build();
		}
		return Response.ok().build();
	}

}
