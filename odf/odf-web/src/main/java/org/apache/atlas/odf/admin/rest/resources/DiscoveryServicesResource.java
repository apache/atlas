/**
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

import java.io.InputStream;
import java.util.List;
import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.admin.rest.RestUtils;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRuntimeStatistics;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceStatus;
import org.apache.atlas.odf.api.discoveryservice.ServiceNotFoundException;
import org.apache.atlas.odf.api.discoveryservice.ServiceStatusCount;
import org.apache.atlas.odf.json.JSONUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/services")
@Api(value = "/services", description = "Manage ODF services", produces = MediaType.APPLICATION_JSON)
public class DiscoveryServicesResource {
	private Logger logger = Logger.getLogger(DiscoveryServicesResource.class.getName());

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get list of discovery services", httpMethod = "GET", notes = "Retrieve list of all discovery services registered in ODF", responseContainer="List", response = DiscoveryServiceProperties.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getDiscoveryServices() {
		logger.entering(DiscoveryServicesResource.class.getName(), "getServices");
		DiscoveryServiceManager dsAdmin = new ODFFactory().create().getDiscoveryServiceManager();
		Response response;
		List<DiscoveryServiceProperties> dsProperties = dsAdmin.getDiscoveryServicesProperties();
		try {
			String json = JSONUtils.toJSON(dsProperties);
			response = Response.ok(json).build();
		} catch (JSONException e) {
			e.printStackTrace();
			logger.info("Parse exception " + e);
			response = RestUtils.createErrorResponse(e);
		}
		return response;
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/status")
	@ApiOperation(value = "Get status of discovery services", httpMethod = "GET", notes = "Retrieve status overview of all discovery services registered in ODF", responseContainer="List", response = ServiceStatusCount.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 404, message = "Not found"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getAllServicesStatus() {
		logger.entering(DiscoveryServicesResource.class.getName(), "getAllServicesStatus");
		List<ServiceStatusCount> servicesStatus = new ODFFactory().create().getDiscoveryServiceManager().getDiscoveryServiceStatusOverview();
		if (servicesStatus == null) {
			return Response.status(Status.NOT_FOUND).build();
		}
		String json;
		try {
			json = JSONUtils.toJSON(servicesStatus);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
		return Response.ok(json).build();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/{serviceId}/status")
	@ApiOperation(value = "Get discovery service status", httpMethod = "GET", notes = "Retrieve status of a discovery service that is registered in ODF", response = Response.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 404, message = "Not found"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getDiscoveryServiceStatus(
			@ApiParam(value = "Discovery service ID", required = true)
			@PathParam("serviceId") String serviceId) {
		logger.entering(DiscoveryServicesResource.class.getName(), "getDiscoveryServiceStatus");
		DiscoveryServiceManager dsAdmin = new ODFFactory().create().getDiscoveryServiceManager();
		Response response;
		try {
			DiscoveryServiceStatus dsStatus = dsAdmin.getDiscoveryServiceStatus(serviceId);
			if (dsStatus == null) {
				response = Response.status(Status.NOT_FOUND).build();
			}
			else {
				try {
					String json = JSONUtils.toJSON(dsStatus);
					response = Response.ok(json).build();
				} catch (JSONException e) {
					e.printStackTrace();
					logger.info("Parse exception " + e);
					response = RestUtils.createErrorResponse(e);
				}
			}
		}
		catch (ServiceNotFoundException snfe) {
			response = Response.status(Status.NOT_FOUND).entity(snfe.getMessage()).build();
		}
		return response;
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/{serviceId}/runtimeStats")
	@ApiOperation(value = "Get runtime statistics of a discovery service", httpMethod = "GET", notes = "Retrieve the runtime statistics of a discovery service that is registered in ODF.", response = Response.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 404, message = "Not found"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getDiscoveryServiceRuntimeStats(
			@ApiParam(value = "Discovery service ID", required = true)
			@PathParam("serviceId") String serviceId) {
		logger.entering(DiscoveryServicesResource.class.getName(), "getDiscoveryServiceRuntimeStats");
		DiscoveryServiceManager dsAdmin = new ODFFactory().create().getDiscoveryServiceManager();
		Response response;
		try {
			DiscoveryServiceRuntimeStatistics dsRuntimeStats = dsAdmin.getDiscoveryServiceRuntimeStatistics(serviceId);
			String json = JSONUtils.toJSON(dsRuntimeStats);
			response = Response.ok(json).build();
		}
		catch (JSONException e) {
			e.printStackTrace();
			logger.info("Parse exception " + e);
			response = RestUtils.createErrorResponse(e);
		}
		catch (ServiceNotFoundException snfe) {
			response = Response.status(Status.NOT_FOUND).entity(snfe.getMessage()).build();
		}
		return response;
	}

	@DELETE
	@Path("/{serviceId}/runtimeStats")
	@ApiOperation(value = "Delete runtime statistics of a discovery service", httpMethod = "DELETE", notes = "Delete the runtime statistics of a discovery service that is registered in ODF.", response = Response.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 404, message = "Not found"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response deleteDiscoveryServiceRuntimeStats(
			@ApiParam(value = "Discovery service ID", required = true)
			@PathParam("serviceId") String serviceId) {
		logger.entering(DiscoveryServicesResource.class.getName(), "deleteDiscoveryServiceRuntimeStats");
		DiscoveryServiceManager dsAdmin = new ODFFactory().create().getDiscoveryServiceManager();
		Response response;
		try {
			dsAdmin.deleteDiscoveryServiceRuntimeStatistics(serviceId);
			response = Response.ok().build();
		}
		catch (ServiceNotFoundException snfe) {
			response = Response.status(Status.NOT_FOUND).entity(snfe.getMessage()).build();
		}
		return response;
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/{serviceId}")
	@ApiOperation(value = "Get properties of a discovery service registered in ODF", httpMethod = "GET", notes = "Retrieve properties of a discovery service that is registered in ODF", response = Response.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 404, message = "Not found"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getDiscoveryServiceProperties(
			@ApiParam(value = "Id string of discovery service", required = true)
			@PathParam("serviceId") String serviceId) {
		logger.entering(DiscoveryServicesResource.class.getName(), "getDiscoveryServiceProperties");
		DiscoveryServiceManager dsAdmin = new ODFFactory().create().getDiscoveryServiceManager();
		Response response;
		try {
			DiscoveryServiceProperties dsStatus = dsAdmin.getDiscoveryServiceProperties(serviceId);
			if (dsStatus == null) {
				response = Response.status(Status.NOT_FOUND).build();
			}
			else {
				try {
					String json = JSONUtils.toJSON(dsStatus);
					response = Response.ok(json).build();
				} catch (JSONException e) {
					e.printStackTrace();
					logger.info("Parse exception " + e);
					response = RestUtils.createErrorResponse(e);
				}
			}
		}
		catch (ServiceNotFoundException snfe) {
			response = Response.status(Status.NOT_FOUND).entity(snfe.getMessage()).build();
		}
		return response;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Register a discovery service", httpMethod = "POST", notes = "Register a new service in ODF", response = Response.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response registerDiscoveryService(
			@ApiParam(value = "ODF service definition", required = true) DiscoveryServiceProperties dsProperties) {
		logger.entering(DiscoveryServicesResource.class.getName(), "registerDiscoveryService");
		Response response;
		try {
			DiscoveryServiceManager dsAdmin = new ODFFactory().create().getDiscoveryServiceManager();
			dsAdmin.createDiscoveryService(dsProperties);
			response = Response.ok().build();
		} catch (ValidationException e) {
			e.printStackTrace();
			logger.info("Validation exception during setting of property " + e.getProperty());
			response = RestUtils.createErrorResponse(e.getErrorCause());
		}
		return response;
	}

	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Update properties of a discovery service", httpMethod = "POST", notes = "Update properties of a discovery service that is registered in ODF", response = Response.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response updateDiscoveryService(
			@ApiParam(value = "ODF service definition", required = true) DiscoveryServiceProperties dsProperties) {
		logger.entering(DiscoveryServicesResource.class.getName(), "updateDiscoveryService");
		Response response;
		try {
			DiscoveryServiceManager dsAdmin = new ODFFactory().create().getDiscoveryServiceManager();
			dsAdmin.replaceDiscoveryService(dsProperties);
			response = Response.ok().build();
		}
		catch (ServiceNotFoundException snfe) {
			response = Response.status(Status.NOT_FOUND).entity(snfe.getMessage()).build();
		}
		catch (ValidationException e) {
			e.printStackTrace();
			logger.info("Validation exception during setting of property " + e.getProperty());
			response = RestUtils.createErrorResponse(e.getErrorCause());
		}
		return response;
	}

	@DELETE
	@Path("/{serviceId}")
	@ApiOperation(value = "Delete a discovery service", httpMethod = "DELETE", notes = "Remove a registered service from ODF", response = Response.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response deleteDiscoveryService(
			@ApiParam(value = "Id string of discovery service to be deleted", required = true)
			@PathParam("serviceId") String serviceId) {
		logger.entering(DiscoveryServicesResource.class.getName(), "deleteDiscoveryService");
		Response response;
		try {
			DiscoveryServiceManager dsAdmin = new ODFFactory().create().getDiscoveryServiceManager();
			dsAdmin.deleteDiscoveryService(serviceId);
			response = Response.ok().build();
		}
		catch (ServiceNotFoundException snfe) {
			response = Response.status(Status.NOT_FOUND).entity(snfe.getMessage()).build();
		}
		catch (ValidationException e) {
			e.printStackTrace();
			logger.info("Validation exception during deletion. Property: " + e.getProperty());
			response = RestUtils.createErrorResponse(e.getErrorCause());
		}
		return response;
	}

	@GET
	@Path("/{serviceId}/image")
	@Produces("image/*")
	@ApiOperation(value = "Get a discovery service logo", httpMethod = "GET", notes = "Retrieve image representing a discovery service", response = InputStream.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 404, message = "Not found"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response getImage(
			@ApiParam(value = "ID of discovery service", required = true)
			@PathParam("serviceId") String serviceId) {

		DiscoveryServiceManager dsAdmin = new ODFFactory().create().getDiscoveryServiceManager();
		Response response = null;
		InputStream is;
		try {
			is = dsAdmin.getDiscoveryServiceImage(serviceId);
			if (is == null) {
				// should never happen
				response = Response.status(Status.NOT_FOUND).build();
			}
			else {
				response = Response.ok(is, "image/png").build();
			}
		} catch (ServiceNotFoundException snfe) {
			response = Response.status(Status.NOT_FOUND).entity(snfe.getMessage()).build();
		}
		return response;
	}

}
