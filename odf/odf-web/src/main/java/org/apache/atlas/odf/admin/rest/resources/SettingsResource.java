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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.atlas.odf.admin.rest.RestUtils;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.ODFFactory;

@Path("/settings")
@Api(value = "/settings", description = "View or update the settings of the Open Discovery Framework", produces = MediaType.APPLICATION_JSON)
public class SettingsResource {

	private Logger logger = Logger.getLogger(SettingsResource.class.getName());

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Retrieve settings", httpMethod = "GET", notes = "Retrieve current ODF settings", response = ODFSettings.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response getSettings() {
		logger.entering(SettingsResource.class.getName(), "getConfig");
		try {
			return Response.ok(JSONUtils.toJSON(new ODFFactory().create().getSettingsManager().getODFSettingsHidePasswords()), MediaType.APPLICATION_JSON).build();
		} catch (JSONException e) {
			e.printStackTrace();
			logger.info("Parse exception " + e);
			return RestUtils.createErrorResponse(e);
		}
	}

	@POST
	@Path("/reset")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Reset settings", httpMethod = "POST", notes = "Reset ODF settings to the default", response = Response.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response resetSettings() {
		logger.entering(SettingsResource.class.getName(), "getConfig");
		new ODFFactory().create().getSettingsManager().resetODFSettings();
		return Response.ok().build();
	}

	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Update settings", httpMethod = "PUT", notes = "Update ODF settings", response = ODFSettings.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Internal server error")
	})
	public Response changeSettings(@ApiParam(value = "ODF configuration options", required = true) ODFSettings odfConfig) {
		logger.entering(SettingsResource.class.getName(), "changeConfig");
		if (odfConfig == null) {
			return Response.status(Status.BAD_REQUEST).entity("The body must be a valid settings JSON.").build();
		}

		try {
			SettingsManager config = new ODFFactory().create().getSettingsManager();
			config.updateODFSettings(odfConfig);
			return Response.ok(JSONUtils.toJSON(config.getODFSettingsHidePasswords())).build();
		} catch (ValidationException e) {
			e.printStackTrace();
			logger.info("Validation exception during setting of property " + e.getProperty());
			return RestUtils.createErrorResponse(e);
		} catch (JSONException e1) {
			e1.printStackTrace();
			return RestUtils.createErrorResponse(MessageFormat.format("The provided input is not valid JSON in form {0}", getEmptyODFConfig()));
		}
	}

	private String getEmptyODFConfig() {
		ODFSettings odf = new ODFSettings();
		odf.setUserDefined(new HashMap<String, Object>());
		String emptyJSON = "";
		try {
			emptyJSON = JSONUtils.toJSON(odf);
		} catch (JSONException e2) {
			e2.printStackTrace();
		}
		return emptyJSON;
	}



}
