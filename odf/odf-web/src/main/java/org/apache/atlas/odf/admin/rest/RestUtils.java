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
package org.apache.atlas.odf.admin.rest;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

public class RestUtils {
	public static Response createErrorResponse(Throwable t) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		t.printStackTrace(pw);
		return createErrorResponse(sw.toString());
	}

	public static Response createErrorResponse(String msg) {
		Logger logger = Logger.getLogger(RestUtils.class.getName());
		logger.log(Level.WARNING, "An unknown exception was thrown: ''{0}''", msg);
		String errorMsg = "{ \"error\": \"An unknown exception occurred\"}";
		try {
			JSONObject errorJSON = new JSONObject();
			errorJSON.put("error", msg);
			errorMsg = errorJSON.write();
		} catch (JSONException e) {
			// do nothing, should never happen
		}
		return Response.status(Status.BAD_REQUEST).entity(errorMsg).build();
	}
}
