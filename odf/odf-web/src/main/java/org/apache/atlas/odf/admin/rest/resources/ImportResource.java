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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.atlas.odf.api.metadata.importer.JDBCMetadataImportResult;
import org.apache.atlas.odf.api.metadata.importer.JDBCMetadataImporter;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import org.apache.atlas.odf.admin.rest.RestUtils;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.metadata.importer.MetadataImportException;
import org.apache.atlas.odf.api.metadata.models.JDBCConnection;
import org.apache.atlas.odf.json.JSONUtils;

@Path("/import")
public class ImportResource {
	private Logger logger = Logger.getLogger(ImportResource.class.getName());

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response doImport(String parameterString) {
		logger.entering(ImportResource.class.getName(), "import");
		try {
			JSONObject parameter = new JSONObject(parameterString);

			Object jdbcObj = parameter.get("jdbcString");
			Object userObj = parameter.get("user");
			Object passwordObj = parameter.get("password");
			Object dbObj = parameter.get("database");
			Object schemaObj = parameter.get("schema");
			Object tableObj = parameter.get("table");

			if (jdbcObj == null || userObj == null || passwordObj == null) {
				return RestUtils.createErrorResponse("jdbcString, user, password, database, schema and table are required!");
			}

			String user = (String) userObj;
			String password = (String) passwordObj;
			String jdbcString = (String) jdbcObj;
			String db = (String) dbObj;
			String schema = (String) schemaObj;
			String table = (String) tableObj;

			JDBCMetadataImporter importer = new ODFFactory().create().getJDBCMetadataImporter();
			JDBCConnection conn = new JDBCConnection();
			conn.setJdbcConnectionString(jdbcString);
			conn.setUser(user);
			conn.setPassword(password);

			JDBCMetadataImportResult result = null;
			try {
				result = importer.importTables(conn, db, schema, table);
			} catch (MetadataImportException ex) {
				return RestUtils.createErrorResponse(ex.getMessage());
			}

			if (result == null) {
				return Response.serverError().build();
			}

			return Response.ok(JSONUtils.toJSON(result)).build();
		} catch (JSONException e) {
			return RestUtils.createErrorResponse(e.getMessage());
		}
	}
}
