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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import org.apache.atlas.odf.admin.rest.RestUtils;
import org.apache.atlas.odf.api.metadata.InternalMetaDataUtils;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.MetadataStoreException;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.json.JSONUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/metadata")
@Api(value = "/metadata", description = "Populate and query metadata repository", produces = MediaType.APPLICATION_JSON)
public class MetadataResource {
	private Logger logger = Logger.getLogger(MetadataResource.class.getName());

	@GET
	@Path("/connectiontest")
	public Response testConnection() {
		try {
			MetadataStore mds = new ODFFactory().create().getMetadataStore();
			MetadataStore.ConnectionStatus status = mds.testConnection();
			switch (status) {
			case OK:
				return Response.ok().build();
			case AUTHORIZATION_FAILED:
				return Response.status(Status.UNAUTHORIZED).build();
			case UNREACHABLE:
				return Response.status(Status.NOT_FOUND).build();
			default:
				return Response.status(Status.INTERNAL_SERVER_ERROR).build();
			}
		} catch (Exception e) {
			logger.log(Level.WARNING, "An exception occurred while getting metatdata store properties", e);
			return RestUtils.createErrorResponse(e);
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get metadata store properties", httpMethod = "GET", notes = "Retrieve type and URL of underlying metadata store", response = Response.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response getMetadataStoreProperties() {
		try {
			JSONObject result = new JSONObject();
			MetadataStore mds = new ODFFactory().create().getMetadataStore();
			Hashtable<Object, Object> propertyHashtable = (Hashtable<Object, Object>) mds.getProperties();
			for (Object propKey : propertyHashtable.keySet()) {
				result.put((String) propKey, (String) propertyHashtable.get(propKey));
			}
			String s = result.write();
			return Response.ok(s).build();
		} catch (Exception e) {
			logger.log(Level.WARNING, "An exception occurred while getting metatdata store properties", e);
			return RestUtils.createErrorResponse(e);
		}
	}

	@GET
	@Path("/referencetypes")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Get list of available reference types", httpMethod = "GET", notes = "Retrieve list of supported metadata object reference types", responseContainer="List", response = MetaDataObject.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response getReferenceTypes() {
		JSONObject result = new JSONObject();
		List<String> referenceTypes = null;
		try {
			MetadataStore mds = new ODFFactory().create().getMetadataStore();
			referenceTypes = mds.getReferenceTypes();
			result = JSONUtils.toJSONObject(referenceTypes);
			return Response.ok(result.write()).build();
		} catch (JSONException e) {
			logger.warning("Parse exception " + e.getMessage() + " Parsed object: " + referenceTypes);
			return RestUtils.createErrorResponse(e);
		}
	}

	@GET
	@Path("/asset/{assetReference}")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Retrieve asset by reference", httpMethod = "GET", notes = "Retrieve object from metadata repository", response = MetaDataObject.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response retrieveAsset(@ApiParam(value = "Metadata object reference id", required = true) @PathParam("assetReference") String assetReference) {
		JSONObject result;
		try {
			MetaDataObjectReference ref = JSONUtils.fromJSON(assetReference, MetaDataObjectReference.class);
			MetadataStore mds = new ODFFactory().create().getMetadataStore();
			MetaDataObject mdo = mds.retrieve(ref);
			if (mdo != null) {
				result = JSONUtils.toJSONObject(mdo);
			} else {
				// Return empty JSON document to indicate that the result should be null.
				result = new JSONObject();
			}
			return Response.ok(result.write()).build();
		} catch (JSONException e) {
			logger.warning("Parse exception " + e.getMessage() + " Parsed object: " + assetReference);
			return RestUtils.createErrorResponse(e);
		}
	}

	@GET
	@Path("/asset/{assetReference}/{referenceType}")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Retrieve objects referenced by an asset", httpMethod = "GET", notes = "Retrieve referenced metadata objects by reference type", responseContainer="List", response = MetaDataObject.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response retrieveAssetReferences(
			@ApiParam(value = "Metadata object reference", required = true) @PathParam("assetReference") String assetReference,
			@ApiParam(value = "Reference type name (including 'PARENT' and 'CHILDREN')", required = true) @PathParam("referenceType") String referenceType) {
		try {
			MetaDataObjectReference ref = JSONUtils.fromJSON(assetReference, MetaDataObjectReference.class);
			MetadataStore mds = new ODFFactory().create().getMetadataStore();
			List<MetaDataObject> referencedObjects = new ArrayList<MetaDataObject>();
			if (InternalMetaDataUtils.ODF_PARENT_REFERENCE.equals(referenceType.toUpperCase())) {
				MetaDataObject parent = mds.getParent(mds.retrieve(ref));
				if (parent != null) {
					referencedObjects.add(parent);
				}
			} else if (InternalMetaDataUtils.ODF_CHILDREN_REFERENCE.toString().equals(referenceType.toUpperCase())) {
				referencedObjects = mds.getChildren(mds.retrieve(ref));
			} else {
				referencedObjects = mds.getReferences(referenceType.toUpperCase(), mds.retrieve(ref));
			}
			List<JSONObject> jsons = new ArrayList<JSONObject>();
			for (MetaDataObject obj : referencedObjects) {
				jsons.add(JSONUtils.toJSONObject(obj));
			}
			String result = JSONUtils.toJSON(jsons);
			logger.log(Level.FINE, "Serialized JSON: {0}", result);
			return Response.ok(result).build();
		} catch (JSONException e) {
			logger.warning("Parse exception " + e.getMessage() + " Parsed object: " + assetReference);
			return RestUtils.createErrorResponse(e);
		}
	}

	@GET
	@Path("/sampledata")
	@ApiOperation(value = "Create sample data", httpMethod = "GET", notes = "Populate metadata repository with ODF sample metadata", response = Response.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response createSampleData() {
		try {
			MetadataStore mds = new ODFFactory().create().getMetadataStore();
			mds.createSampleData();
			return Response.ok().build();
		} catch (Exception exc) {
			exc.printStackTrace();
			throw new RuntimeException(exc);
		}
	}

	@POST
	@Path("/resetalldata")
	public Response resetAllData() {
		try {
			MetadataStore mds = new ODFFactory().create().getMetadataStore();
			mds.resetAllData();
			return Response.ok().build();
		} catch (Exception e) {
			logger.log(Level.WARNING, "An exception occurred while resetting metatdata store", e);
			return RestUtils.createErrorResponse(e);
		}
	}

	@GET
	@Path("/search")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Query metadata repository", httpMethod = "GET", notes = "Search for objects in metadata repository", responseContainer="List", response = MetaDataObjectReference.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Internal server error") })
	public Response search(@ApiParam(value = "Query to be sent to metadata repository (refer to Atlas query notation)", required = true) @QueryParam("query") String query,
			@ApiParam(value = "Type of results to be returned, 'objects' vs. 'references'", required = false) @QueryParam("resulttype") String resultType) {
		List<MetaDataObjectReference> queryResults;
		try {
			MetadataStore mds = new ODFFactory().create().getMetadataStore();
			try {
				queryResults = mds.search(query);
			} catch(MetadataStoreException e) {
				logger.log(Level.WARNING, MessageFormat.format("Error processing query ''{0}''.", query), e);
				return Response.status(Status.BAD_REQUEST).build();
			}
			List<JSONObject> jsons = new ArrayList<JSONObject>();
			if ((resultType != null) && resultType.equals("references")) {
				for (MetaDataObjectReference ref : queryResults) {
					jsons.add(JSONUtils.toJSONObject(ref));
				}
			} else {
				// TODO very slow, retrieve results in bulk ?!?
				//FIXME serialization of each object on its own is necessary because of a jackson issue (https://github.com/FasterXML/jackson-databind/issues/336)
				//this should be replaced by a custom objectmapper initialization, issue #59 in gitlab
				for (MetaDataObjectReference ref : queryResults) {
					MetaDataObject retrievedMdo = mds.retrieve(ref);
					jsons.add(JSONUtils.toJSONObject(retrievedMdo));
				}
			}
			String result = JSONUtils.toJSON(jsons);
			logger.log(Level.FINE, "Serialized JSON: {0}", result);
			return Response.ok(result).build();
		} catch (Exception exc) {
			exc.printStackTrace();
			throw new RuntimeException(exc);
		}
	}
}
