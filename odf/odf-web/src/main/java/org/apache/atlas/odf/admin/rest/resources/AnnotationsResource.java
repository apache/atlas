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

import org.apache.atlas.odf.admin.rest.RestUtils;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.annotation.AnnotationStoreUtils;
import org.apache.atlas.odf.api.annotation.Annotations;
import org.apache.atlas.odf.json.JSONUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/annotations")
@Api(value = "/annotations", description = "Create and query ODF annotations", produces = MediaType.APPLICATION_JSON)
public class AnnotationsResource {

	Logger logger = Logger.getLogger(AnnotationsResource.class.getName());

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Retrieve annotations", httpMethod = "GET", notes = "Retrieve annotations for an asset and/or for a specific analysis request.", response = Annotations.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response retrieveAnnotationsForAsset(@ApiParam(value = "Reference ID of the asset", required = false) @QueryParam("assetReference") String assetReference,
			@ApiParam(value = "Analysis request ID", required = false) @QueryParam("analysisRequestId") String analysisRequestId) {
		try {
			MetaDataObjectReference ref = null;
			if (assetReference != null) {
				ref = new MetaDataObjectReference();
				String repoId = new ODFFactory().create().getMetadataStore().getRepositoryId();
				ref.setRepositoryId(repoId);
				ref.setId(assetReference);
			}
			AnnotationStore as = new ODFFactory().create().getAnnotationStore();
			List<Annotation> annots = as.getAnnotations(ref, analysisRequestId);
			Annotations result = new Annotations();
			result.setAnnotations(annots);
			return Response.ok(JSONUtils.toJSON(result)).build();
		} catch (Exception exc) {
			logger.log(Level.WARNING, "An exception occurred while retrieving annotations", exc);
			return RestUtils.createErrorResponse(exc);
		}
	}


	@GET
	@Path("/objects/{objectReference}")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Retrieve annotation", httpMethod = "GET", notes = "Retrieve annotation by Id.", response = Annotation.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response retrieveAnnotation(@ApiParam(value = "Reference ID of the annotation", required = true) @PathParam("objectReference") String objectReference) {
		try {
			MetaDataObjectReference ref = new MetaDataObjectReference();
			AnnotationStore as = new ODFFactory().create().getAnnotationStore();
			ref.setRepositoryId(as.getRepositoryId());
			ref.setId(objectReference);
			Annotation annot = as.retrieveAnnotation(ref);
			return Response.ok(JSONUtils.toJSON(annot)).build();
		} catch (Exception exc) {
			logger.log(Level.WARNING, "An exception occurred while retrieving annotation", exc);
			return RestUtils.createErrorResponse(exc);
		}
	}


	// no swagger documentation as this will be replaced by "annotation propagation"
	@GET
	@Path("/newestAnnotations/{assetReference}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response retrieveMostRecentAnnotations(@PathParam("assetReference") String assetReference) {
		try {
			MetaDataObjectReference ref = JSONUtils.fromJSON(assetReference, MetaDataObjectReference.class);
			AnnotationStore as = new ODFFactory().create().getAnnotationStore();
			List<Annotation> annotations = AnnotationStoreUtils.getMostRecentAnnotationsByType(as, ref);
			String result = JSONUtils.toJSON(annotations);
			return Response.ok(result).build();
		} catch (Exception e) {
			logger.log(Level.WARNING, "An exception occurred while retrieving most recent annotations", e);
			return RestUtils.createErrorResponse(e);
		}
	}

	@POST
	@ApiOperation(value = "Create annotation", httpMethod = "POST", notes = "Create new annotation object", response = MetaDataObjectReference.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"), @ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 500, message = "Internal server error") })
	public Response createAnnotation(@ApiParam(value = "Analysis request to be started", required = true) String annotString) {
		try {
			Annotation annot = JSONUtils.fromJSON(annotString, Annotation.class);
			AnnotationStore as = new ODFFactory().create().getAnnotationStore();
			MetaDataObjectReference annotRef = as.store(annot);
			return Response.status(Status.CREATED).entity(JSONUtils.toJSON(annotRef)).build();
		} catch (Exception exc) {
			logger.log(Level.WARNING, "An exception occurred while storing an annotation", exc);
			return RestUtils.createErrorResponse(exc);
		}
	}

}
