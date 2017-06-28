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
package org.apache.atlas.odf.integrationtest.annotations;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.annotation.Annotations;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.integrationtest.metadata.MetadataResourceTest;
import org.apache.atlas.odf.rest.test.RestTestBase;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.wink.json4j.JSON;
import org.apache.wink.json4j.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.json.JSONUtils;

public class AnnotationsResourceTest extends RestTestBase {
	Logger logger = Logger.getLogger(AnnotationsResourceTest.class.getName());

	@Before
	public void createSampleData() throws Exception {
		Executor exec = getRestClientManager().getAuthenticatedExecutor();
		Request req = Request.Get(getBaseURI() + "/metadata/sampledata");
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		checkResult(httpResp, HttpStatus.SC_OK);
	}

	public static class AnnotationsResourceTestProfilingAnnotation extends ProfilingAnnotation {
		private String newAnnotProp;

		public String getNewAnnotProp() {
			return newAnnotProp;
		}

		public void setNewAnnotProp(String newAnnotProp) {
			this.newAnnotProp = newAnnotProp;
		}

	}

	static String newAnnotPropValue = "newAnnotPropValue" + UUID.randomUUID().toString();
	static String newAnnotPropKey = "newAnnotProp";

	static String unknownAnnotType = "UnknownAnnotType" + UUID.randomUUID().toString();

	List<Annotation> createTestAnnotations(MetaDataObjectReference ref, String reqId) {
		List<Annotation> result = new ArrayList<>();
		AnnotationsResourceTestProfilingAnnotation annot = new AnnotationsResourceTestProfilingAnnotation();
		annot.setProfiledObject(ref);
		annot.setNewAnnotProp(newAnnotPropValue);
		annot.setAnalysisRun(reqId);
		result.add(annot);

		ProfilingAnnotation genericAnnot = new ProfilingAnnotation();
		genericAnnot.setProfiledObject(ref);
		genericAnnot.setAnalysisRun(reqId);
		genericAnnot.setJsonProperties("{\"" + newAnnotPropKey + "\": \"" + newAnnotPropValue + "\"}");
		result.add(genericAnnot);

		return result;
	}

	MetaDataObjectReference getTestDataSetRef() throws Exception {
		String s = MetadataResourceTest.getAllMetadataObjectsOfType("DataFile");
		logger.info("Retrieved test data set refs: " + s);
		List<DataFile> dfRefs = JSONUtils.fromJSONList(s, DataFile.class);
		return dfRefs.get(0).getReference();
	}

	@Test
	public void testAnnotationStore() throws Exception {
		MetaDataObjectReference dfRef = getTestDataSetRef();
		String reqId = "TestRequestId" + UUID.randomUUID().toString();
		logger.info("Test Annotatoin store with request ID: " + reqId);
		List<Annotation> newAnnots = createTestAnnotations(dfRef, reqId);

		Executor exec = getRestClientManager().getAuthenticatedExecutor();
		List<String> createdAnnotIds = new ArrayList<>();
		// create annotations
		for (Annotation annot : newAnnots) {
			String restRequestBody = JSONUtils.toJSON(annot);
			logger.info("Creating annotation via request " + restRequestBody);
			Request req = Request.Post(getBaseURI() + "/annotations").bodyString(restRequestBody, ContentType.APPLICATION_JSON);
			Response resp = exec.execute(req);
			HttpResponse httpResp = resp.returnResponse();
			checkResult(httpResp, HttpStatus.SC_CREATED);
			InputStream is = httpResp.getEntity().getContent();
			MetaDataObjectReference createdAnnot = JSONUtils.fromJSON(is, MetaDataObjectReference.class);
			Assert.assertNotNull(createdAnnot);
			Assert.assertNotNull(createdAnnot.getId());
			createdAnnotIds.add(createdAnnot.getId());
		}
		logger.info("Annotations created, now retrieving them again: " + createdAnnotIds);

		// check retrieval
		Request req = Request.Get(getBaseURI() + "/annotations?assetReference=" + dfRef.getId());
		Response resp = exec.execute(req);

		HttpResponse httpResp = resp.returnResponse();
		checkResult(httpResp, HttpStatus.SC_OK);
		Annotations retrieveResult = JSONUtils.fromJSON(httpResp.getEntity().getContent(), Annotations.class);
		List<Annotation> retrievedAnnots = retrieveResult.getAnnotations();
		logger.info("Retrieved annotations: " + retrievedAnnots);
		int foundAnnots = 0;
		for (Annotation retrievedAnnot : retrievedAnnots) {
			logger.info("Checking annotation: " + retrievedAnnot.getReference());
			logger.info("Annotation " + retrievedAnnot.getReference().getId() + " has request ID: " + retrievedAnnot.getAnalysisRun());
			if (reqId.equals(retrievedAnnot.getAnalysisRun())) {
				logger.info("Checking annotation " + retrievedAnnot + " of class " + retrievedAnnot.getClass());
				Assert.assertTrue(retrievedAnnot instanceof ProfilingAnnotation);

				if (retrievedAnnot instanceof AnnotationsResourceTestProfilingAnnotation) {
					AnnotationsResourceTestProfilingAnnotation tpa = (AnnotationsResourceTestProfilingAnnotation) retrievedAnnot;
					Assert.assertEquals(dfRef, tpa.getProfiledObject());
					Assert.assertEquals(newAnnotPropValue, tpa.getNewAnnotProp());
				} else {
					// other annotations are "unknown", thus no subclass of ProfilingAnnotation
					Assert.assertTrue(retrievedAnnot.getClass().equals(ProfilingAnnotation.class));
					
					String jsonProps = retrievedAnnot.getJsonProperties();
					Assert.assertNotNull(jsonProps);
					JSONObject jo = (JSONObject) JSON.parse(jsonProps);
					Assert.assertTrue(jo.containsKey(newAnnotPropKey));
					Assert.assertEquals(newAnnotPropValue, jo.getString(newAnnotPropKey));
				}
				Assert.assertTrue(createdAnnotIds.contains(retrievedAnnot.getReference().getId()));
				foundAnnots++;
				
				// check that retrieval by Id works
				logger.info("Retrieving annotation " + retrievedAnnot.getReference().getId() + " again");
				String url = getBaseURI() + "/annotations/objects/" + retrievedAnnot.getReference().getId();
				logger.info("Retriveing annotation with URL: " + url);
				Request req1 = Request.Get(url);
				Response resp1 = exec.execute(req1);

				HttpResponse httpResp1 = resp1.returnResponse();
				checkResult(httpResp1, HttpStatus.SC_OK);
				Annotation newRetrievedAnnot = JSONUtils.fromJSON(httpResp1.getEntity().getContent(), Annotation.class);
				Assert.assertEquals(retrievedAnnot.getReference(), newRetrievedAnnot.getReference());
				Assert.assertEquals(retrievedAnnot.getClass(), newRetrievedAnnot.getClass());
				Assert.assertEquals(retrievedAnnot.getJsonProperties(), newRetrievedAnnot.getJsonProperties());
			}
		}
		Assert.assertEquals(createdAnnotIds.size(), foundAnnots);

	}
}
