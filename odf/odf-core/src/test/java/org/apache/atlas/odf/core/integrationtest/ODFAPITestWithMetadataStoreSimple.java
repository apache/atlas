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
package org.apache.atlas.odf.core.integrationtest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.atlas.odf.core.test.discoveryservice.TestSyncDiscoveryServiceWritingAnnotations1;
import org.apache.wink.json4j.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;

public class ODFAPITestWithMetadataStoreSimple extends ODFAPITestWithMetadataStoreBase {

	public ODFAPITestWithMetadataStoreSimple() {
		ODFTestBase.log.info("Classpath: " + System.getProperty("java.class.path"));
	}

	@Test
	public void testSuccessASync() throws Exception {
		testSuccess("asynctestservice-with-annotations");
	}

	@Test
	public void testSuccessSync() throws Exception {
		testSuccess("synctestservice-with-annotations");
	}

	void testSuccess(String dsId) throws Exception {
		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		AnnotationStore as = new ODFFactory().create().getAnnotationStore();
		List<MetaDataObjectReference> dataSets = getTables(mds);

		String correlationId = UUID.randomUUID().toString();
		
		String requestId = test(dsId, dataSets, AnalysisRequestStatus.State.FINISHED, false, correlationId);
		Thread.sleep(3000); // give time for notifications to arrive

		List<MetaDataObjectReference> annotationsOfThisRun = new ArrayList<>();
		
		ODFTestBase.log.info("Checking if annotations exist");
		for (MetaDataObjectReference dataSet : dataSets) {
			List<Annotation> retrievedAnnotations = as.getAnnotations(dataSet, null);
			Assert.assertTrue(retrievedAnnotations.size() > 0);
			List<Annotation> annotations = new ArrayList<>();
			for (Annotation annot : retrievedAnnotations) {
				Assert.assertNotNull(annot);
				Assert.assertNotNull(annot.getAnalysisRun());
				if (annot.getAnalysisRun().equals(requestId)) {
					annotationsOfThisRun.add(annot.getReference());
					Assert.assertNotNull(annot.getJsonProperties());
					JSONObject props = new JSONObject(annot.getJsonProperties());
					if (props != null) {
						String annotCorrId = (String) props.get(TestSyncDiscoveryServiceWritingAnnotations1.REQUEST_PROPERTY_CORRELATION_ID);
						if (annotCorrId != null) {
							Assert.assertNotNull(annot.getAnnotationType());
						}
					}
					annotations.add(annot);
				}
			}
			ODFTestBase.log.info("Checking that annotation notifications were received");
			// check that we got notified of all annotations
			
			// assume at least that those new annotations were created
			Assert.assertTrue(TestSyncDiscoveryServiceWritingAnnotations1.getNumberOfAnnotations() <= annotations.size());
			int found = 0;
			for (int i = 0; i < TestSyncDiscoveryServiceWritingAnnotations1.getNumberOfAnnotations(); i++) {
				String[] annotValues = TestSyncDiscoveryServiceWritingAnnotations1.getPropsOfNthAnnotation(i);
				for (Annotation annotation : annotations) {
					if (annotation.getAnnotationType() != null) {
						if (annotation.getAnnotationType().equals(annotValues[0])) {
							JSONObject jo = new JSONObject(annotation.getJsonProperties());
							String foundCorrelationId = (String) jo.get(TestSyncDiscoveryServiceWritingAnnotations1.REQUEST_PROPERTY_CORRELATION_ID);
							// only look at those where the correlation ID property is set
							if (correlationId.equals(foundCorrelationId)) {
								String val = (String) jo.get(annotValues[1]);
								Assert.assertEquals(annotValues[2], val);
								Assert.assertEquals(requestId, annotation.getAnalysisRun());
								// annotation types and the JSON properties match
								found++;
							}
						}
					}
				}
			}
			// assert that we have found all and not more
			Assert.assertEquals(TestSyncDiscoveryServiceWritingAnnotations1.getNumberOfAnnotations(), found);

			checkMostRecentAnnotations(mds, new ODFFactory().create().getAnnotationStore(), dataSet);
		}
	}

	
	
	@Test
	public void testFailureASync() throws Exception {
		testFailure("asynctestservice-with-annotations");
	}

	@Test
	public void testFailureSync() throws Exception {
		testFailure("synctestservice-with-annotations");
	}

	void testFailure(String dsId) throws Exception {
		MetaDataObjectReference invalidRef = new MetaDataObjectReference();
		invalidRef.setId("error-this-is-hopefully-an-invalid-id");
		List<MetaDataObjectReference> dataSets = Collections.singletonList(invalidRef);
		test(dsId, dataSets, AnalysisRequestStatus.State.ERROR, true, UUID.randomUUID().toString());
	}

}
