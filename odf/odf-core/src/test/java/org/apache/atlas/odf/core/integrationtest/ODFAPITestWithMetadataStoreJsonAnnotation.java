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

import java.util.List;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus.State;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.json.JSONUtils;

public class ODFAPITestWithMetadataStoreJsonAnnotation extends ODFAPITestWithMetadataStoreBase {

	Logger logger = ODFTestLogger.get();

	String expectedJson = Utils.getInputStreamAsString(this.getClass().getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/integrationtest/metadata/internal/atlas/nested_annotation_example.json"), "UTF-8");

	@Test
	public void testSuccessSyncJsonAnnotations() throws Exception {

		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		AnnotationStore as = new ODFFactory().create().getAnnotationStore();
		List<MetaDataObjectReference> dataSets = getTables(mds);
		String dsID = "synctestservice-with-json-annotations";

		String requestId = test(dsID, dataSets, State.FINISHED, false, null);

		log.info("Checking if annotations exist for request ID: " + requestId);
		int numMatchingAnnotations = 0;
		for (MetaDataObjectReference dataSet : dataSets) {
			List<Annotation> annotationRefs = as.getAnnotations(dataSet, null);
			Assert.assertTrue(annotationRefs.size() >= 1);
			for (Annotation annot : annotationRefs) {
				Assert.assertNotNull(annot);
				if (annot.getAnalysisRun().equals(requestId)) {
					log.info("Found annotation: " + annot + ", json: " + JSONUtils.toJSON(annot));
					Assert.assertNotNull(annot);
					String jsonProperties = annot.getJsonProperties();
					Assert.assertNotNull(jsonProperties);
					logger.info("Actual annotation string: " + jsonProperties + ". Expected json: " + expectedJson);
					Assert.assertEquals(expectedJson, jsonProperties);
					numMatchingAnnotations++;
				}
			}
//			Assert.assertEquals(1, numMatchingAnnotations);
		}
	}

}
