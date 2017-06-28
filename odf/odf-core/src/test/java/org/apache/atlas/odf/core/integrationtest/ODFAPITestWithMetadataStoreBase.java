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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.configuration.ConfigContainer;
import org.apache.atlas.odf.json.JSONUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.annotation.AnnotationStoreUtils;
import org.apache.atlas.odf.core.configuration.ConfigManager;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.atlas.odf.core.test.discoveryservice.TestSyncDiscoveryServiceWritingAnnotations1;

public class ODFAPITestWithMetadataStoreBase extends ODFTestBase {

	@Before
	public void createSampleData() throws Exception {
		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		mds.resetAllData();
		mds.createSampleData();
	}

	@BeforeClass
	public static void registerServices() throws Exception {
		ConfigContainer config = JSONUtils.readJSONObjectFromFileInClasspath(ConfigContainer.class, "org/apache/atlas/odf/core/test/internal/odf-initial-configuration.json",
				ODFAPITestWithMetadataStoreBase.class.getClassLoader());
		ConfigManager configManager = new ODFInternalFactory().create(ConfigManager.class);
		configManager.updateConfigContainer(config);
	}

	protected List<MetaDataObjectReference> getTables(MetadataStore mds) {
		List<MetaDataObjectReference> dataSets = mds.search(mds.newQueryBuilder().objectType("DataFile").build());
		Assert.assertTrue(dataSets.size() > 0);
		// take only maximal 5 data sets
		int MAX_DATASETS = 5;
		if (dataSets.size() > MAX_DATASETS) {
			dataSets = dataSets.subList(0, MAX_DATASETS);
		}
		return dataSets;
	}

	public String test(String dsId, List<MetaDataObjectReference> dataSets, AnalysisRequestStatus.State expectedFinalState, boolean requestIsInvalid, String correlationId) throws Exception {
		log.log(Level.INFO, "Testing ODF with metadata store. Discovery service Id: {0}, dataSets: {1}, expected state: {2}, correlationId: {3}, should request be invalid: {4}", new Object[] { dsId,
				dataSets, expectedFinalState, correlationId, requestIsInvalid });
		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		Assert.assertTrue(dataSets.size() > 0);

		Assert.assertNotNull(mds);
		AnalysisRequest request = new AnalysisRequest();
		request.setDiscoveryServiceSequence(Collections.singletonList(dsId));
		request.setDataSets(dataSets);
		Map<String, Object> additionalProps = new HashMap<String, Object>();
		additionalProps.put(TestSyncDiscoveryServiceWritingAnnotations1.REQUEST_PROPERTY_CORRELATION_ID, correlationId);
		request.setAdditionalProperties(additionalProps);
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();
		AnalysisResponse resp = analysisManager.runAnalysis(request);

		log.info("Analysis started on data sets: " + dataSets + ", response: " + JSONUtils.toJSON(resp));
		log.info("Response message: " + resp.getDetails());
		if (requestIsInvalid) {
			Assert.assertTrue(resp.isInvalidRequest());
			return null;
		}

		Assert.assertFalse(resp.isInvalidRequest());
		String id = resp.getId();
		AnalysisRequestStatus status = null;
		int maxPolls = 100;
		do {
			status = analysisManager.getAnalysisRequestStatus(id);
			log.log(Level.INFO, "Poll request for request ID ''{0}'' (expected state: ''{3}''): state: ''{1}'', details: ''{2}''", new Object[] { id, status.getState(), status.getDetails(),
					expectedFinalState });
			maxPolls--;
			Thread.sleep(1000);
		} while (maxPolls > 0 && (status.getState() == AnalysisRequestStatus.State.ACTIVE || status.getState() == AnalysisRequestStatus.State.QUEUED));
		log.log(Level.INFO, "Expected state: {0}, actual state: {1}", new Object[] { expectedFinalState, status.getState() });
		Assert.assertEquals(expectedFinalState, status.getState());
		return resp.getId();
	}

	public void checkMostRecentAnnotations(MetadataStore mds, AnnotationStore as, MetaDataObjectReference ref) {
		Map<MetaDataObjectReference, MetaDataObject> ref2Retrieved = new HashMap<>();
		for (Annotation annot : as.getAnnotations(ref, null)) {
			ref2Retrieved.put(annot.getReference(), annot);
		}

		List<Annotation> mostRecentAnnotations = AnnotationStoreUtils.getMostRecentAnnotationsByType(as, ref);
		Assert.assertNotNull(mostRecentAnnotations);
		Assert.assertTrue(mostRecentAnnotations.size() <= ref2Retrieved.size());
		Set<MetaDataObjectReference> mostRecentAnnoationRefs = new HashSet<>();
		Set<String> annotationTypes = new HashSet<>();
		for (Annotation annot : mostRecentAnnotations) {
			// every annotation type occurs at most once
			Assert.assertFalse( annotationTypes.contains(annot.getAnnotationType()));
			mostRecentAnnoationRefs.add(annot.getReference());
			annotationTypes.add(annot.getAnnotationType());
		}

		// all most recent annotations are a subset of all annotations
		Assert.assertTrue(ref2Retrieved.keySet().containsAll(mostRecentAnnoationRefs));

	}

}
