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
package org.apache.atlas.odf.core.test.spark;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.core.metadata.DefaultMetadataStore;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.core.test.ODFTestBase;

public class SimpleSparkDiscoveryServiceTest extends ODFTestBase {

	public static int WAIT_MS_BETWEEN_POLLING = 500;
	public static int MAX_NUMBER_OF_POLLS = 500;
	
	@Test
	public void testSparkService() throws Exception{
		log.info("Running request ");
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();
		AnalysisRequest request = new AnalysisRequest();
		List<MetaDataObjectReference> dataSetRefs = new ArrayList<>();
		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		if (!(mds instanceof DefaultMetadataStore)) {
			throw new RuntimeException(MessageFormat.format("This tests does not work with metadata store implementation \"{0}\" but only with the DefaultMetadataStore.", mds.getClass().getName()));
		}
		DefaultMetadataStore defaultMds = (DefaultMetadataStore) mds;
		defaultMds.resetAllData();
		RelationalDataSet dataSet = new DataFile();
		MetaDataObjectReference ref = new MetaDataObjectReference();
		ref.setId("datafile-mock");
		dataSet.setReference(ref);
		defaultMds.createObject(dataSet);
		defaultMds.commit();
		dataSetRefs.add(dataSet.getReference());
		request.setDataSets(dataSetRefs);
		List<String> serviceIds = Arrays.asList(new String[]{"spark-service-test"});
		request.setDiscoveryServiceSequence(serviceIds);

		log.info("Starting analyis");
		AnalysisResponse response = analysisManager.runAnalysis(request);
		Assert.assertNotNull(response);
		String requestId = response.getId();
		Assert.assertNotNull(requestId);
		log.info("Request id is " + requestId + ".");

		log.info("Waiting for request to finish");
		AnalysisRequestStatus status = null;
		int maxPolls = MAX_NUMBER_OF_POLLS;
		do {
			status = analysisManager.getAnalysisRequestStatus(requestId);
			log.log(Level.INFO, "Poll request for request ID ''{0}'', state: ''{1}'', details: ''{2}''", new Object[] { requestId, status.getState(), status.getDetails() });
			maxPolls--;
			try {
				Thread.sleep(WAIT_MS_BETWEEN_POLLING);
			} catch (InterruptedException e) {
				log.log(Level.INFO, "Exception thrown: ", e);
			}
		} while (maxPolls > 0 && (status.getState() == AnalysisRequestStatus.State.ACTIVE || status.getState() == AnalysisRequestStatus.State.QUEUED || status.getState() == AnalysisRequestStatus.State.NOT_FOUND));
		if (maxPolls == 0) {
			log.log(Level.INFO, "Request ''{0}'' is not finished yet, don't wait for it", requestId);
		}
		Assert.assertEquals(AnalysisRequestStatus.State.FINISHED, status.getState());
		log.log(Level.INFO, "Request ''{0}'' is finished.", requestId);
	}
}
