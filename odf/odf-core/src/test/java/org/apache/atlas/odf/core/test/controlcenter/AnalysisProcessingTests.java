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
package org.apache.atlas.odf.core.test.controlcenter;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisCancelResult;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.settings.MessagingConfiguration;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.core.controlcenter.ControlCenter;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.ODFTestcase;
import org.apache.atlas.odf.json.JSONUtils;

public class AnalysisProcessingTests extends ODFTestcase {
	Logger logger = ODFTestLogger.get();

	@Test
	public void testAnalysisProcessingAfterShutdown() throws Exception {
		final SettingsManager config = new ODFFactory().create().getSettingsManager();
		final ODFSettings odfSettings = config.getODFSettings();
		final MessagingConfiguration messagingConfiguration = odfSettings.getMessagingConfiguration();
		final Long origRequestRetentionMs = messagingConfiguration.getAnalysisRequestRetentionMs();
		messagingConfiguration.setAnalysisRequestRetentionMs(300000l);
		config.updateODFSettings(odfSettings);

		ControlCenter cc = new ODFInternalFactory().create(ControlCenter.class);
		AnalysisRequestTracker tracker = JSONUtils.readJSONObjectFromFileInClasspath(AnalysisRequestTracker.class, "org/apache/atlas/odf/core/test/messaging/kafka/tracker1.json", null);
		AnalysisRequest req = tracker.getRequest();
		req.setDiscoveryServiceSequence(Arrays.asList("asynctestservice"));
		req.getDataSets().get(0).setId(ODFAPITest.DUMMY_SUCCESS_ID + "_dataset");
		final AnalysisResponse startRequest = cc.startRequest(req);
		logger.info("Analysis :" + startRequest.getId());

		Assert.assertNull(startRequest.getOriginalRequest());
		Assert.assertFalse(startRequest.isInvalidRequest());
		final AnalysisResponse duplicate = cc.startRequest(req);
		Assert.assertNotNull(duplicate.getOriginalRequest());
		Assert.assertEquals(startRequest.getId(), duplicate.getId());
		logger.info("Analysis1 duplciate :" + duplicate.getId());

		final AnalysisCancelResult cancelRequest = cc.cancelRequest(startRequest.getId());
		Assert.assertEquals(AnalysisCancelResult.State.SUCCESS, cancelRequest.getState());

		cc.getQueueManager().stop();

		AnalysisResponse response2 = cc.startRequest(req);
		logger.info("Analysis2:" + response2.getId());
		AnalysisRequestStatus requestStatus = cc.getRequestStatus(response2.getId());
		int maxWait = 20;

		int currentWait = 0;
		while (currentWait < maxWait && requestStatus.getState() != AnalysisRequestStatus.State.ACTIVE) {
			Thread.sleep(100);
			currentWait++;
			requestStatus = cc.getRequestStatus(response2.getId());
		}
		logger.info("THREAD ACTIVE, KILL IT!");

		cc.getQueueManager().start();
		logger.info("restarted");
		Assert.assertNull(response2.getOriginalRequest());
		Assert.assertFalse(response2.isInvalidRequest());

		messagingConfiguration.setAnalysisRequestRetentionMs(origRequestRetentionMs);
		config.updateODFSettings(odfSettings);

		currentWait = 0;
		while (currentWait < maxWait && requestStatus.getState() != AnalysisRequestStatus.State.FINISHED) {
			Thread.sleep(100);
			requestStatus = cc.getRequestStatus(response2.getId());
		}
		Assert.assertEquals(AnalysisRequestStatus.State.FINISHED, requestStatus.getState());
	}

	@Test
	public void testRequestWithAnnotationTypes() throws Exception {
		ControlCenter cc = new ODFInternalFactory().create(ControlCenter.class);
		AnalysisRequestTracker tracker = JSONUtils.readJSONObjectFromFileInClasspath(AnalysisRequestTracker.class, "org/apache/atlas/odf/core/test/messaging/kafka/tracker1.json", null);
		AnalysisRequest req = tracker.getRequest();
		req.getDataSets().get(0).setId(ODFAPITest.DUMMY_SUCCESS_ID + "_dataset");
		List<String> annotationTypes = Arrays.asList(new String[] { "AsyncTestDummyAnnotation" });
		req.setAnnotationTypes(annotationTypes);
		logger.info(MessageFormat.format("Running discovery request for annotation type {0}.", annotationTypes));
		AnalysisResponse resp = cc.startRequest(req);
		logger.info(MessageFormat.format("Started request id {0}.", resp.getId()));
		Assert.assertNotNull(resp.getId());
		Assert.assertFalse(resp.isInvalidRequest());

		int currentWait = 0;
		int maxWait = 20;
		AnalysisRequestStatus requestStatus = cc.getRequestStatus(resp.getId());
		while (currentWait < maxWait && requestStatus.getState() != AnalysisRequestStatus.State.FINISHED) {
			Thread.sleep(100);
			requestStatus = cc.getRequestStatus(resp.getId());
		}
		Assert.assertEquals(AnalysisRequestStatus.State.FINISHED, requestStatus.getState());
		Assert.assertEquals("Generated service has incorrect number of elements.", 1, requestStatus.getRequest().getDiscoveryServiceSequence().size());
		Assert.assertEquals("Generated service sequence differs from expected value.", "asynctestservice", requestStatus.getRequest().getDiscoveryServiceSequence().get(0));
	}

	@Test
	public void testRequestWithMissingAnnotationTypes() throws Exception {
		ControlCenter cc = new ODFInternalFactory().create(ControlCenter.class);
		AnalysisRequestTracker tracker = JSONUtils.readJSONObjectFromFileInClasspath(AnalysisRequestTracker.class, "org/apache/atlas/odf/core/test/messaging/kafka/tracker1.json", null);
		AnalysisRequest req = tracker.getRequest();
		req.getDataSets().get(0).setId(ODFAPITest.DUMMY_SUCCESS_ID + "_dataset");
		List<String> annotationTypes = Arrays.asList(new String[] { "noServiceExistsForThisAnnotationType" });
		req.setAnnotationTypes(annotationTypes);
		logger.info(MessageFormat.format("Running discovery request for non-existing annotation type {0}.", annotationTypes));
		AnalysisResponse resp = cc.startRequest(req);
		Assert.assertTrue(resp.isInvalidRequest());
		Assert.assertEquals("Unexpected error message.", "No suitable discovery services found to create the requested annotation types.", resp.getDetails());
	}
}
