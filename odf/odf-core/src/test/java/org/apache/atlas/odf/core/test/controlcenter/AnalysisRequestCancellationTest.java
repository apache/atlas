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

import java.util.Collections;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisCancelResult;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.AnalysisRequestTrackerStore;
import org.apache.atlas.odf.core.test.ODFTestcase;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.core.controlcenter.ControlCenter;
import org.apache.atlas.odf.core.test.ODFTestLogger;

public class AnalysisRequestCancellationTest extends ODFTestcase {

	Logger logger = ODFTestLogger.get();

	AnalysisRequestTracker generateTracker(String id, STATUS status) {
		AnalysisRequestTracker tracker = new AnalysisRequestTracker();
		Utils.setCurrentTimeAsLastModified(tracker);
		tracker.setNextDiscoveryServiceRequest(0);
		AnalysisRequest req = new AnalysisRequest();
		req.setId(id);
		MetaDataObjectReference ref = new MetaDataObjectReference();
		ref.setId("DataSet" + id);
		req.setDataSets(Collections.singletonList(ref));
		tracker.setRequest(req);
		tracker.setStatus(status);
		return tracker;
	}

	@Test
	public void testRequestCancellationNotFoundFailure() {
		ControlCenter cc = new ODFInternalFactory().create(ControlCenter.class);
		AnalysisCancelResult cancelRequest = cc.cancelRequest("dummy_id");
		Assert.assertEquals(cancelRequest.getState(), AnalysisCancelResult.State.NOT_FOUND);
	}

	@Test
	public void testRequestCancellationWrongStateFailure() {
		ControlCenter cc = new ODFInternalFactory().create(ControlCenter.class);
		AnalysisRequestTrackerStore store = (new ODFInternalFactory()).create(AnalysisRequestTrackerStore.class);
		String testId = "test_id1";
		AnalysisRequestTracker tracker = null;
		AnalysisCancelResult cancelRequest = null;
		
		tracker = generateTracker(testId, STATUS.FINISHED);
		store.store(tracker);
		cancelRequest = cc.cancelRequest(testId);
		Assert.assertEquals(cancelRequest.getState(), AnalysisCancelResult.State.INVALID_STATE);

		tracker = generateTracker(testId, STATUS.ERROR);
		store.store(tracker);
		cancelRequest = cc.cancelRequest(testId);
		Assert.assertEquals(cancelRequest.getState(), AnalysisCancelResult.State.INVALID_STATE);

		tracker = generateTracker(testId, STATUS.CANCELLED);
		store.store(tracker);
		cancelRequest = cc.cancelRequest(testId);
		Assert.assertEquals(cancelRequest.getState(), AnalysisCancelResult.State.INVALID_STATE);
	}

	@Test
	public void testRequestCancellationSuccess() {
		ControlCenter cc = new ODFInternalFactory().create(ControlCenter.class);
		AnalysisRequestTrackerStore store = (new ODFInternalFactory()).create(AnalysisRequestTrackerStore.class);
		String testId = "test_id2";

		AnalysisRequestTracker tracker = generateTracker(testId, STATUS.INITIALIZED);
		store.store(tracker);
		AnalysisCancelResult cancelRequest = cc.cancelRequest(testId);
		Assert.assertEquals(AnalysisCancelResult.State.SUCCESS, cancelRequest.getState());

		tracker = generateTracker(testId, STATUS.IN_DISCOVERY_SERVICE_QUEUE);
		store.store(tracker);
		cancelRequest = cc.cancelRequest(testId);
		Assert.assertEquals(AnalysisCancelResult.State.SUCCESS, cancelRequest.getState());

		tracker = generateTracker(testId, STATUS.DISCOVERY_SERVICE_RUNNING);
		store.store(tracker);
		cancelRequest = cc.cancelRequest(testId);
		Assert.assertEquals(AnalysisCancelResult.State.SUCCESS, cancelRequest.getState());
}
}
