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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus.State;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.engine.EngineManager;
import org.apache.atlas.odf.api.engine.SystemHealth;
import org.apache.atlas.odf.api.engine.SystemHealth.HealthStatus;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.core.analysis.AnalysisManagerImpl;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.ODFTestcase;

public class ParallelODFTest extends ODFTestcase {
	Logger log = ODFTestLogger.get();
	
	@Test
	public void runDataSetsInParallelSuccess() throws Exception {
		runDataSetsInParallelAndCheckResult(Arrays.asList(new String[] { "successID1", "successID2" }), State.FINISHED);
	}

	@Test 
	public void runDataSetsInParallelError() throws Exception {
		runDataSetsInParallelAndCheckResult(Arrays.asList(new String[] { "successID1", "errorID2" }), State.ERROR);
	}

	private void runDataSetsInParallelAndCheckResult(List<String> dataSetIDs, State expectedState) throws Exception {
		log.info("Running data sets in parallel: " + dataSetIDs);
		log.info("Expected state: " + expectedState);
		AnalysisRequest req = ODFAPITest.createAnalysisRequest(dataSetIDs);
		// Enable parallel processing because this is a parallel test
		req.setProcessDataSetsSequentially(false);
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();
		EngineManager engineManager = new ODFFactory().create().getEngineManager();

		SystemHealth healthCheckResult = engineManager.checkHealthStatus();
		Assert.assertEquals(HealthStatus.OK, healthCheckResult.getStatus());
		AnalysisResponse resp = analysisManager.runAnalysis(req);
		log.info("Parallel requests started");

		String id = resp.getId();
		List<String> singleIds = Utils.splitString(id, AnalysisManagerImpl.COMPOUND_REQUEST_SEPARATOR);
		List<String> singleDetails = Utils.splitString(resp.getDetails(), AnalysisManagerImpl.COMPOUND_REQUEST_SEPARATOR);
		Assert.assertEquals(dataSetIDs.size(), singleIds.size());
		Assert.assertEquals(dataSetIDs.size(), singleDetails.size());

		AnalysisRequestStatus status = null;

		// check that requests are processed in parallel: 
		//   there must be a point in time where both requests are in status "active"
		log.info("Polling for status of parallel request...");
		boolean foundPointInTimeWhereBothRequestsAreActive = false;
		int maxPolls = ODFAPITest.MAX_NUMBER_OF_POLLS;
		do {
			List<State> allSingleStates = new ArrayList<AnalysisRequestStatus.State>();
			for (String singleId : singleIds) {
				allSingleStates.add(analysisManager.getAnalysisRequestStatus(singleId).getState());
			}
			if (Utils.containsOnly(allSingleStates, new State[] { State.ACTIVE })) {
				foundPointInTimeWhereBothRequestsAreActive = true;
			}

			status = analysisManager.getAnalysisRequestStatus(id);
			log.log(Level.INFO, "Poll request for parallel request ID ''{0}'' (expected state: ''{3}''): state: ''{1}'', details: ''{2}''", new Object[] { id, status.getState(), status.getDetails(),
					expectedState });
			log.info("States of single requests: " + singleIds + ": " + allSingleStates);
			maxPolls--;
			Thread.sleep(ODFAPITest.WAIT_MS_BETWEEN_POLLING);
		} while (maxPolls > 0 && (status.getState() == State.ACTIVE || status.getState() == State.QUEUED));

		Assert.assertTrue(maxPolls > 0);
		Assert.assertEquals(expectedState, status.getState());
		Assert.assertTrue(foundPointInTimeWhereBothRequestsAreActive);
		log.info("Parallel request status details: " + status.getDetails());
	}
}
