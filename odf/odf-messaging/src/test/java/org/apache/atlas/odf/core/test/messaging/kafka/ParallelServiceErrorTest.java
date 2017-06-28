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
package org.apache.atlas.odf.core.test.messaging.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.ODFTestcase;
import org.apache.atlas.odf.core.test.controlcenter.ODFAPITest;

public class ParallelServiceErrorTest extends ODFTestcase {
	private static final int NUMBER_OF_QUEUED_REQUESTS = 1;
	Logger log = ODFTestLogger.get();

	@Test
	public void runDataSetsInParallelError() throws Exception {
		runDataSetsInParallelAndCheckResult(Arrays.asList(new String[] { "successID1", "errorID2" }), AnalysisRequestStatus.State.FINISHED, AnalysisRequestStatus.State.ERROR);
	}

	private void runDataSetsInParallelAndCheckResult(List<String> dataSetIDs, AnalysisRequestStatus.State... expectedState) throws Exception {
		log.info("Running data sets in parallel: " + dataSetIDs);
		log.info("Expected state: " + expectedState);
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();

		List<AnalysisRequest> requests = new ArrayList<AnalysisRequest>();
		List<AnalysisResponse> responses = new ArrayList<AnalysisResponse>();
		List<String> idList = new ArrayList<String>();

		for (int no = 0; no < NUMBER_OF_QUEUED_REQUESTS; no++) {
			for (String dataSet : dataSetIDs) {
				final AnalysisRequest req = ODFAPITest.createAnalysisRequest(Arrays.asList(dataSet + UUID.randomUUID().toString()));
				AnalysisResponse resp = analysisManager.runAnalysis(req);
				req.setId(resp.getId());
				requests.add(req);
				idList.add(resp.getId());
				responses.add(resp);
			}
		}
		log.info("Parallel requests started: " + idList.toString());

		Assert.assertEquals(NUMBER_OF_QUEUED_REQUESTS * dataSetIDs.size(), requests.size());
		Assert.assertEquals(NUMBER_OF_QUEUED_REQUESTS * dataSetIDs.size(), responses.size());

		// check that requests are processed in parallel: 
		//   there must be a point in time where both requests are in status "active"
		log.info("Polling for status of parallel request...");
		boolean foundPointInTimeWhereBothRequestsAreActive = false;
		int maxPolls = ODFAPITest.MAX_NUMBER_OF_POLLS;
		List<AnalysisRequestStatus.State> allSingleStates = new ArrayList<AnalysisRequestStatus.State>();
		do {
			int foundActive = 0;
			allSingleStates.clear();
			for (AnalysisRequest request : requests) {
				final AnalysisRequestStatus.State state = analysisManager.getAnalysisRequestStatus(request.getId()).getState();
				if (state == AnalysisRequestStatus.State.ACTIVE) {
					log.info("ACTIVE: " + request.getId() + " foundactive: " + foundActive);
					foundActive++;
				} else {
					log.info("NOT ACTIVE " + request.getId() + " _ " + state);
				}
				allSingleStates.add(state);
			}
			if (foundActive > 1) {
				foundPointInTimeWhereBothRequestsAreActive = true;
			}

			maxPolls--;
			Thread.sleep(ODFAPITest.WAIT_MS_BETWEEN_POLLING);
		} while (maxPolls > 0 && Utils.containsNone(allSingleStates, new AnalysisRequestStatus.State[] { AnalysisRequestStatus.State.ACTIVE, AnalysisRequestStatus.State.QUEUED }));

		Assert.assertTrue(maxPolls > 0);
		Assert.assertTrue(foundPointInTimeWhereBothRequestsAreActive);
		Assert.assertTrue(allSingleStates.containsAll(Arrays.asList(expectedState)));
	}
}
