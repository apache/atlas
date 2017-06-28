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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.core.controlcenter.AnalysisRequestTrackerStore;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.ODFTestcase;

public class AnalysisRequestTrackerStoreTest extends ODFTestcase {

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
	public void testStore() throws Exception {
		AnalysisRequestTrackerStore store = (new ODFInternalFactory()).create(AnalysisRequestTrackerStore.class);
		assertNotNull(store);
		int MAX_TRACKERS = 50;
		List<AnalysisRequestTracker> trackers1 = new ArrayList<AnalysisRequestTracker>();
		STATUS lastStatus = STATUS.IN_DISCOVERY_SERVICE_QUEUE;
		for (int i = 0; i < MAX_TRACKERS; i++) {
			trackers1.add(generateTracker("STORETEST_ID" + i, lastStatus));
		}

		logger.info("Storing " + MAX_TRACKERS + " Trackers");
		long pass1Start = System.currentTimeMillis();
		for (AnalysisRequestTracker tracker : trackers1) {
			store.store(tracker);
		}
		long pass1End = System.currentTimeMillis();

		logger.info("Storing " + MAX_TRACKERS + " Trackers again with new status");

		lastStatus = STATUS.FINISHED;
		List<AnalysisRequestTracker> trackers2 = new ArrayList<AnalysisRequestTracker>();
		for (int i = 0; i < MAX_TRACKERS; i++) {
			trackers2.add(generateTracker("STORETEST_ID" + i, lastStatus));
		}
		long pass2Start = System.currentTimeMillis();
		for (AnalysisRequestTracker tracker : trackers2) {
			store.store(tracker);
		}
		long pass2End = System.currentTimeMillis();

		Thread.sleep(2000);
		logger.info("Querying and checking " + MAX_TRACKERS + " Trackers");

		long queryStart = System.currentTimeMillis();

		for (int i = 0; i < MAX_TRACKERS; i++) {
			final String analysisRequestId = "STORETEST_ID" + i;
			AnalysisRequestTracker tracker = store.query(analysisRequestId);
			assertNotNull(tracker);
			assertEquals(1, tracker.getRequest().getDataSets().size());
			MetaDataObjectReference ref = new MetaDataObjectReference();
			ref.setId("DataSet" + analysisRequestId);
			assertEquals(tracker.getRequest().getDataSets().get(0), ref);
			assertEquals(lastStatus, tracker.getStatus());
		}
		long queryEnd = System.currentTimeMillis();

		System.out.println("First pass: " + (pass1End - pass1Start) + "ms, second pass: " + (pass2End - pass2Start) + "ms, query: " + (queryEnd - queryStart) + "ms");

	}
}
