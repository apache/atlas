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

import java.util.logging.Level;

import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.core.controlcenter.AnalysisRequestTrackerStore;
import org.apache.atlas.odf.core.test.ODFTestBase;

public class SetTrackerStatusTest extends ODFTestBase {

	@Test
	public void testSetTrackerStatus() throws Exception {
		AnalysisManager am = new ODFFactory().create().getAnalysisManager();
		AnalysisRequestTrackerStore arts = new ODFInternalFactory().create(AnalysisRequestTrackerStore.class);
		String requestId = ODFAPITest.runRequest("successId", am);
		Thread.sleep(1000);
		long cutOffTimestamp = System.currentTimeMillis();		
		String testMessage = "Message was set to error at " + cutOffTimestamp;
		arts.setStatusOfOldRequest(cutOffTimestamp, STATUS.ERROR, testMessage);
		AnalysisRequestTracker tracker = arts.query(requestId);
		Assert.assertEquals(STATUS.ERROR, tracker.getStatus());
		Assert.assertEquals(testMessage, tracker.getStatusDetails());
		
		// wait until request is finished and state is set back to finished
		log.log(Level.INFO, "Waiting for request ''{0}'' to finish", requestId);
		int maxPolls = ODFAPITest.MAX_NUMBER_OF_POLLS;
		AnalysisRequestStatus status = null;
		do {
			status = am.getAnalysisRequestStatus(requestId);
			log.log(Level.INFO, "Poll request for request ID ''{0}'', state: ''{1}'', details: ''{2}''", new Object[] { requestId, status.getState(), status.getDetails() });
			maxPolls--;
			try {
				Thread.sleep(ODFAPITest.WAIT_MS_BETWEEN_POLLING);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} while (maxPolls > 0 && (status.getState() != AnalysisRequestStatus.State.FINISHED) );
		
		Assert.assertEquals(AnalysisRequestStatus.State.FINISHED, am.getAnalysisRequestStatus(requestId).getState());
		tracker = arts.query(requestId);
		Assert.assertEquals(STATUS.FINISHED, tracker.getStatus());
	}
	
}
