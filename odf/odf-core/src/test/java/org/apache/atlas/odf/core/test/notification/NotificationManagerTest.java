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
package org.apache.atlas.odf.core.test.notification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.atlas.odf.api.OpenDiscoveryFramework;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.notification.NotificationListener;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.core.notification.NotificationManager;
import org.apache.atlas.odf.core.test.controlcenter.ODFAPITest;

public class NotificationManagerTest extends ODFTestBase {

	@Test
	public void testNotifications() throws Exception {
		NotificationManager nm = new ODFInternalFactory().create(NotificationManager.class);
		Assert.assertNotNull(nm);
		log.info("Notification manager found " + nm.getClass().getName());
		Assert.assertTrue(nm instanceof TestNotificationManager);
		List<NotificationListener> listeners = nm.getListeners();
		Assert.assertTrue(listeners.size() > 0);

		OpenDiscoveryFramework odf = new ODFFactory().create();
		List<String> dataSetIDs = Collections.singletonList("successID");
		String id = ODFAPITest.runRequest(dataSetIDs, odf.getAnalysisManager());
		ODFAPITest.waitForRequest(id, odf.getAnalysisManager());

		int polls = 20;
		boolean found = false;
		boolean foundFinished = false;
		do {
			// now check that trackers were found through the notification mechanism
			log.info("Checking that trackers were consumed, " + polls + " seconds left");
			List<AnalysisRequestTracker> trackers = new ArrayList<>(TestNotificationManager.receivedTrackers);
			log.info("Received trackers: " + trackers.size());
			for (AnalysisRequestTracker tracker : trackers) {
				String foundId = tracker.getRequest().getId();
				if (foundId.equals(id)) {
					found = true;
					if (tracker.getStatus().equals(STATUS.FINISHED)) {
						foundFinished = true;
					}
				}
			}
			polls--;
			Thread.sleep(1000);
		} while (!found && !foundFinished && polls > 0);
		Assert.assertTrue(found);
		Assert.assertTrue(foundFinished);
	}
}
