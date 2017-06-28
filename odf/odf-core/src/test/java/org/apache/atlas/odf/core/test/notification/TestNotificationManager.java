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
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.core.controlcenter.StatusQueueEntry;
import org.apache.atlas.odf.core.notification.NotificationListener;
import org.apache.atlas.odf.core.notification.NotificationManager;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;

public class TestNotificationManager implements NotificationManager {

	public static class TestListener1 implements NotificationListener {

		@Override
		public String getTopicName() {
			return "odf-status-topic";
		}

		@Override
		public void onEvent(String event, OpenDiscoveryFramework odf) {
			try {
				StatusQueueEntry sqe = JSONUtils.fromJSON(event, StatusQueueEntry.class);
				AnalysisRequestTracker tracker = sqe.getAnalysisRequestTracker();
				if (tracker != null) {
					receivedTrackers.add(tracker);					
				}
			} catch (JSONException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String getName() {
			return this.getClass().getName();
		}

	}

	public static List<AnalysisRequestTracker> receivedTrackers = Collections.synchronizedList(new ArrayList<AnalysisRequestTracker>());

	@Override
	public List<NotificationListener> getListeners() {
		List<NotificationListener> result = new ArrayList<>();
		result.add(new TestListener1());
		return result;
	}

}
