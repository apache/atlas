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
package org.apache.atlas.odf.core.controlcenter;

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;

public class TrackerUtil {
	
	/**
	 * @param tracker
	 * @return true if the first analysis of the tracker has not yet been started
	 */
	public static boolean isAnalysisWaiting(AnalysisRequestTracker tracker) {
		return tracker.getNextDiscoveryServiceRequest() == 0 && (tracker.getStatus() == STATUS.IN_DISCOVERY_SERVICE_QUEUE || tracker.getStatus() == STATUS.INITIALIZED); // || tracker.getStatus() == STATUS.DISCOVERY_SERVICE_RUNNING);
	}
	
	public static boolean isCancellable(AnalysisRequestTracker tracker)  {
		return (tracker.getStatus() == STATUS.IN_DISCOVERY_SERVICE_QUEUE || tracker.getStatus() == STATUS.INITIALIZED || tracker.getStatus() == STATUS.DISCOVERY_SERVICE_RUNNING);
	}

	public static DiscoveryServiceRequest getCurrentDiscoveryServiceStartRequest(AnalysisRequestTracker tracker) {
		int i = tracker.getNextDiscoveryServiceRequest();
		List<DiscoveryServiceRequest> requests = tracker.getDiscoveryServiceRequests();
		if (i >= 0 && i < requests.size()) {
			return requests.get(i);
		}
		return null;
	}

	public static DiscoveryServiceResponse getCurrentDiscoveryServiceStartResponse(AnalysisRequestTracker tracker) {
		int i = tracker.getNextDiscoveryServiceRequest();
		List<DiscoveryServiceResponse> responses = tracker.getDiscoveryServiceResponses();
		if (responses == null || responses.isEmpty()) {
			return null;
		}
		if (i >= 0 && i < responses.size()) {
			return responses.get(i);
		}
		return null;
	}

	public static void moveToNextDiscoveryService(AnalysisRequestTracker tracker) {
		int i = tracker.getNextDiscoveryServiceRequest();
		List<DiscoveryServiceRequest> requests = tracker.getDiscoveryServiceRequests();
		if (i >= 0 && i < requests.size()) {
			tracker.setNextDiscoveryServiceRequest(i+1);
		}
	}

	public static void addDiscoveryServiceStartResponse(AnalysisRequestTracker tracker, DiscoveryServiceResponse response) {
		List<DiscoveryServiceResponse> l = tracker.getDiscoveryServiceResponses();
		if (l == null) {
			l = new ArrayList<DiscoveryServiceResponse>();
			tracker.setDiscoveryServiceResponses(l);
		}
		l.add(response);
	}

}
