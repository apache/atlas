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
package org.apache.atlas.odf.core.discoveryservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.ServiceStatusCount;

public class DiscoveryServiceStatistics {

	private List<AnalysisRequestTracker> requests = new ArrayList<AnalysisRequestTracker>();

	public DiscoveryServiceStatistics(List<AnalysisRequestTracker> requests) {
		this.requests = requests;
	}

	public List<ServiceStatusCount> getStatusCountPerService() {
		List<ServiceStatusCount> result = new ArrayList<ServiceStatusCount>();

		Map<String, LinkedHashMap<STATUS, Integer>> statusMap = new HashMap<String, LinkedHashMap<STATUS, Integer>>();

		for (AnalysisRequestTracker tracker : requests) {
			int maxDiscoveryServiceRequest = (tracker.getNextDiscoveryServiceRequest() == 0 ? 1 : tracker.getNextDiscoveryServiceRequest());
			for (int no = 0; no < maxDiscoveryServiceRequest; no++) {
				STATUS cntStatus = tracker.getStatus();

				//No parallel requests are possible atm -> all requests leading to current one must be finished
				if (no < maxDiscoveryServiceRequest - 1) {
					cntStatus = STATUS.FINISHED;
				}

				DiscoveryServiceRequest req = tracker.getDiscoveryServiceRequests().get(no);
				LinkedHashMap<STATUS, Integer> cntMap = statusMap.get(req.getDiscoveryServiceId());
				if (cntMap == null) {
					cntMap = new LinkedHashMap<STATUS, Integer>();
					//add 0 default values
					for (STATUS status : STATUS.values()) {
						cntMap.put(status, 0);
					}
				}
				Integer val = cntMap.get(cntStatus);
				val++;
				cntMap.put(cntStatus, val);
				statusMap.put(req.getDiscoveryServiceId(), cntMap);
			}
		}

		for (String key : statusMap.keySet()) {
			ServiceStatusCount cnt = new ServiceStatusCount();
			cnt.setId(key);
			for (DiscoveryServiceProperties info : new ODFFactory().create().getDiscoveryServiceManager().getDiscoveryServicesProperties()) {
				if (info.getId().equals(key)) {
					cnt.setName(info.getName());
					break;
				}
			}
			cnt.setStatusCountMap(statusMap.get(key));
			result.add(cnt);
		}

		return result;
	}
}
