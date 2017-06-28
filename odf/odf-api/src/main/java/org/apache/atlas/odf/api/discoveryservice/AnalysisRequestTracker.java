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
package org.apache.atlas.odf.api.discoveryservice;

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus;

// JSON
@ApiModel(description="Container for tracking the status of an analysis request.")
public class AnalysisRequestTracker {

	@ApiModelProperty(value="Analysis request", required=true)
	private AnalysisRequest request;

	@ApiModelProperty(value="List of discovery service requests that make up the analysis request", required=true)
	private List<DiscoveryServiceRequest> discoveryServiceRequests = new ArrayList<DiscoveryServiceRequest>();

	@ApiModelProperty(value="List of responses, one for each discovery service request", required=true)
	private List<DiscoveryServiceResponse> discoveryServiceResponses = new ArrayList<DiscoveryServiceResponse>();

	@ApiModelProperty(value="Status of the analysis request", required=true)
	private AnalysisRequestTrackerStatus.STATUS status = AnalysisRequestTrackerStatus.STATUS.INITIALIZED;

	@ApiModelProperty(value="Detailed status of the analysis request", required=false)
	private String statusDetails;

	@ApiModelProperty(value="Timestamp of last status update", required=true)
	private long lastModified;

	@ApiModelProperty(value="User who has submitted the analysis request", required=true)
	private String user;

	// A tracker object is used to publish changes across all ODF nodes. When writing a tracker on the queue,
	// a revision is added so that we know when a tracker has successfully been stored in the ODF that wrote it.
	// This is necessary to make storing of these trackers a synchronous method.
	@ApiModelProperty(value="Internal revision id of the analysis request", required=true)
	private String revisionId;

	@ApiModelProperty(value="Next discovery service request to be issued")
	private int nextDiscoveryServiceRequest = 0;

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public long getLastModified() {
		return lastModified;
	}

	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
	}

	public List<DiscoveryServiceRequest> getDiscoveryServiceRequests() {
		return discoveryServiceRequests;
	}

	public void setDiscoveryServiceRequests(List<DiscoveryServiceRequest> discoveryServiceRequests) {
		this.discoveryServiceRequests = discoveryServiceRequests;
	}

	public int getNextDiscoveryServiceRequest() {
		return nextDiscoveryServiceRequest;
	}

	public void setNextDiscoveryServiceRequest(int nextDiscoveryServiceRequest) {
		this.nextDiscoveryServiceRequest = nextDiscoveryServiceRequest;
	}

	public AnalysisRequest getRequest() {
		return request;
	}

	public void setRequest(AnalysisRequest request) {
		this.request = request;
	}

	public AnalysisRequestTrackerStatus.STATUS getStatus() {
		return status;
	}

	public void setStatus(AnalysisRequestTrackerStatus.STATUS status) {
		this.status = status;
	}

	public String getStatusDetails() {
		return statusDetails;
	}

	public void setStatusDetails(String statusDetails) {
		this.statusDetails = statusDetails;
	}

	public List<DiscoveryServiceResponse> getDiscoveryServiceResponses() {
		return discoveryServiceResponses;
	}

	public void setDiscoveryServiceResponses(List<DiscoveryServiceResponse> discoveryServiceResponses) {
		this.discoveryServiceResponses = discoveryServiceResponses;
	}

	public String getRevisionId() {
		return revisionId;
	}

	public void setRevisionId(String revisionId) {
		this.revisionId = revisionId;
	}
}
