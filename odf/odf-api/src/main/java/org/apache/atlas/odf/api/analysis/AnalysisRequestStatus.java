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
package org.apache.atlas.odf.api.analysis;

import java.util.List;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
@ApiModel(description="Status of a specific analysis request.")
public class AnalysisRequestStatus {

	public static enum State {
		ACTIVE, // some discovery service is processing the request 
		QUEUED, // in the queue for some discovery service
		ERROR, // something went wrong 
		FINISHED, // processing finished successfully 
		NOT_FOUND, // request ID was not found
		CANCELLED // request was cancelled by the user
	}

	@ApiModelProperty(value="Analysis request that was submitted", readOnly=true, required=true)
	private AnalysisRequest request;

	@ApiModelProperty(value="Status of the request", readOnly=true, required=true)
	private State state;

	@ApiModelProperty(value="Detailed status description", readOnly=true, required=false)
	private String details;

	@ApiModelProperty(value="Indicates whether an equivalent request was found", readOnly=true, required=true)
	private boolean foundExistingRequest = false;

	@ApiModelProperty(value="List of individual discovery service requests that make up the analysis request", readOnly=true, required=true)
	private List<DiscoveryServiceRequest> serviceRequests;

	@ApiModelProperty(value="Total time the request was queued in milliseconds", readOnly=true, required=true)
	private long totalTimeOnQueues;

	@ApiModelProperty(value="Total time needed for processing the analysis request in milliseconds", readOnly=true, required=true)
	private long totalTimeProcessing;

	@ApiModelProperty(value="Total time needed for storing the annotations in the metadata repository in milliseconds", readOnly=true, required=true)
	private long totalTimeStoringAnnotations;

	public AnalysisRequest getRequest() {
		return request;
	}

	public void setRequest(AnalysisRequest request) {
		this.request = request;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public String getDetails() {
		return details;
	}

	public void setDetails(String details) {
		this.details = details;
	}

	public boolean isFoundExistingRequest() {
		return foundExistingRequest;
	}

	public void setFoundExistingRequest(boolean foundExistingRequest) {
		this.foundExistingRequest = foundExistingRequest;
	}

	public List<DiscoveryServiceRequest> getServiceRequests() {
		return serviceRequests;
	}

	public void setServiceRequests(List<DiscoveryServiceRequest> requests) {
		this.serviceRequests = requests;
	}

	public long getTotalTimeOnQueues() {
		return totalTimeOnQueues;
	}

	public void setTotalTimeOnQueues(long totalTimeOnQueues) {
		this.totalTimeOnQueues = totalTimeOnQueues;
	}

	public long getTotalTimeProcessing() {
		return totalTimeProcessing;
	}

	public void setTotalTimeProcessing(long totalTimeProcessing) {
		this.totalTimeProcessing = totalTimeProcessing;
	}

	public long getTotalTimeStoringAnnotations() {
		return totalTimeStoringAnnotations;
	}

	public void setTotalTimeStoringAnnotations(long totalTimeStoringAnnotations) {
		this.totalTimeStoringAnnotations = totalTimeStoringAnnotations;
	}

}
