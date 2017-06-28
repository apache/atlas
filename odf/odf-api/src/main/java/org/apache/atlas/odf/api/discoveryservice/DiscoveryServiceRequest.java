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

import java.util.Map;

import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
/**
 * 
 * This class represents an analysis request that is passed from ODF to the service.
 *
 */
@ApiModel(description="Request for running a single discovery service.")
public class DiscoveryServiceRequest {
	/**
	 * The discoveryService identifier
	 */
	@ApiModelProperty(value="Id string of the discovery service to be issued", required=true)
	private String discoveryServiceId;
	/**
	 * This property can be used by a user to pass additional information from the analysis request to the service execution
	 */
	@ApiModelProperty(value="Optional additional properties to be passed to the discovery service", required=false)
	private Map<String, Object> additionalProperties;

	@ApiModelProperty(value="User id under which the discovery service is supposed to run", required=true)
	private String user;
	/**
	 * This property contains information about the data that is supposed to be analysed
	 */
	@ApiModelProperty(value="Data set to be analyzed along with cached metadata objects", required=true)
	private DataSetContainer dataSetContainer;

	@ApiModelProperty(value="Unique id of the analysis request to which the discovery service request belongs to", required=true)
	private String odfRequestId;

	@ApiModelProperty(value="URL of ODF admin API for remote access to metadata", required=false)
	private String odfUrl;

	@ApiModelProperty(value="ODF user id for remote access to metadata", required=false)
	private String odfUser;

	@ApiModelProperty(value="ODF password for remote access to metadata", required=false)
	private String odfPassword;
	/**
	 * timestamp of the time the request was put on the ODF request queue
	 */
	@ApiModelProperty(value="Timestamp when the request was put on ODF request queue", required=true)
	private long putOnRequestQueue;
	/**
	 * timestamp of the time the request was taken from the queue and execution was started
	 */
	@ApiModelProperty(value="Timestamp when the execution was started", required=true)
	private long takenFromRequestQueue;
	/**
	 * timestamp of the time the request was processed successfully
	 */
	@ApiModelProperty(value="Timestamp when processing was finished", required=true)
	private long finishedProcessing;
	/**
	 * duration needed for storing the analysis results
	 */
	@ApiModelProperty(value="Time needed for storing results in metadata repository", required=true)
	private long timeSpentStoringResults;

	public String getDiscoveryServiceId() {
		return discoveryServiceId;
	}

	public void setDiscoveryServiceId(String discoveryServiceId) {
		this.discoveryServiceId = discoveryServiceId;
	}

	public Map<String, Object> getAdditionalProperties() {
		return additionalProperties;
	}

	public void setAdditionalProperties(Map<String, Object> additionalProperties) {
		this.additionalProperties = additionalProperties;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public DataSetContainer getDataSetContainer() {
		return dataSetContainer;
	}

	public void setDataSetContainer(DataSetContainer dataSet) {
		this.dataSetContainer = dataSet;
	}

	public String getOdfRequestId() {
		return odfRequestId;
	}

	public void setOdfRequestId(String odfRequestId) {
		this.odfRequestId = odfRequestId;
	}

	public String getOdfUrl() {
		return odfUrl;
	}

	public void setOdfUrl(String odfUrl) {
		this.odfUrl = odfUrl;
	}

	public String getOdfUser() {
		return this.odfUser;
	}

	public void setOdfUser(String odfUser) {
		this.odfUser = odfUser;
	}

	public String getOdfPassword() {
		return this.odfPassword;
	}

	public void setOdfPassword(String odfPassword) {
		this.odfPassword = odfPassword;
	}

	public long getFinishedProcessing() {
		return finishedProcessing;
	}

	public void setFinishedProcessing(long finishedProcessing) {
		this.finishedProcessing = finishedProcessing;
	}

	public long getTakenFromRequestQueue() {
		return takenFromRequestQueue;
	}

	public void setTakenFromRequestQueue(long takenFromRequestQueue) {
		this.takenFromRequestQueue = takenFromRequestQueue;
	}

	public long getPutOnRequestQueue() {
		return putOnRequestQueue;
	}

	public void setPutOnRequestQueue(long putOnRequestQueue) {
		this.putOnRequestQueue = putOnRequestQueue;
	}

	public long getTimeSpentStoringResults() {
		return timeSpentStoringResults;
	}

	public void setTimeSpentStoringResults(long timeSpentStoringResults) {
		this.timeSpentStoringResults = timeSpentStoringResults;
	}

}

