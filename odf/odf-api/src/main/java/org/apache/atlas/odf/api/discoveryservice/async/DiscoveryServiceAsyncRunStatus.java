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
package org.apache.atlas.odf.api.discoveryservice.async;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResult;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
/**
 * 
 * An object of this class must be returned when ODF requests the status of an analysis run.
 *
 */
@ApiModel(description="Status of an asynchronous discovery service run.")
public class DiscoveryServiceAsyncRunStatus {
	public static enum State {
		RUNNING, ERROR, NOT_FOUND, FINISHED
	}

	@ApiModelProperty(value="Id of the discovery service run", readOnly=true, required=true)
	private String runId;

	@ApiModelProperty(value="Status of the discovery service run", readOnly=true, required=true)
	private State state;

	@ApiModelProperty(value="Optional status message", readOnly=true)
	private String details;

	@ApiModelProperty(value="Result of the discovery service run (if already available)", readOnly=true)
	private DiscoveryServiceResult result;

	public String getRunId() {
		return runId;
	}

	public void setRunId(String runId) {
		this.runId = runId;
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

	public DiscoveryServiceResult getResult() {
		return result;
	}

	public void setResult(DiscoveryServiceResult result) {
		this.result = result;
	}

}
