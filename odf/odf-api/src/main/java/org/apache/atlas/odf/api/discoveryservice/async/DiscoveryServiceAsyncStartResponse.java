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

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * An object of this class must be returned by an asynchronous service after starting
 *
 */
@ApiModel(description="Response returned by an asynchronous discovery service.")
public class DiscoveryServiceAsyncStartResponse extends DiscoveryServiceResponse {
	/**
	 * Property identifying the running analysis. This id will be used to repeatedly request the status of the analysis.
	 */
	@ApiModelProperty(value="Id of the analysis request (asynchronous requests only)", readOnly=true, required=true)
	private String runId;

	public String getRunId() {
		return runId;
	}

	public void setRunId(String runId) {
		this.runId = runId;
	}

}
