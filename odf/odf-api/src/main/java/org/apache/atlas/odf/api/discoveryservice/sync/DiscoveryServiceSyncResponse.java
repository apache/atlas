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
package org.apache.atlas.odf.api.discoveryservice.sync;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResult;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
/**
 * An object of this class must be returned by a synchronous discovery service
 *
 */
@ApiModel(description="Response returned by a synchronous discovery service.")
public class DiscoveryServiceSyncResponse extends DiscoveryServiceResponse {
	@ApiModelProperty(value="Result of the analysis (synchronous requests only)", readOnly=true, required=true)
	private DiscoveryServiceResult result;

	public DiscoveryServiceResult getResult() {
		return result;
	}

	public void setResult(DiscoveryServiceResult result) {
		this.result = result;
	}

}
