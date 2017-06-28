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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
@ApiModel(description="Response returned by the analysis request.")
public class AnalysisResponse {
	@ApiModelProperty(value="Unique request id", readOnly=true, required=true)
	private String id;

	@ApiModelProperty(value="Original request that is equivalent to the submitted one which is therefore skipped", readOnly=true, required=true)
	private AnalysisRequest originalRequest;

	private boolean isInvalidRequest = false;

	@ApiModelProperty(value="Details about why the request is invalid", readOnly=true, required=false)
	private String details;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public AnalysisRequest getOriginalRequest() {
		return originalRequest;
	}

	public void setOriginalRequest(AnalysisRequest originalRequest) {
		this.originalRequest = originalRequest;
	}

	@ApiModelProperty(name="isInvalidRequest", value="Indicates whether the submitted request is invalid", readOnly=true, required=true)
	public boolean isInvalidRequest() {
		return isInvalidRequest;
	}

	public void setInvalidRequest(boolean isInvalidRequest) {
		this.isInvalidRequest = isInvalidRequest;
	}

	public String getDetails() {
		return details;
	}

	public void setDetails(String details) {
		this.details = details;
	}

}
