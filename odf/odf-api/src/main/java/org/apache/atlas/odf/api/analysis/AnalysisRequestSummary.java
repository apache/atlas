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

@ApiModel(description="Status summary of all analysis requests submitted since last start of ODF.")
public class AnalysisRequestSummary {

	@ApiModelProperty(value="Number of successful analysis requests", readOnly=true, required=true)
	private int success;

	@ApiModelProperty(value="Number of failing analysis requests", readOnly=true, required=true)
	private int failure;

	AnalysisRequestSummary() {
	}

	public AnalysisRequestSummary(int success, int failure) {
		this.success = success;
		this.failure = failure;
	}
	
	public int getSuccess() {
		return this.success;
	}

	public void setSuccess(int success) {
		this.success = success;
	}

	public int getFailure() {
		return this.failure;
	}

	public void setFailure(int failure) {
		this.failure = failure;
	}

}
