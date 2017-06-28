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
package org.apache.atlas.odf.api.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.atlas.odf.api.settings.validation.NumberPositiveValidator;
import org.apache.atlas.odf.api.settings.validation.ValidationException;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description="Messaging configuration to be used for queuing requests.")
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class MessagingConfiguration {
	@ApiModelProperty(value="Time analysis requests are retained on the queue in milliseconds")
	private Long analysisRequestRetentionMs;
	
	public Long getAnalysisRequestRetentionMs() {
		return analysisRequestRetentionMs;
	}

	public void setAnalysisRequestRetentionMs(Long analysisRequestRetentionMs) {
		this.analysisRequestRetentionMs = analysisRequestRetentionMs;
	}
	
	public void validate() throws ValidationException {
		if (this.getAnalysisRequestRetentionMs() != null) {
			new NumberPositiveValidator().validate("ODFConfig.analysisRequestRetentionMs", this.analysisRequestRetentionMs);
		}
	}
}
