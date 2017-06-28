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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description="Status of a discovery service")
public class DiscoveryServiceStatus {
	public static enum Status {
		OK, ERROR
	};

	/**
	 * JSON object representing the status of a discovery service.
	 */

	@ApiModelProperty(value="Status of the ODF service", allowableValues="OK,ERROR", readOnly=true, required=true)
	Status status;

	@ApiModelProperty(value="Status message", readOnly=true, required=true)
	String message;
	
	@ApiModelProperty(value="Status count of the discovery service", readOnly=true, required=true)
	ServiceStatusCount statusCount;

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public ServiceStatusCount getStatusCount() {
		return statusCount;
	}

	public void setStatusCount(ServiceStatusCount statusCount) {
		this.statusCount = statusCount;
	}
	
}
