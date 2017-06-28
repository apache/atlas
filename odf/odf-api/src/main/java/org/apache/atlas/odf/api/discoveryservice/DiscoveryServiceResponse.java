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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncStartResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
@JsonTypeInfo(  
	    use = JsonTypeInfo.Id.NAME,  
	    include = JsonTypeInfo.As.PROPERTY,  
	    property = "type")

@JsonSubTypes({  
    @Type(value = DiscoveryServiceAsyncStartResponse.class, name = "async"),  
    @Type(value = DiscoveryServiceSyncResponse.class, name = "sync") })  
@ApiModel(description="Response returned by the discovery service.", subTypes={DiscoveryServiceAsyncStartResponse.class,DiscoveryServiceSyncResponse.class}, discriminator="type")
public abstract class DiscoveryServiceResponse {
	public static enum ResponseCode {
		OK, NOT_AUTHORIZED, TEMPORARILY_UNAVAILABLE, UNKNOWN_ERROR
	};

	@ApiModelProperty(value="Response code indicating whether the discovery service request was issued successfully", readOnly=true, required=true)
	private ResponseCode code;

	@ApiModelProperty(value="Detailed status of the analysis request", readOnly=true, required=false)
	private String details;

	public ResponseCode getCode() {
		return code;
	}

	public void setCode(ResponseCode code) {
		this.code = code;
	}

	public String getDetails() {
		return details;
	}

	public void setDetails(String details) {
		this.details = details;
	}

}
