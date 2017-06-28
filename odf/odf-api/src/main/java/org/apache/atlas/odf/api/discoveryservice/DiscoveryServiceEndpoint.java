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

import java.util.HashMap;
import java.util.Map;

import org.apache.wink.json4j.JSONException;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import io.swagger.annotations.ApiModel;

//JSON
@ApiModel(description="Endpoint of the discovery service.")
public class DiscoveryServiceEndpoint {
	private String runtimeName;
	private Map<String, Object> props = new HashMap<>();

	public String getRuntimeName() {
		return runtimeName;
	}

	public void setRuntimeName(String runtimeName) {
		this.runtimeName = runtimeName;
	}
	
	@JsonAnyGetter
	public Map<String, Object> get() {
		return props;
	}

	@JsonAnySetter
	public void set(String name, Object value) {
		props.put(name, value);
	}
	
}
