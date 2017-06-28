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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

//JSON
@ApiModel(description = "Configuration of Spark cluster.")
public class SparkConfig {

	@ApiModelProperty(value = "Master URL of the Spark cluster", required = true)
	private String clusterMasterUrl = null;

	@ApiModelProperty(value="Custom Spark configuration options", required=false)
	private Map<String, Object> configs = new HashMap<>();

	public String getClusterMasterUrl() {
		return this.clusterMasterUrl;
	}

	public void setClusterMasterUrl(String clusterMasterUrl) {
		this.clusterMasterUrl = clusterMasterUrl;
	}

	@JsonAnyGetter
	public Map<String, Object> getConfigs() {
		return this.configs;
	}

	@JsonAnySetter
	public void setConfig(String name, Object value) {
		this.configs.put(name, value);
	}

}
