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
package org.apache.atlas.odf.api.engine;

import java.util.ArrayList;
import java.util.List;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description="Overall ODF system health.")
public class SystemHealth {
	
	public static enum HealthStatus {
		OK, WARNING, ERROR
	}

	@ApiModelProperty(value="ODF health status", readOnly=true, required=true)
	private HealthStatus status;

	@ApiModelProperty(value="List of status messages", readOnly=true)
	private List<String> messages = new ArrayList<>();

	@ApiModelProperty(value="Health status of the individual subsystems", readOnly=true)
	private List<SystemHealth> subSystemsHealth = new ArrayList<>();

	public HealthStatus getStatus() {
		return status;
	}

	public void setStatus(HealthStatus status) {
		this.status = status;
	}

	public List<String> getMessages() {
		return messages;
	}

	public void setMessages(List<String> messages) {
		this.messages = messages;
	}

	public List<SystemHealth> getSubSystemsHealth() {
		return subSystemsHealth;
	}

	public void setSubSystemsHealth(List<SystemHealth> subSystemsHealth) {
		this.subSystemsHealth = subSystemsHealth;
	}

}
