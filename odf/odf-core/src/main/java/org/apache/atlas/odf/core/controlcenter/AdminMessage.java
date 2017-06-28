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
package org.apache.atlas.odf.core.controlcenter;

import org.apache.atlas.odf.core.configuration.ConfigContainer;

// JSON
public class AdminMessage {
	public static enum Type {
		SHUTDOWN, RESTART, CONFIGCHANGE
	}

	private Type adminMessageType;
	private String details;
	private ConfigContainer configUpdateDetails;
	private String messageId;

	public Type getAdminMessageType() {
		return adminMessageType;
	}

	public void setAdminMessageType(Type adminMessageType) {
		this.adminMessageType = adminMessageType;
	}

	public String getDetails() {
		return details;
	}

	public void setDetails(String details) {
		this.details = details;
	}

	public ConfigContainer getConfigUpdateDetails() {
		return configUpdateDetails;
	}

	public void setConfigUpdateDetails(ConfigContainer configUpdateDetails) {
		this.configUpdateDetails = configUpdateDetails;
	}

	public String getId() {
		return this.messageId;
	}

	public void setId(String messageId) {
		this.messageId = messageId;
	}
}
