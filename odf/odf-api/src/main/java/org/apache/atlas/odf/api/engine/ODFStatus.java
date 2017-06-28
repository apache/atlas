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

import java.util.List;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description="Overall ODF status.")
public class ODFStatus {

	@ApiModelProperty(value="Status of the ODF queues", readOnly=true)
	private MessagingStatus messagingStatus;

	@ApiModelProperty(value="Status of the ODF thread manager", readOnly=true)
	private List<ThreadStatus> threadManagerStatus;

	public MessagingStatus getMessagingStatus() {
		return this.messagingStatus;
	}

	public void setMessagingStatus(MessagingStatus messagingStatus) {
		this.messagingStatus = messagingStatus;
	}

	public List<ThreadStatus> getThreadManagerStatus() {
		return this.threadManagerStatus;
	}

	public void setThreadManagerStatus(List<ThreadStatus> threadManagerStatus) {
		this.threadManagerStatus = threadManagerStatus;
	}
}
