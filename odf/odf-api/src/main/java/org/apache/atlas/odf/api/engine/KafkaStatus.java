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

@ApiModel(description="Status of the Kafka ODF queues")
public class KafkaStatus extends MessagingStatus {
	@ApiModelProperty(value="List of message brokers", readOnly=true)
	private List<String> brokers = new ArrayList<String>();

	@ApiModelProperty(value="Status of the individual topics", readOnly=true)
	private List<KafkaTopicStatus> topicStatus = new ArrayList<KafkaTopicStatus>();

	public List<String> getBrokers() {
		return brokers;
	}

	public void setBrokers(List<String> brokers) {
		this.brokers = brokers;
	}

	public List<KafkaTopicStatus> getTopicStatus() {
		return topicStatus;
	}

	public void setTopicStatus(List<KafkaTopicStatus> topicStatus) {
		this.topicStatus = topicStatus;
	}

}
