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

@ApiModel(description="Status of an individual Kafka topic")
public class KafkaTopicStatus {
	
	@ApiModelProperty(value="Kafka topic", readOnly=true, required=true)
	private String topic;

	@ApiModelProperty(value="Information on Kafka offsets per group id (can be used by the admin to track how many messages are still waiting to be consumed)", readOnly=true, required=true)
	private List<KafkaGroupOffsetInfo> consumerGroupOffsetInfo = new ArrayList<KafkaGroupOffsetInfo>();

	@ApiModelProperty(value="List of Kafka partitions and the nodes they belong to", readOnly=true, required=true)
	private List<KafkaPartitionInfo> partitionBrokersInfo = new ArrayList<KafkaPartitionInfo>();

	@ApiModelProperty(value="Message counts of individual brokers", readOnly=true, required=true)
	private List<KafkaBrokerPartitionMessageCountInfo> brokerPartitionMessageCountInfo = new ArrayList<KafkaBrokerPartitionMessageCountInfo>();

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public List<KafkaGroupOffsetInfo> getConsumerGroupOffsetInfo() {
		return consumerGroupOffsetInfo;
	}

	public void setConsumerGroupOffsetInfo(List<KafkaGroupOffsetInfo> offsetInfoList) {
		this.consumerGroupOffsetInfo = offsetInfoList;
	}

	public List<KafkaPartitionInfo> getPartitionBrokersInfo() {
		return partitionBrokersInfo;
	}

	public void setPartitionBrokersInfo(List<KafkaPartitionInfo> partitionBrokersMap) {
		this.partitionBrokersInfo = partitionBrokersMap;
	}

	public List<KafkaBrokerPartitionMessageCountInfo> getBrokerPartitionMessageInfo() {
		return brokerPartitionMessageCountInfo;
	}

	public void setBrokerPartitionMessageInfo(List<KafkaBrokerPartitionMessageCountInfo> brokerInfo) {
		this.brokerPartitionMessageCountInfo = brokerInfo;
	}

}
