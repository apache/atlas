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

@ApiModel(description="Kafka nodes belonging to a specific partition")
public class KafkaPartitionInfo {
	@ApiModelProperty(value="Partition id", readOnly=true, required=true)
	private Integer partitionId;

	@ApiModelProperty(value="List of nodes containing this partition", readOnly=true, required=true)
	private List<BrokerNode> nodes;

	public List<BrokerNode> getNodes() {
		return nodes;
	}

	public void setNodes(List<BrokerNode> nodes) {
		this.nodes = nodes;
	}

	public Integer getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(Integer partitionId) {
		this.partitionId = partitionId;
	}

}
