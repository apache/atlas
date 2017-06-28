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

@ApiModel(description="Information on Kafka offsets per group id")
public class KafkaGroupOffsetInfo {
	@ApiModelProperty(value="Kafka group id", readOnly=true, required=true)
	private String groupId;

	@ApiModelProperty(value="List of Kafka offsets", readOnly=true, required=true)
	private List<PartitionOffsetInfo> offsets = new ArrayList<PartitionOffsetInfo>();

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public List<PartitionOffsetInfo> getOffsets() {
		return offsets;
	}

	public void setOffsets(List<PartitionOffsetInfo> offsets) {
		this.offsets = offsets;
	}
}
