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

import java.util.HashMap;
import java.util.Map;

public class KafkaBrokerPartitionMessageCountInfo {

	private String broker;
	private Map<Integer, Long> partitionMsgCountMap = new HashMap<Integer, Long>();

	public String getBroker() {
		return broker;
	}

	public void setBroker(String broker) {
		this.broker = broker;
	}

	public Map<Integer, Long> getPartitionMsgCountMap() {
		return partitionMsgCountMap;
	}

	public void setPartitionMsgCountMap(Map<Integer, Long> partitionMsgCountMap) {
		this.partitionMsgCountMap = partitionMsgCountMap;
	}
}
