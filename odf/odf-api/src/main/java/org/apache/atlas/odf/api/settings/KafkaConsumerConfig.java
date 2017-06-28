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

import org.apache.atlas.odf.api.settings.validation.EnumValidator;
import org.apache.atlas.odf.api.settings.validation.NumberPositiveValidator;
import org.apache.atlas.odf.api.settings.validation.ValidationException;

/*
 * This class is final, because reflection is used to access getters / setters in order to merge. This doesn't work with inherited methods
 */
public final class KafkaConsumerConfig {
	/*
	 * ############ !!!!!!!!!!!!!!!!!!! ###################
	 * 
	 * Because of a jackson defect, JsonProperty annotations must be on all properties AND their getters and setters!
	 * 
	 * https://github.com/FasterXML/jackson-module-scala/issues/197
	 */

	private String offsetsStorage;

	private Long zookeeperSessionTimeoutMs;

	private Long zookeeperConnectionTimeoutMs;

	public String getOffsetsStorage() {
		return offsetsStorage;
	}

	public void setOffsetsStorage(String offsetsStorage) {
		this.offsetsStorage = offsetsStorage;
	}

	public Long getZookeeperSessionTimeoutMs() {
		return zookeeperSessionTimeoutMs;
	}

	public void setZookeeperSessionTimeoutMs(Long zookeeperSessionTimeoutMs) {
		this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
	}

	public Long getZookeeperConnectionTimeoutMs() {
		return zookeeperConnectionTimeoutMs;
	}

	public void setZookeeperConnectionTimeoutMs(Long zookeeperConnectionTimeoutMs) {
		this.zookeeperConnectionTimeoutMs = zookeeperConnectionTimeoutMs;
	}

	public void validate() throws ValidationException {
		if (getOffsetsStorage() != null) {
			new EnumValidator("kafka", "zookeeper").validate("KafkaConsumerConfig.offsetsStorage", this.offsetsStorage);
		}
		if (getZookeeperConnectionTimeoutMs() != null) {
			new NumberPositiveValidator().validate("KafkaConsumerConfig.zookeeperConnectionTimeoutMs", this.zookeeperConnectionTimeoutMs);
		}
		if (getZookeeperSessionTimeoutMs() != null) {
			new NumberPositiveValidator().validate("KafkaConsumerConfig.zookeeperSessionTimeoutMs", this.zookeeperSessionTimeoutMs);
		}
	}

}
