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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description="Status of the ODF thread manager")
public class ThreadStatus {

	public static enum ThreadState { RUNNING, FINISHED, NON_EXISTENT }

	@ApiModelProperty(value="Thread id", readOnly=true)
	private String id;

	@ApiModelProperty(value="Thread status", readOnly=true)
	private ThreadState state;

	@ApiModelProperty(value="Thread type", readOnly=true)
	private String type;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public ThreadState getState() {
		return state;
	}

	public void setState(ThreadState state) {
		this.state = state;
	}

}
