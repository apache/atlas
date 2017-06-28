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

import java.util.concurrent.ExecutorService;


public interface QueueMessageProcessor {

	/**
	 * callback to process the message taken from the queue.
	 * 
	 * @param executorService
	 * @param msg The message to be processed
	 * @param partition The kafka topic partition this message was read from
	 * @param msgOffset The offset of this particular message on this kafka partition
	 * @return
	 */
	void process(ExecutorService executorService, String msg, int partition, long msgOffset);

}
