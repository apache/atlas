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
package org.apache.atlas.odf.core.notification;

import org.apache.atlas.odf.api.OpenDiscoveryFramework;

public interface NotificationListener {
	
	/**
	 * A human readable name for this listener. Used for logging and management.
	 */
	String getName();
	
	/**
	 * The Kafka topic to listen on.
	 */
	String getTopicName();

	/**
	 * This is called whenever an event arrives. Typically, one would initiate
	 * some analysis request on the passed odf instance.
	 */
	void onEvent(String event, OpenDiscoveryFramework odf);
}
