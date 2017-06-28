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

/**
*
* External Java API for managing and controlling the ODF engine
*
*/
public interface EngineManager {

	/**
	 * Checks the health status of ODF
	 *
	 * @return Health status of the ODF engine
	 */
	public SystemHealth checkHealthStatus();

	
	/**
	 * Get information about all available service runtimes.
	 * 
	 * @return Runtimes info
	 */
	ServiceRuntimesInfo getRuntimesInfo();
	
	/**
	 * Returns the status of the ODF thread manager
	 *
	 * @return Status of all threads making up the ODF thread manager
	 */
	public List<ThreadStatus> getThreadManagerStatus();

	/**
	 * Returns the status of the ODF messaging subsystem
	 *
	 * @return Status of the ODF messaging subsystem
	 */
	public MessagingStatus getMessagingStatus();

	/**
	 * Returns the status of the messaging subsystem and the internal thread manager
	 *
	 * @return Combined status of the messaging subsystem and the internal thread manager
	 */
	public ODFStatus getStatus();

	/**
	 * Returns the current ODF version
	 *
	 * @return ODF version identifier
	 */
	public ODFVersion getVersion();

	/**
	 * Shuts down the ODF engine, purges all scheduled analysis requests from the queues, and cancels all running analysis requests.
	 * This means that all running jobs will be cancelled or their results will not be reported back.
	 * (for debugging purposes only)
	 * 
	 * @param options Option for immediately restarting the engine after shutdown (default is not to restart immediately but only when needed) 
	 */
	public void shutdown(ODFEngineOptions options);
}
