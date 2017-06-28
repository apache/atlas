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
package org.apache.atlas.odf.api.analysis;

public class AnalysisRequestTrackerStatus {
	public static enum STATUS {
		INITIALIZED, //tracker was created, nothing else happened so far
		IN_DISCOVERY_SERVICE_QUEUE, //tracker is put on queue but not running yet
		DISCOVERY_SERVICE_RUNNING, //only for async services, analysis is running
		FINISHED, //analysis finished
		ERROR, // an error occurred during analysis / processing
		CANCELLED //the analysis was cancelled by the user
	};
}
