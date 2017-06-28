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

import java.util.List;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestSummary;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;


public interface AnalysisRequestTrackerStore {
	
	/**
	 * set the status of old requests which were last modified before the cutOffTimestamp
	 * with an optional detailsMessage
	 */
	void setStatusOfOldRequest(long cutOffTimestamp, STATUS status, String detailsMessage);
	
	// store / update the passed tracker
	void store(AnalysisRequestTracker tracker);
	
	AnalysisRequestTracker query(String analysisRequestId);

	AnalysisRequestTracker findSimilarQueuedRequest(AnalysisRequest request);
	
	/**
	 * @param number - number of trackers to retrieve, -1 for all
	 * @return
	 */
	List<AnalysisRequestTracker> getRecentTrackers(int offset, int limit);
	
	/**
	 * Clear any internal caches, if any.
	 */
	void clearCache(); 

	int getSize();

	AnalysisRequestSummary getRequestSummary();
}
