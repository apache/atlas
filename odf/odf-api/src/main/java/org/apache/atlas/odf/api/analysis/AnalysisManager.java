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

/**
 *
 * External interface for creating and managing analysis requests
 *
 */
public interface AnalysisManager {

	/**
	 * Issues a new ODF analysis request
	 *
	 * @param request Analysis request
	 * @return Response containing the request id and status information
	 */
	public AnalysisResponse runAnalysis(AnalysisRequest request);

	/**
	 * Retrieve status of an ODF analysis request
	 *
	 * @param requestId Unique id of the analysis request
	 * @return Status of the analysis request
	 */
	public AnalysisRequestStatus getAnalysisRequestStatus(String requestId);

	/**
	 * Retrieve statistics about all previous ODF analysis requests
	 *
	 * @return Request summary
	 */
	public AnalysisRequestSummary getAnalysisStats();

	/**
	 * Retrieve status details of recent ODF analysis requests
	 *
	 * @param offset Starting offset (use 0 to start with the latest request)
	 * @param limit Maximum number of analysis requests to be returned (use -1 to retrieve all requests)
	 * @return Status details for each discovery request
	 */
	public AnalysisRequestTrackers getAnalysisRequests(int offset, int limit);

	/**
	 * Request a specific ODF discovery request to be canceled
	 *
	 * @param requestId Unique id of the analysis request
	 * @return Status of the cancellation attempt
	 */
	public AnalysisCancelResult cancelAnalysisRequest(String requestId);
}
