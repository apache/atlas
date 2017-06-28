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
package org.apache.atlas.odf.core.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisCancelResult;
import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus.State;
import org.apache.atlas.odf.api.analysis.AnalysisRequestSummary;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackers;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.ODFUtils;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.core.controlcenter.AnalysisRequestTrackerStore;
import org.apache.atlas.odf.core.controlcenter.ControlCenter;
import org.apache.atlas.odf.json.JSONUtils;

/**
 *
 * External Java API for creating and managing analysis requests
 *
 */
public class AnalysisManagerImpl implements AnalysisManager {

	public final static char COMPOUND_REQUEST_SEPARATOR = ',';
	private Logger logger = Logger.getLogger(AnalysisManagerImpl.class.getName());
	private ControlCenter controlCenter;

	public AnalysisManagerImpl() {
		controlCenter = new ODFInternalFactory().create(ControlCenter.class);
	}

	/**
	 * Issues a new ODF analysis request
	 *
	 * @param request Analysis request
	 * @return Response containing the request id and status information
	 */
	public AnalysisResponse runAnalysis(AnalysisRequest request) {
		if (((request.getDiscoveryServiceSequence() == null) || request.getDiscoveryServiceSequence().isEmpty())
			&& ((request.getAnnotationTypes() == null) || request.getAnnotationTypes().isEmpty())) {
			AnalysisResponse response = new AnalysisResponse();
			response.setId(request.getId());
			response.setDetails("Either a sequence of discovery service ids or a list of annotation types must be specified to initiate an analysis request.");
			response.setInvalidRequest(true);
			return response;
		}

		if ((request.getDataSets().size() == 1) || request.isProcessDataSetsSequentially()) {
			logger.log(Level.INFO, "Using sequential request processing (maybe because there is only a single data set)");
			AnalysisResponse response = controlCenter.startRequest(request);
			logger.log(Level.INFO, "Request with ID ''{0}'' started on data sets ''{1}''. Complete request: {2}.",
					new Object[] { response.getId(), request.getDataSets(), JSONUtils.lazyJSONSerializer(request) });
			return response;
		}

		List<String> requestIDs = new ArrayList<String>();
		List<String> detailsMessages = new ArrayList<String>();
		boolean invalidRequest = true;
		logger.log(Level.INFO, "Running requests for ''{0}'' data sets in parallel", request.getDataSets().size());
		logger.log(Level.FINE, "Splitting request into multiple request for each data set. Data Sets: {0}", request.getDataSets());
		for (MetaDataObjectReference dataSet : request.getDataSets()) {
			AnalysisRequest partRequest = new AnalysisRequest();
			partRequest.setDiscoveryServiceSequence(request.getDiscoveryServiceSequence());
			partRequest.setAdditionalProperties(request.getAdditionalProperties());
			partRequest.setDataSets(Collections.singletonList(dataSet));
			AnalysisResponse partResponse = controlCenter.startRequest(partRequest);
			if (!partResponse.isInvalidRequest()) {
				String partRequestID = partResponse.getId();
				requestIDs.add(partRequestID);
				detailsMessages.add(partResponse.getDetails());
				// as soon as one request is valid, we make the compound request valid
				invalidRequest = false;
			}
		}
		AnalysisResponse response = new AnalysisResponse();
		response.setId(Utils.joinStrings(requestIDs, COMPOUND_REQUEST_SEPARATOR));
		response.setDetails(Utils.joinStrings(detailsMessages, COMPOUND_REQUEST_SEPARATOR));
		response.setInvalidRequest(invalidRequest);
		return response;
	}

	/**
	 * Retrieve status of an ODF analysis request
	 *
	 * @param requestId Unique id of the analysis request
	 * @return Status of the analysis request
	 */
	public AnalysisRequestStatus getAnalysisRequestStatus(String requestId) {
		List<String> singleRequestIds = Utils.splitString(requestId, COMPOUND_REQUEST_SEPARATOR);
		if (singleRequestIds.size() == 1) {
			AnalysisRequestStatus status = controlCenter.getRequestStatus(requestId);
			return status;
		}
		AnalysisRequestStatus compoundStatus = new AnalysisRequestStatus();
		compoundStatus.setState(State.QUEUED);
		AnalysisRequest compoundRequest = new AnalysisRequest(); // assemble a compound request 
		compoundRequest.setId(requestId);
		List<String> allMessages = new ArrayList<String>();
		List<MetaDataObjectReference> allDataSets = new ArrayList<>();
		List<State> allStates = new ArrayList<>();
		for (String singleRequestId : singleRequestIds) {	
			AnalysisRequestStatus singleStatus = controlCenter.getRequestStatus(singleRequestId);
			if (compoundRequest.getDiscoveryServiceSequence() == null) {
				// assume all fields of the single requests are the same
				// since they were created through runAnalysis()
				compoundRequest.setDiscoveryServiceSequence(singleStatus.getRequest().getDiscoveryServiceSequence());
				compoundRequest.setAdditionalProperties(singleStatus.getRequest().getAdditionalProperties());
			}
			if (singleStatus.getRequest().getDataSets() != null) {
				allDataSets.addAll(singleStatus.getRequest().getDataSets());
			}
			allStates.add(singleStatus.getState());
			allMessages.add(singleStatus.getDetails());
		}
		compoundRequest.setDataSets(allDataSets);

		compoundStatus.setState(ODFUtils.combineStates(allStates));
		compoundStatus.setRequest(compoundRequest);
		compoundStatus.setDetails(Utils.joinStrings(allMessages, COMPOUND_REQUEST_SEPARATOR));
		return compoundStatus;
	}

	/**
	 * Retrieve statistics about all previous ODF analysis requests
	 *
	 * @return Request summary
	 */
	public AnalysisRequestSummary getAnalysisStats() {
		AnalysisRequestTrackerStore store = new ODFInternalFactory().create(AnalysisRequestTrackerStore.class);
		return store.getRequestSummary();
	}

	/**
	 * Retrieve status details of recent ODF analysis requests
	 *
	 * @param offset Starting offset (use 0 to start with the latest request)
	 * @param limit Maximum number of analysis requests to be returned (use -1 to retrieve all requests)
	 * @return Status details for each discovery request
	 */
	public AnalysisRequestTrackers getAnalysisRequests(int offset, int limit) {
		AnalysisRequestTrackerStore store = new ODFInternalFactory().create(AnalysisRequestTrackerStore.class);
		AnalysisRequestTrackers analysisrequestTrackers = new AnalysisRequestTrackers();
		analysisrequestTrackers.setAnalysisRequestTrackers(store.getRecentTrackers(offset, limit));
		return analysisrequestTrackers;
	}

	/**
	 * Request a specific ODF discovery request to be canceled
	 *
	 * @param requestId Unique id of the analysis request
	 * @return Status of the cancellation attempt
	 */
	public AnalysisCancelResult cancelAnalysisRequest(String requestId) {
		return controlCenter.cancelRequest(requestId);
	}
}
