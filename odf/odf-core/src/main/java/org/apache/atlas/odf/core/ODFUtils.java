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
package org.apache.atlas.odf.core;

import java.text.MessageFormat;
import java.util.List;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;

public class ODFUtils {
	public static int DEFAULT_TIMEOUT_SECS = 10 * 60; // 10 minutes

	public static AnalysisRequestStatus runSynchronously(AnalysisManager analysisManager, AnalysisRequest request) {
		return runSynchronously(analysisManager, request, DEFAULT_TIMEOUT_SECS); // default is 
	}

	public static AnalysisRequestStatus runSynchronously(AnalysisManager analysisManager, AnalysisRequest request, int timeoutInSeconds) {
		Logger logger = Logger.getLogger(ODFUtils.class.getName());
		AnalysisResponse response = analysisManager.runAnalysis(request);
		if (response.isInvalidRequest()) {
			AnalysisRequestStatus status = new AnalysisRequestStatus();
			status.setState(AnalysisRequestStatus.State.ERROR);
			status.setDetails(MessageFormat.format("Request was invalid. Details: {0}", response.getDetails()));
			status.setRequest(request);
			return status;
		}
		AnalysisRequestStatus status = null;
		long startTime = System.currentTimeMillis();
		boolean timeOutReached = false;
		do {
			logger.fine("Polling for result...");
			status = analysisManager.getAnalysisRequestStatus(response.getId());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			long currentTime = System.currentTimeMillis();
			timeOutReached = (currentTime - startTime) > (timeoutInSeconds * 1000);
		} while ((AnalysisRequestStatus.State.ACTIVE.equals(status.getState()) || AnalysisRequestStatus.State.QUEUED.equals(status.getState()) //
				&& !timeOutReached));
		return status;

	}

	public static AnalysisRequestStatus.State combineStates(List<AnalysisRequestStatus.State> allStates) {
		// if one of the requests is in error, so is the complete request
		if (allStates.contains(AnalysisRequestStatus.State.ERROR)) {
			return AnalysisRequestStatus.State.ERROR;
		}
		// if no request could be found -> not found
		if (Utils.containsOnly(allStates, new AnalysisRequestStatus.State[] { AnalysisRequestStatus.State.NOT_FOUND })) {
			return AnalysisRequestStatus.State.NOT_FOUND;
		}
		// if all request are either not found or finished -> finished
		if (Utils.containsOnly(allStates, new AnalysisRequestStatus.State[] { AnalysisRequestStatus.State.NOT_FOUND, AnalysisRequestStatus.State.FINISHED })) {
			return AnalysisRequestStatus.State.FINISHED;
		}
		// else always return active
		return AnalysisRequestStatus.State.ACTIVE;
	}
}
