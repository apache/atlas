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

import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.discoveryservice.DataSetCheckResult;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse.ResponseCode;
import org.apache.atlas.odf.api.discoveryservice.async.AsyncDiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncRunStatus;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncStartResponse;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.core.Utils;

public class AsyncDiscoveryServiceWrapper implements SyncDiscoveryService {

	AsyncDiscoveryService wrappedService = null;

	public AsyncDiscoveryServiceWrapper(AsyncDiscoveryService wrappedService) {
		this.wrappedService = wrappedService;
	}

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
		try {
			DiscoveryServiceAsyncStartResponse asyncResponse = wrappedService.startAnalysis(request);
			ResponseCode code = asyncResponse.getCode();
			if (code != ResponseCode.OK) {
				DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
				response.setCode(code);
				response.setDetails(asyncResponse.getDetails());
				return response;
			}
			// poll the async service
			final long maxWaitTimeSecs = Utils.getEnvironmentProperty("odf.async.max.wait.secs", 10 * 60); // default: 10 minutes
			final long pollingIntervalMS = Utils.getEnvironmentProperty("odf.async.poll.interval.ms", 1000);
			long maxPolls = (maxWaitTimeSecs * 1000) / pollingIntervalMS;
			int pollCounter = 0;

			DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
			String runId = asyncResponse.getRunId();
			while (pollCounter < maxPolls) {
				Thread.sleep(pollingIntervalMS);
				DiscoveryServiceAsyncRunStatus status = wrappedService.getStatus(runId);
				switch (status.getState()) {
				case NOT_FOUND:
					// should not happen
					response.setCode(ResponseCode.UNKNOWN_ERROR);
					response.setDetails("Run ID " + runId + " was not found. This should not have happened.");
					return response;
				case ERROR:
					response.setCode(ResponseCode.UNKNOWN_ERROR);
					response.setDetails(status.getDetails());
					return response;
				case FINISHED:
					response.setCode(ResponseCode.OK);
					response.setDetails(status.getDetails());
					response.setResult(status.getResult());
					return response;
				default:
					// continue polling
					pollCounter++;
				}
			}
			response.setCode(ResponseCode.UNKNOWN_ERROR);
			response.setDetails("Polled Async service for " + maxWaitTimeSecs + " seconds without positive result");
			return response;
		} catch (Exception exc) {
			DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
			response.setCode(ResponseCode.UNKNOWN_ERROR);
			response.setDetails("An unknown error occurred: " + Utils.getExceptionAsString(exc));
			return response;
		}
	}

	public void setExecutorService(ExecutorService executorService) {
		wrappedService.setExecutorService(executorService);
	}

	public void setMetadataStore(MetadataStore metadataStore) {
		wrappedService.setMetadataStore(metadataStore);
	}

	public void setAnnotationStore(AnnotationStore annotationStore) {
		wrappedService.setAnnotationStore(annotationStore);
	}

	public DataSetCheckResult checkDataSet(DataSetContainer dataSetContainer) {
		return wrappedService.checkDataSet(dataSetContainer);
	}

}
