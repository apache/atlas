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
package org.apache.atlas.odf.core.test.discoveryservice;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceBase;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.async.AsyncDiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncRunStatus;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncStartResponse;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;

public class TestAsyncDiscoveryServiceWritingAnnotations1 extends DiscoveryServiceBase implements AsyncDiscoveryService {

	static Logger logger = ODFTestLogger.get();

	static Map<String, MyThread> id2Thread = Collections.synchronizedMap(new HashMap<String, MyThread>());

	class MyThread extends Thread {

		String errorMessage = null;
		String correlationId;
		MetaDataObjectReference dataSetRef;

		public MyThread(MetaDataObjectReference dataSetRef, String correlationId) {
			super();
			this.dataSetRef = dataSetRef;
			this.correlationId = correlationId;
		}

		@Override
		public void run() {
			this.errorMessage = TestSyncDiscoveryServiceWritingAnnotations1.createAnnotations(dataSetRef, correlationId, metadataStore, annotationStore);
		}

	}

	@Override
	public DiscoveryServiceAsyncStartResponse startAnalysis(DiscoveryServiceRequest request) {
		DiscoveryServiceAsyncStartResponse response = new DiscoveryServiceAsyncStartResponse();
		MetaDataObjectReference dataSetRef = request.getDataSetContainer().getDataSet().getReference();

		String newRunID = "RunId-" + this.getClass().getSimpleName() + "-" + UUID.randomUUID().toString();
		MyThread t = new MyThread(dataSetRef, (String) request.getAdditionalProperties().get(TestSyncDiscoveryServiceWritingAnnotations1.REQUEST_PROPERTY_CORRELATION_ID));
		t.start();
		id2Thread.put(newRunID, t);
		response.setCode(DiscoveryServiceResponse.ResponseCode.OK);
		response.setRunId(newRunID);
		response.setDetails("Thread started");
		logger.info("Analysis writing annotations has started");

		return response;
	}

	@Override
	public DiscoveryServiceAsyncRunStatus getStatus(String runId) {
		DiscoveryServiceAsyncRunStatus status = new DiscoveryServiceAsyncRunStatus();

		MyThread t = id2Thread.get(runId);
		status.setRunId(runId);
		if (t == null) {
			status.setState(DiscoveryServiceAsyncRunStatus.State.NOT_FOUND);
		} else {
			java.lang.Thread.State ts = t.getState();
			if (!ts.equals(java.lang.Thread.State.TERMINATED)) {
				status.setState(DiscoveryServiceAsyncRunStatus.State.RUNNING);
			} else {
				if (t.errorMessage != null) {
					status.setState(DiscoveryServiceAsyncRunStatus.State.ERROR);
					status.setDetails(t.errorMessage);
				} else {
					status.setState(DiscoveryServiceAsyncRunStatus.State.FINISHED);
					status.setDetails("All went fine");
				}
			}
		}
		logger.info("Status of analysis with annotations: " + status.getState() + ", " + status.getDetails());
		return status;
	}

}
