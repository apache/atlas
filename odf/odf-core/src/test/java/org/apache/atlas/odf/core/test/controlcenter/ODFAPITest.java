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
package org.apache.atlas.odf.core.test.controlcenter;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.DataSet;
import org.apache.atlas.odf.api.metadata.models.UnknownDataSet;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.AnalysisRequestTrackerStore;
import org.apache.atlas.odf.core.controlcenter.DefaultStatusQueueStore;
import org.apache.atlas.odf.core.metadata.DefaultMetadataStore;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisCancelResult;
import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;

public class ODFAPITest extends ODFTestBase {

	public static int WAIT_MS_BETWEEN_POLLING = 500;
	public static int MAX_NUMBER_OF_POLLS = 500;
	public static String DUMMY_SUCCESS_ID = "success";
	public static String DUMMY_ERROR_ID = "error";

	public static void runRequestAndCheckResult(String dataSetID, AnalysisRequestStatus.State expectedState, int expectedProcessedDiscoveryRequests) throws Exception{
		runRequestAndCheckResult(Collections.singletonList(dataSetID), expectedState, expectedProcessedDiscoveryRequests);
	}
	
	public static void runRequestAndCheckResult(List<String> dataSetIDs, AnalysisRequestStatus.State expectedState, int expectedProcessedDiscoveryRequests) throws Exception{
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();
		String id = runRequest(dataSetIDs, analysisManager);
		log.info("Running request "+id+" on data sets: " + dataSetIDs);
		AnalysisRequestStatus status = null;

		int maxPolls = MAX_NUMBER_OF_POLLS;
		do {
			status = analysisManager.getAnalysisRequestStatus(id);
			log.log(Level.INFO, "{4}th poll request for request ID ''{0}'' (expected state: ''{3}''): state: ''{1}'', details: ''{2}''", new Object[] { id, status.getState(), status.getDetails(),
					expectedState, (MAX_NUMBER_OF_POLLS-maxPolls) });
			maxPolls--;
			Thread.sleep(WAIT_MS_BETWEEN_POLLING);
		} while (maxPolls > 0 && (status.getState() == AnalysisRequestStatus.State.ACTIVE || status.getState() == AnalysisRequestStatus.State.QUEUED || status.getState() == AnalysisRequestStatus.State.NOT_FOUND));

		log.log(Level.INFO, "Polling result after {0} polls for request id {1}: status: {2}", new Object[] {(MAX_NUMBER_OF_POLLS-maxPolls), id, status.getState()});
		
		Assert.assertTrue(maxPolls > 0);		
		Assert.assertEquals(expectedState, status.getState());
		AnalysisRequestTrackerStore store = new ODFInternalFactory().create(AnalysisRequestTrackerStore.class);
		AnalysisRequestTracker tracker = store.query(id);
		Assert.assertNotNull(tracker);
		checkTracker(tracker, expectedProcessedDiscoveryRequests);
		log.info("Status details: " + status.getDetails());
	}

	static void checkTracker(AnalysisRequestTracker tracker, int expectedProcessedDiscoveryRequests) {
		if (expectedProcessedDiscoveryRequests == -1) {
			expectedProcessedDiscoveryRequests = tracker.getDiscoveryServiceRequests().size(); 
		}
		Assert.assertEquals(expectedProcessedDiscoveryRequests, tracker.getDiscoveryServiceResponses().size());
		
	}

	static String runRequest(String dataSetID, AnalysisManager analysisManager) throws Exception {
		return runRequest(Collections.singletonList(dataSetID), analysisManager);
	}

	public static String runRequest(List<String> dataSetIDs, AnalysisManager analysisManager) throws Exception {
		AnalysisRequest request = createAnalysisRequest(dataSetIDs);
		log.info("Starting analyis");
		AnalysisResponse response = analysisManager.runAnalysis(request);
		Assert.assertNotNull(response);
		Assert.assertFalse(response.isInvalidRequest());
		String id = response.getId();
		Assert.assertNotNull(id);
		return id;
	}

	
	@Test
	public void testSimpleSuccess() throws Exception {
		runRequestAndCheckResult("successID", AnalysisRequestStatus.State.FINISHED, -1);
	}

	public static void waitForRequest(String requestId, AnalysisManager analysisManager) {
		waitForRequest(requestId, analysisManager, MAX_NUMBER_OF_POLLS);
	}
	
	public static void waitForRequest(String requestId, AnalysisManager analysisManager, int maxPolls) {
		AnalysisRequestStatus status = null;

		log.log(Level.INFO, "Waiting for request ''{0}'' to finish", requestId);
		do {
			status = analysisManager.getAnalysisRequestStatus(requestId);
			
			log.log(Level.INFO, "Poll request for request ID ''{0}'', state: ''{1}'', details: ''{2}''", new Object[] { requestId, status.getState(), status.getDetails() });
			maxPolls--;
			try {
				Thread.sleep(WAIT_MS_BETWEEN_POLLING);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		} while (maxPolls > 0 && (status.getState() == AnalysisRequestStatus.State.ACTIVE || status.getState() == AnalysisRequestStatus.State.QUEUED || status.getState() == AnalysisRequestStatus.State.NOT_FOUND));
		if (maxPolls == 0) {
			log.log(Level.INFO, "Request ''{0}'' is not finished yet, don't wait for it", requestId);
		}
		log.log(Level.INFO, "Request ''{0}'' is finished with state: ''{1}''", new Object[] { requestId, status.getState() });
	}

	public static boolean waitForRequest(String requestId, AnalysisManager analysisManager, int maxPolls, AnalysisRequestStatus.State expectedState) {
		AnalysisRequestStatus status = null;

		log.log(Level.INFO, "Waiting for request ''{0}'' to finish", requestId);
		do {
			status = analysisManager.getAnalysisRequestStatus(requestId);
			log.log(Level.INFO, "Poll request for request ID ''{0}'', state: ''{1}'', details: ''{2}''", new Object[] { requestId, status.getState(), status.getDetails() });
			maxPolls--;
			try {
				Thread.sleep(WAIT_MS_BETWEEN_POLLING);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		} while (maxPolls > 0 && (status.getState() == AnalysisRequestStatus.State.ACTIVE || status.getState() == AnalysisRequestStatus.State.QUEUED || status.getState() == AnalysisRequestStatus.State.NOT_FOUND));
		if (maxPolls == 0) {
			log.log(Level.INFO, "Request ''{0}'' is not finished yet, don't wait for it", requestId);
		}
		return expectedState.equals(status.getState());
	}

	
	@Test
	public void testSimpleSuccessDuplicate() throws Exception {
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();
		String id = runRequest("successID", analysisManager);
		String secondId = runRequest("successID", analysisManager);
		Assert.assertNotEquals(id, secondId);
		//Wait limit and try if new analysis is started
		Thread.sleep(DefaultStatusQueueStore.IGNORE_SIMILAR_REQUESTS_TIMESPAN_MS*2 + 5000);
		String thirdId = runRequest("successID", analysisManager);
		Assert.assertNotEquals(secondId, thirdId);
		waitForRequest(id, analysisManager);
		waitForRequest(thirdId, analysisManager);
	}

	@Test
	public void testSimpleSuccessNoDuplicate() throws Exception {
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();
		String id = runRequest("successID", analysisManager);
		String secondId = runRequest("successID2", analysisManager);
		Assert.assertNotEquals(id, secondId);
		waitForRequest(id, analysisManager);
		waitForRequest(secondId, analysisManager);
	}

	@Test
	public void testSimpleSuccessDuplicateSubset() throws Exception {
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();
		String id = runRequest(Arrays.asList("successID", "successID2", "successID3"), analysisManager);
		String secondId = runRequest("successID2", analysisManager);
		Assert.assertNotEquals(id, secondId);
		Thread.sleep(DefaultStatusQueueStore.IGNORE_SIMILAR_REQUESTS_TIMESPAN_MS + 5000);
		String thirdId = runRequest("successID", analysisManager);
		Assert.assertNotEquals(secondId, thirdId);
		waitForRequest(id, analysisManager);
		waitForRequest(thirdId, analysisManager);
	}
	
	/**
	 * This test depends on the speed of execution.
	 * An analysis that is not in state INITIALIZED or IN_SERVICE_QUEUE cannot be cancelled. 
	 * Therefore if the analysis is started too quickly this test will fail!
	 * 
	 * Ignore for now as this can go wrong in the build.
	 */
	@Test
	@Ignore
	public void testCancelRequest() throws Exception {
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();
		String id = runRequest(Arrays.asList("successID", "successID2", "successID3"), analysisManager);
		AnalysisCancelResult cancelAnalysisRequest = analysisManager.cancelAnalysisRequest(id);
		Assert.assertEquals(cancelAnalysisRequest.getState(), AnalysisCancelResult.State.SUCCESS);
		String secondId = runRequest("successID2", analysisManager);
		Assert.assertNotEquals(id, secondId);
	}

	
	@Test
	public void testRequestsWithDataSetListSuccess() throws Exception {
		runRequestAndCheckResult(Arrays.asList("success1", "success2", "success3"), AnalysisRequestStatus.State.FINISHED, 6);
	}
	
	@Test
	public void testRequestsWithDataSetListError() throws Exception {
		runRequestAndCheckResult(Arrays.asList("success1", "error2", "success3"), AnalysisRequestStatus.State.ERROR, 3);
	}

		

	@Test
	public void testSimpleFailure() throws Exception {
		runRequestAndCheckResult("errorID", AnalysisRequestStatus.State.ERROR, 1);
	}
	
	@Test 
	public void testManyRequests()  throws Exception {
		List<String> dataSets = new ArrayList<String>();
		List<AnalysisRequestStatus.State> expectedStates = new ArrayList<AnalysisRequestStatus.State>();
		int dataSetNum = 5;
		for (int i=0; i<dataSetNum; i++) {
			AnalysisRequestStatus.State expectedState = AnalysisRequestStatus.State.FINISHED;
			String dataSet = "successdataSet" + i;
			if (i % 3 == 0) {
				// every third data set should fail
				dataSet = "errorDataSet" + i;
				expectedState = AnalysisRequestStatus.State.ERROR;
			} 
			dataSets.add(dataSet);
			expectedStates.add(expectedState);
		}
		
		runRequests(dataSets, expectedStates);
	}

	public void runRequests(List<String> dataSetIDs, List<AnalysisRequestStatus.State> expectedStates) throws Exception {
		Assert.assertTrue(dataSetIDs.size() == expectedStates.size());
		AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();

		Map<AnalysisRequest, AnalysisRequestStatus.State> request2ExpectedState = new HashMap<AnalysisRequest, AnalysisRequestStatus.State>();

		for (int i = 0; i < dataSetIDs.size(); i++) {
			String dataSetID = dataSetIDs.get(i);
			AnalysisRequestStatus.State expectedState = expectedStates.get(i);

			AnalysisRequest request = createAnalysisRequest(Collections.singletonList(dataSetID));

			log.info("Starting analyis");
			AnalysisResponse response = analysisManager.runAnalysis(request);
			Assert.assertNotNull(response);
			String id = response.getId();
			Assert.assertFalse(response.isInvalidRequest());
			Assert.assertNotNull(id);
			request.setId(id);
			request2ExpectedState.put(request, expectedState);
		}

		//		Set<AnalysisRequest> finishedRequests = new HashSet<AnalysisRequest>();
		Map<AnalysisRequest, AnalysisRequestStatus> actualFinalStatePerRequest = new HashMap<AnalysisRequest, AnalysisRequestStatus>();
		int maxPollPasses = 10;
		for (int i = 0; i < maxPollPasses; i++) {
			log.info("Polling all requests for the " + i + " th time");
			boolean allRequestsFinished = true;
			for (Map.Entry<AnalysisRequest, AnalysisRequestStatus.State> entry : request2ExpectedState.entrySet()) {

				AnalysisRequest request = entry.getKey();
				String id = request.getId();
				if (actualFinalStatePerRequest.containsKey(request)) {
					log.log(Level.INFO, "Request with ID ''{0}'' already finished, skipping it", id);
				} else {
					allRequestsFinished = false;

					AnalysisRequestStatus.State expectedState = entry.getValue();

					AnalysisRequestStatus status = null;

					int maxPollsPerRequest = 3;
					do {
						status = analysisManager.getAnalysisRequestStatus(id);
						log.log(Level.INFO, "Poll request for request ID ''{0}'' (expected state: ''{3}''): state: ''{1}'', details: ''{2}''",
								new Object[] { id, status.getState(), status.getDetails(), expectedState });
						maxPollsPerRequest--;
						Thread.sleep(1000);
					} while (maxPollsPerRequest > 0 && (status.getState() == AnalysisRequestStatus.State.ACTIVE || status.getState() == AnalysisRequestStatus.State.QUEUED || status.getState() == AnalysisRequestStatus.State.NOT_FOUND));

					if (maxPollsPerRequest > 0) {
						// final state found
						actualFinalStatePerRequest.put(request, status);
						//				Assert.assertEquals(expectedState, status.getState());
					}
				}
			}
			if (allRequestsFinished) {
				log.info("All requests finished");
				break;
			}
		}
		Assert.assertTrue(actualFinalStatePerRequest.size() == request2ExpectedState.size());
		Assert.assertTrue(actualFinalStatePerRequest.keySet().equals(request2ExpectedState.keySet()));
		for (Map.Entry<AnalysisRequest, AnalysisRequestStatus> actual : actualFinalStatePerRequest.entrySet()) {
			AnalysisRequest req = actual.getKey();
			Assert.assertNotNull(req);
			AnalysisRequestStatus.State expectedState = request2ExpectedState.get(req);
			Assert.assertNotNull(expectedState);
			AnalysisRequestStatus.State actualState = actual.getValue().getState();
			Assert.assertNotNull(actualState);

			log.log(Level.INFO, "Checking request ID ''{0}'', actual state: ''{1}'', expected state: ''{2}''", new Object[] { req.getId(), actualState, expectedState });
			Assert.assertNotNull(expectedState);
			Assert.assertEquals(expectedState, actualState);
		}
	}

	public static AnalysisRequest createAnalysisRequest(List<String> dataSetIDs) throws JSONException {
		AnalysisRequest request = new AnalysisRequest();
		List<MetaDataObjectReference> dataSetRefs = new ArrayList<>();
		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		if (!(mds instanceof DefaultMetadataStore)) {
			throw new RuntimeException(MessageFormat.format("This tests does not work with metadata store implementation \"{0}\" but only with the DefaultMetadataStore.", mds.getClass().getName()));
		}
		DefaultMetadataStore defaultMds = (DefaultMetadataStore) mds;
		defaultMds.resetAllData();
		for (String id : dataSetIDs) {
			MetaDataObjectReference mdr = new MetaDataObjectReference();
			mdr.setId(id);
			dataSetRefs.add(mdr);
			if (id.startsWith(DUMMY_SUCCESS_ID) || id.startsWith(DUMMY_ERROR_ID)) {
				log.info("Creating dummy data set for reference : " + id.toString());
				DataSet ds = new UnknownDataSet();
				ds.setReference(mdr);
				defaultMds.createObject(ds);
			}
		}
		defaultMds.commit();
		request.setDataSets(dataSetRefs);
		List<String> serviceIds = Arrays.asList(new String[]{"asynctestservice", "synctestservice"});
		/* use a fix list of services 
		List<DiscoveryServiceRegistrationInfo> registeredServices = new ODFFactory().create(ControlCenter.class).getConfig().getRegisteredServices();		
		for(DiscoveryServiceRegistrationInfo service : registeredServices){
			serviceIds.add(service.getId());
		}
		*/
		request.setDiscoveryServiceSequence(serviceIds);
		Map<String, Object> additionalProps = new HashMap<String, Object>();
		additionalProps.put("aaa", "bbb");
		JSONObject jo = new JSONObject();
		jo.put("p1", "v1");
		jo.put("p2", "v2");
		additionalProps.put("jo", jo);
		request.setAdditionalProperties(additionalProps);
		return request;
	}

}
