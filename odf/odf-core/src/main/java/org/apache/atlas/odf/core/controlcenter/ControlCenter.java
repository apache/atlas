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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.OpenDiscoveryFramework;
import org.apache.atlas.odf.api.analysis.*;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncStartResponse;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.api.metadata.AnnotationPropagator;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.UnknownDataSet;
import org.apache.atlas.odf.core.Encryption;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.messaging.DiscoveryServiceQueueManager;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisCancelResult;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.ServiceNotFoundException;
import org.apache.atlas.odf.api.discoveryservice.async.AsyncDiscoveryService;
import org.apache.atlas.odf.core.Utils;

public class ControlCenter {

	private static final String CLASSNAME = ControlCenter.class.getName();
	private Logger logger = Logger.getLogger(ControlCenter.class.getName());

	public static final String HEALTH_TEST_DISCOVERY_SERVICE_ID = "odf-health-test-discovery-service-id";
	public static final String HEALTH_TEST_DATA_SET_ID_PREFIX = "odf-health-test-dummy-data-set-id";

	DiscoveryServiceQueueManager queueManager = null;
	AnalysisRequestTrackerStore store = null;
	Environment environment = null;
	OpenDiscoveryFramework odf;

	public ControlCenter() {
		ODFInternalFactory f = new ODFInternalFactory();
		queueManager = f.create(DiscoveryServiceQueueManager.class);
		store = f.create(AnalysisRequestTrackerStore.class);
		odf = new ODFFactory().create();
		environment = f.create(Environment.class);
	}

	private String createNewRequestId() {
		return "odf-request-" + UUID.randomUUID().toString() + "_" + System.currentTimeMillis();
	}

	public DiscoveryServiceQueueManager getQueueManager() {
		return queueManager;
	}

	public AnalysisResponse startRequest(AnalysisRequest request) {
		final String METHODNAME = "startRequest()";
		logger.entering(CLASSNAME, METHODNAME);
		AnalysisResponse response = new AnalysisResponse();
		AnalysisRequest requestWithServiceSequence = null;
		try {
			requestWithServiceSequence = JSONUtils.fromJSON(JSONUtils.toJSON(request), AnalysisRequest.class);
		} catch (JSONException e) {
			throw new RuntimeException("Error cloning analysis request.");
		}
		if ((request.getDiscoveryServiceSequence() == null) || request.getDiscoveryServiceSequence().isEmpty()) {
			DeclarativeRequestMapper mapper = new DeclarativeRequestMapper(request);
			List<String> discoveryServiceSequence = mapper.getRecommendedDiscoveryServiceSequence();
			logger.log(Level.INFO, "Using discovery service sequence: " + Utils.joinStrings(discoveryServiceSequence, ','));
			if (discoveryServiceSequence == null) {
				response.setId(request.getId());
				response.setInvalidRequest(true);
				response.setDetails("No suitable discovery services found to create the requested annotation types.");
				return response;
			}
			requestWithServiceSequence.setDiscoveryServiceSequence(discoveryServiceSequence);
		}
		try {
			//Initialize queues to make sure analysis can be started
			queueManager.start();
		} catch (TimeoutException e) {
			logger.warning("queues could not be started in time");
		}
		AnalysisRequestTracker similarTracker = store.findSimilarQueuedRequest(requestWithServiceSequence);
		if (similarTracker != null) {
			logger.log(Level.WARNING, "A similar request for the issued one is already in the queue.");
			logger.log(Level.FINE, "A similar request for the issued one is already in the queue. Original request: {0}, found similar request: {1}",
					new Object[] { JSONUtils.lazyJSONSerializer(requestWithServiceSequence),
					JSONUtils.lazyJSONSerializer(similarTracker) });
		}
		String newRequestId = createNewRequestId();
		response.setId(newRequestId);
		requestWithServiceSequence.setId(newRequestId);
		AnalysisRequestTracker tracker = createTracker(requestWithServiceSequence, response);
		// if request is invalid, response was already modified and null is returned
		if (tracker != null) {
			tracker.setStatus(AnalysisRequestTrackerStatus.STATUS.IN_DISCOVERY_SERVICE_QUEUE);
			logger.log(Level.FINE, "Starting new request with ID ''{0}''. Tracker: {1}", new Object[] { newRequestId, JSONUtils.lazyJSONSerializer(tracker) });
			store.store(tracker);
			logger.log(Level.FINEST, "Stored tracker for new request with ID ''{0}''. Tracker: {1}", new Object[] { newRequestId, JSONUtils.lazyJSONSerializer(tracker) });
			queueManager.enqueue(tracker);
			logger.log(Level.FINEST, "Tracker enqueued for new request with ID ''{0}''. Tracker: {1}", new Object[] { newRequestId, JSONUtils.lazyJSONSerializer(tracker) });
		}
		logger.exiting(CLASSNAME, METHODNAME);
		return response;
	}

	public AnalysisRequestStatus getRequestStatus(String requestId) {
		final String METHODNAME = "getRequestStatus(String)";
		logger.entering(CLASSNAME, METHODNAME);
		AnalysisRequestStatus result = new AnalysisRequestStatus();
		AnalysisRequestTracker tracker = store.query(requestId);
		if (tracker == null) {
			result.setState(AnalysisRequestStatus.State.NOT_FOUND);
		} else {
			AnalysisRequestStatus.State state = null;
			switch (tracker.getStatus()) {
			case INITIALIZED:
			case IN_DISCOVERY_SERVICE_QUEUE:
				state = AnalysisRequestStatus.State.QUEUED;
				break;
			case ERROR:
				state = AnalysisRequestStatus.State.ERROR;
				break;
			case DISCOVERY_SERVICE_RUNNING:
				state = AnalysisRequestStatus.State.ACTIVE;
				break;
			case FINISHED:
				state = AnalysisRequestStatus.State.FINISHED;
				break;
			case CANCELLED:
				state = AnalysisRequestStatus.State.CANCELLED;
			default:
				;
			}
			result.setState(state);
			result.setDetails(tracker.getStatusDetails());
			result.setRequest(tracker.getRequest());

			long totalProcessingTime = 0;
			long totalQueuingTime = 0;
			long totalTimeSpentStoringAnnotations = 0;

			List<DiscoveryServiceRequest> requests = new ArrayList<DiscoveryServiceRequest>();
			for (DiscoveryServiceRequest req : tracker.getDiscoveryServiceRequests()) {
				DiscoveryServiceRequest copyReq = new DiscoveryServiceRequest();
				copyReq.setDiscoveryServiceId(req.getDiscoveryServiceId());
				long putOnQueue = req.getPutOnRequestQueue();
				long startedProcessing = req.getTakenFromRequestQueue();
				long finishedProcessing = req.getFinishedProcessing();

				totalProcessingTime += (finishedProcessing > 0 ? finishedProcessing - startedProcessing : finishedProcessing);
				totalQueuingTime += (startedProcessing > 0 ? startedProcessing - putOnQueue : startedProcessing);
				totalTimeSpentStoringAnnotations += req.getTimeSpentStoringResults();

				copyReq.setFinishedProcessing(finishedProcessing);
				copyReq.setPutOnRequestQueue(putOnQueue);
				copyReq.setTakenFromRequestQueue(startedProcessing);
				requests.add(copyReq);
			}

			result.setTotalTimeOnQueues(totalQueuingTime);
			result.setTotalTimeProcessing(totalProcessingTime);
			result.setTotalTimeStoringAnnotations(totalTimeSpentStoringAnnotations);
			result.setServiceRequests(requests);
		}
		logger.log(Level.FINE, "Returning request status object {0}", JSONUtils.lazyJSONSerializer(result));
		logger.exiting(CLASSNAME, METHODNAME);
		return result;
	}

	public AnalysisCancelResult cancelRequest(String requestId) {
		final String METHODNAME = "cancelRequest(String)";
		logger.entering(CLASSNAME, METHODNAME);

		AnalysisCancelResult result = new AnalysisCancelResult();
		result.setState(AnalysisCancelResult.State.NOT_FOUND);

		AnalysisRequestTracker request = store.query(requestId);
		//TODO implement cancellation of running instead of only queued requests.
		if (request != null) {
			if (TrackerUtil.isCancellable(request)) {
				request.setStatus(AnalysisRequestTrackerStatus.STATUS.CANCELLED);
				store.store(request);
				logger.info("cancelled request with id " + requestId);
				result.setState(AnalysisCancelResult.State.SUCCESS);
			} else {
				logger.log(Level.FINER, "Request ''{0}'' could not be cancelled. State ''{1}'', next request number:. ''{2}''", new Object[]{requestId, request.getStatus(), request.getNextDiscoveryServiceRequest()});
				result.setState(AnalysisCancelResult.State.INVALID_STATE);
			}
		}
		logger.exiting(CLASSNAME, METHODNAME);
		return result;
	}

	private AnalysisRequestTracker createTracker(AnalysisRequest request, AnalysisResponse response) {
		DiscoveryServiceManager discoveryServiceManager = odf.getDiscoveryServiceManager();
		List<DiscoveryServiceProperties> registeredServices = new ArrayList<>(discoveryServiceManager.getDiscoveryServicesProperties());
		registeredServices.add(HealthCheckServiceRuntime.getHealthCheckServiceProperties());
		String currentUser = this.environment.getCurrentUser();

		/*
		List<MetaDataObjectReference> datasets = request.getDataSets();
		
		if (datasets.size() == 1 && datasets.get(0).getId().startsWith(HEALTH_TEST_DATA_SET_ID_PREFIX)) {
			// health test mode
			AnalysisRequestTracker healthTestTracker = new AnalysisRequestTracker();
			DiscoveryServiceRequest dssr = new DiscoveryServiceRequest();
			dssr.setOdfRequestId(request.getId());
			dssr.setDiscoveryServiceId(ControlCenter.HEALTH_TEST_DISCOVERY_SERVICE_ID);
			String odfUrl = new ODFFactory().create().getSettingsManager().getODFSettings().getOdfUrl();
			dssr.setOdfUrl(odfUrl);
			MetaDataObjectReference dsr = datasets.get(0);
			
			DataSetContainer dataSetContainer = new DataSetContainer();
			DataSet oMDataSet = new UnknownDataSet();	
			oMDataSet.setReference(dsr);
			dataSetContainer.setDataSet(oMDataSet);
			
			dssr.setDataSetContainer(dataSetContainer);
			dssr.setUser(currentUser);
			dssr.setAdditionalProperties(request.getAdditionalProperties());
			healthTestTracker.setDiscoveryServiceRequests(Collections.singletonList(dssr));
			healthTestTracker.setRequest(request);
			healthTestTracker.setStatus(STATUS.INITIALIZED);
			Utils.setCurrentTimeAsLastModified(healthTestTracker);
			healthTestTracker.setUser(currentUser);
			response.setDetails("Request is a special health test request.");
			return healthTestTracker;
		}
		*/

		List<DiscoveryServiceRequest> startRequests = new ArrayList<DiscoveryServiceRequest>();
		List<String> discoveryServiceSequence = request.getDiscoveryServiceSequence();
		if (discoveryServiceSequence != null && !discoveryServiceSequence.isEmpty()) {
			logger.log(Level.FINE, "Request issued with fixed discovery service sequence: {0}", discoveryServiceSequence);
			// first check if discoveryService IDs are valid
			Set<String> foundDSs = new HashSet<String>(discoveryServiceSequence);
			for (String ds : discoveryServiceSequence) {
				for (DiscoveryServiceProperties regInfo : registeredServices) {
					if (regInfo.getId().equals(ds)) {
						foundDSs.remove(ds);
					}
				}
			}
			// if there are some IDs left that were not found 
			if (!foundDSs.isEmpty()) {
				String msg = MessageFormat.format("The discovery services {0} could not be found", Utils.collectionToString(foundDSs, ","));
				logger.log(Level.WARNING, msg);
				response.setInvalidRequest(true);
				response.setDetails(msg);
				return null;
			}

			// for each data set process all discovery services
			// (possible alternative, not used here: for all discovery services process each data set)
			for (MetaDataObjectReference dataSetId : request.getDataSets()) {
				MetaDataObject mdo = null;
				if (dataSetId.getId().startsWith(HEALTH_TEST_DATA_SET_ID_PREFIX)) {
					mdo = new UnknownDataSet();
					mdo.setReference(dataSetId);
				} else {
					mdo = odf.getMetadataStore().retrieve(dataSetId);
				}
				if (mdo == null) {
					String msg = MessageFormat.format("The meta data object id ''{0}'' does not reference an existing metadata object. Request will be set to error.", dataSetId.toString());
					logger.log(Level.WARNING, msg);
					response.setInvalidRequest(true);
					response.setDetails(msg);
					return null;
				}
				if (dataSetId.getUrl() == null) {
					dataSetId.setUrl(mdo.getReference().getUrl());
				}
				for (String ds : discoveryServiceSequence) {
					DiscoveryServiceRequest req = new DiscoveryServiceRequest();
					DataSetContainer dataSetContainer = new DataSetContainer();
					dataSetContainer.setDataSet(mdo);
					req.setDataSetContainer(dataSetContainer);
					req.setOdfRequestId(request.getId());
					req.setDiscoveryServiceId(ds);
					req.setUser(currentUser);
					req.setAdditionalProperties(request.getAdditionalProperties());
					String odfUrl = odf.getSettingsManager().getODFSettings().getOdfUrl();
					req.setOdfUrl(odfUrl);
					for (DiscoveryServiceProperties dsri : odf.getDiscoveryServiceManager().getDiscoveryServicesProperties()) {
						if (dsri.getId().equals(ds)) {
							if (dsri.getEndpoint().getRuntimeName().equals(SparkServiceRuntime.SPARK_RUNTIME_NAME)) {
								req.setOdfUser(odf.getSettingsManager().getODFSettings().getOdfUser());
								//Note that the password has to be provided as plain text here because the remote service cannot decrypt it otherwise.
								//TODO: Consider to provide a temporary secure token instead of the password.
								req.setOdfPassword(Encryption.decryptText(odf.getSettingsManager().getODFSettings().getOdfPassword()));
							}
						}
					}
					startRequests.add(req);
				}
			}
		} else {
			String msg = "The request didn't contain any processing hints. ODF cannot process a request without an analysis sequence.";
			logger.log(Level.WARNING, msg);
			response.setInvalidRequest(true);
			response.setDetails(msg);
			return null;
		}

		AnalysisRequestTracker tracker = new AnalysisRequestTracker();
		tracker.setDiscoveryServiceRequests(startRequests);
		tracker.setNextDiscoveryServiceRequest(0);
		tracker.setRequest(request);
		tracker.setStatus(AnalysisRequestTrackerStatus.STATUS.INITIALIZED);
		Utils.setCurrentTimeAsLastModified(tracker);
		tracker.setUser(currentUser);
		return tracker;
	}
	
	boolean requiresMetaDataCache(DiscoveryService service) {
		return service instanceof SparkDiscoveryServiceProxy;
	}

	public static SyncDiscoveryService getDiscoveryServiceProxy(String discoveryServiceId, AnalysisRequest request) {
		try {
			ODFInternalFactory factory = new ODFInternalFactory();
			DiscoveryServiceManager dsm = factory.create(DiscoveryServiceManager.class);
			DiscoveryServiceProperties serviceProps = null;
			if (discoveryServiceId.startsWith(HEALTH_TEST_DISCOVERY_SERVICE_ID)) {
				serviceProps = HealthCheckServiceRuntime.getHealthCheckServiceProperties();
			} else {
				serviceProps = dsm.getDiscoveryServiceProperties(discoveryServiceId);
			}
			ServiceRuntime runtime = ServiceRuntimes.getRuntimeForDiscoveryService(discoveryServiceId);
			if (runtime == null) {
				throw new RuntimeException(MessageFormat.format("Service runtime for service ''{0}'' was not found.", discoveryServiceId));
			}
			DiscoveryService runtimeProxy = runtime.createDiscoveryServiceProxy(serviceProps);
			SyncDiscoveryService proxy = null;
			if (runtimeProxy instanceof AsyncDiscoveryService) {
				proxy = new AsyncDiscoveryServiceWrapper( (AsyncDiscoveryService) runtimeProxy);
			} else {
				proxy = (SyncDiscoveryService) runtimeProxy;
			}
			proxy.setMetadataStore(factory.create(MetadataStore.class));
			AnnotationStore as = factory.create(AnnotationStore.class);
			if (request != null) {
				as.setAnalysisRun(request.getId());
			}
			proxy.setAnnotationStore(as);
			return proxy;
		} catch (ServiceNotFoundException exc) {
			throw new RuntimeException(exc);
		}
	}

	/**
	 * package private helper method that can be called when the current discovery service was finished
	 * and you want to advance to the next.
	 * NOTE: This should only be called once for all nodes, i.e., typically from a Kafka consumer
	 *       that has runs on all nodes with the same consumer group ID.
	 * 
	 * @param dsRunID runID is just used for logging, could be any value
	 * @param dsID
	 */
	void advanceToNextDiscoveryService(final AnalysisRequestTracker tracker) {
		DiscoveryServiceRequest req = TrackerUtil.getCurrentDiscoveryServiceStartRequest(tracker);
		DiscoveryServiceResponse resp = TrackerUtil.getCurrentDiscoveryServiceStartResponse(tracker);
		String dsRunID = "N/A";
		if (resp instanceof DiscoveryServiceAsyncStartResponse) {
			dsRunID = ((DiscoveryServiceAsyncStartResponse) resp).getRunId();
		}
		String dsID = req.getDiscoveryServiceId();

		TrackerUtil.moveToNextDiscoveryService(tracker);
		DiscoveryServiceRequest nextDSReq = TrackerUtil.getCurrentDiscoveryServiceStartRequest(tracker);
		if (nextDSReq == null) {
			logger.log(Level.FINER, "DSWatcher: Run ''{0}'' of DS ''{1}'' was last of request ''{2}'', marking overall request as finished",
					new Object[] { dsRunID, dsID, tracker.getRequest().getId() });
			// overall request is finished
			tracker.setStatus(AnalysisRequestTrackerStatus.STATUS.FINISHED);
			tracker.setStatusDetails("All discovery services ran successfully");
			
			// now propagate annotations if configured
			logger.log(Level.FINE, "Request is finished, checking for annotation propagation");
			Boolean doPropagation = odf.getSettingsManager().getODFSettings().getEnableAnnotationPropagation();
			if (Boolean.TRUE.equals(doPropagation)) {
				TransactionContextExecutor transactionContextExecutor = new ODFInternalFactory().create(TransactionContextExecutor.class);
				try {
					transactionContextExecutor.runInTransactionContext(new Callable<Object>() {
						
						@Override
						public Object call() throws Exception {
							AnnotationPropagator ap = odf.getMetadataStore().getAnnotationPropagator();
							if (ap != null) {
								logger.log(Level.FINE, "Annotation Propagator exists, running propagation");
								try {
									ap.propagateAnnotations(new ODFFactory().create().getAnnotationStore(), tracker.getRequest().getId());
								} catch(Exception exc) {
									logger.log(Level.SEVERE, "An unexcepted exception occurred while propagating annotations", exc);
									tracker.setStatus(AnalysisRequestTrackerStatus.STATUS.ERROR);
									String msg = MessageFormat.format("An unexpected exception occured while propagating annotations: ''{0}''", Utils.getExceptionAsString(exc));
									tracker.setStatusDetails(msg);
								}
							}
							return null;
						}
					});
				} catch (Exception e) {
					// should never happen as exception is handled inside the callable
					throw new RuntimeException(e);
				}
			}
		} else {
			logger.log(Level.FINER, "DSWatcher: Run ''{0}'' of DS ''{1}'' was not the last of request ''{2}'', moving over to next request",
					new Object[] { dsRunID, dsID, tracker.getRequest().getId() });
			tracker.setStatus(AnalysisRequestTrackerStatus.STATUS.IN_DISCOVERY_SERVICE_QUEUE);
			queueManager.enqueue(tracker);
		}
		Utils.setCurrentTimeAsLastModified(tracker);
		store.store(tracker);
	}

}
