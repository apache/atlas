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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DataSetCheckResult;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.CachedMetadataStore;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.core.annotation.InternalAnnotationStoreUtils;
import org.apache.atlas.odf.json.JSONUtils;

/**
 * This class processes the entries of a discovery service queue and runs its respective discovery services in a separate thread. 
 * 
 */
public class DiscoveryServiceStarter implements QueueMessageProcessor {

	private Logger logger = Logger.getLogger(DiscoveryServiceStarter.class.getName());

	AnalysisRequestTrackerStore trackerStore = null;
	ControlCenter controlCenter = null;
	Environment environment = null;
	
	/**
	 * parameters must be a three element String[] containing the DiscoveryServiceRequest, the partition number (int) and the offset (long).
	 */
	public DiscoveryServiceStarter() {
		ODFInternalFactory factory = new ODFInternalFactory();
		trackerStore = factory.create(AnalysisRequestTrackerStore.class);
		controlCenter = factory.create(ControlCenter.class);
		environment = factory.create(Environment.class);
	}
	
	private DiscoveryServiceRequest cloneDSRequestAndAddServiceProps(DiscoveryServiceRequest request, boolean requiresMetaDataCache) throws JSONException {
		DiscoveryServiceRequest clonedRequest = JSONUtils.cloneJSONObject(request);
		Map<String, Object> additionalProps = clonedRequest.getAdditionalProperties();
		if (additionalProps == null) {
			additionalProps = new HashMap<>();
			clonedRequest.setAdditionalProperties(additionalProps);
		}
		// add service specific properties
		String id = request.getDiscoveryServiceId();
		Map<String, String> serviceProps = environment.getPropertiesWithPrefix(id);
		additionalProps.putAll(serviceProps);
		
		// add cached metadata objects to request if required
		if (requiresMetaDataCache) {
			MetaDataObject mdo = request.getDataSetContainer().getDataSet();
			MetadataStore mds = new ODFInternalFactory().create(MetadataStore.class);
			clonedRequest.getDataSetContainer().setMetaDataCache(CachedMetadataStore.retrieveMetaDataCache(mds, mdo));
		}

		return clonedRequest;
	}

	
	/**
	 * starts the service taken from the service runtime topic.
	 */
	public void process(ExecutorService executorService, String message, int partition, long offset) {
		AnalysisRequestTracker tracker = null;
		try {
			tracker = JSONUtils.fromJSON(message, AnalysisRequestTracker.class);
			logger.log(Level.FINEST, "DSStarter: received tracker {0}", JSONUtils.lazyJSONSerializer(tracker));
			// load tracker from store and check if it was cancelled in the meantime
			AnalysisRequestTracker storedRequest = trackerStore.query(tracker.getRequest().getId());

			if (storedRequest == null || storedRequest.getStatus() != STATUS.CANCELLED) {
				// set tracker to running
				tracker.setStatus(STATUS.DISCOVERY_SERVICE_RUNNING);
				trackerStore.store(tracker);
				
				DiscoveryServiceRequest nextRequest = TrackerUtil.getCurrentDiscoveryServiceStartRequest(tracker);
				if (nextRequest == null) {
					logger.log(Level.WARNING, "Request in queue has wrong format");
					tracker.setStatus(STATUS.ERROR);
				} else {
					nextRequest.setTakenFromRequestQueue(System.currentTimeMillis());
					trackerStore.store(tracker);
					String dsID = nextRequest.getDiscoveryServiceId();
					SyncDiscoveryService nextService = ControlCenter.getDiscoveryServiceProxy(dsID, tracker.getRequest());
					if (nextService == null) {
						logger.log(Level.WARNING, "Discovery Service ''{0}'' could not be created", dsID);
						throw new DiscoveryServiceUnreachableException("Java proxy for service with id " + dsID + " could not be created");
					} else {
						DataSetContainer ds = nextRequest.getDataSetContainer();
						DataSetCheckResult checkResult = nextService.checkDataSet(ds);
						if (checkResult.getDataAccess() == DataSetCheckResult.DataAccess.NotPossible) {
							String responseDetails = "";
							if (checkResult.getDetails() != null) {
								responseDetails = " Reason: " + checkResult.getDetails();
							}
							if (tracker.getRequest().isIgnoreDataSetCheck()) {
								String msg = MessageFormat.format("Discovery service ''{0}'' cannot process data set ''{1}''.{2} - Ignoring and advancing to next service",
										new Object[]{dsID, ds.getDataSet().getReference(), responseDetails});
								logger.log(Level.INFO, msg);
								// check for next queue
								DiscoveryServiceSyncResponse dummyResponse = new DiscoveryServiceSyncResponse();
								dummyResponse.setCode(DiscoveryServiceResponse.ResponseCode.OK);
								dummyResponse.setDetails(msg);
								TrackerUtil.addDiscoveryServiceStartResponse(tracker, dummyResponse);
								controlCenter.advanceToNextDiscoveryService(tracker);
							} else {
								tracker.setStatus(STATUS.ERROR);
								String msg = MessageFormat.format("Discovery service ''{0}'' cannot process data set ''{1}''.{2}",
										new Object[]{dsID, ds.getDataSet().getReference(), responseDetails});
								tracker.setStatusDetails(msg);
								logger.log(Level.WARNING, msg);
							}
						} else {
							nextService.setExecutorService(executorService);
							runServiceInBackground(executorService, tracker, nextRequest, nextService);
						}
					}
				}
			}
		} catch (DiscoveryServiceUnreachableException exc) {
			logger.log(Level.WARNING, "Discovery service could not be started because it is unreachable", exc);
			if (tracker != null) {
				tracker.setStatus(STATUS.ERROR);
				tracker.setStatusDetails(exc.getReason());
			}
		} catch (Throwable exc) {
			logger.log(Level.WARNING, "An error occurred when starting the discovery service", exc);
			if (tracker != null) {
				tracker.setStatus(STATUS.ERROR);
				tracker.setStatusDetails(Utils.getExceptionAsString(exc));
			}
		}
		updateTracker(tracker);
	}

	
	class ServiceRunner implements ODFRunnable {
		AnalysisRequestTracker tracker;
		DiscoveryServiceRequest nextRequest;
		SyncDiscoveryService nextService;
		
		public ServiceRunner(AnalysisRequestTracker tracker, DiscoveryServiceRequest nextRequest, SyncDiscoveryService nextService) {
			super();
			this.tracker = tracker;
			this.nextRequest = nextRequest;
			this.nextService = nextService;
		}

		@Override
		public void run() {
			try {
				runService(tracker, nextRequest, nextService);
			} catch (Throwable exc) {
				logger.log(Level.WARNING, "An error occurred when running the discovery service", exc);
				if (tracker != null) {
					tracker.setStatus(STATUS.ERROR);
					tracker.setStatusDetails(Utils.getExceptionAsString(exc));
				}
			}
			updateTracker(tracker);
		}
		
		@Override
		public void setExecutorService(ExecutorService service) {
			
		}
		
		@Override
		public boolean isReady() {
			return true;
		}
		
		@Override
		public void cancel() {
		}

	}
	
	
	private void runServiceInBackground(ExecutorService executorService, final AnalysisRequestTracker tracker, final DiscoveryServiceRequest nextRequest, final SyncDiscoveryService nextService) throws JSONException {
		String suffix = nextRequest.getDiscoveryServiceId() + "_" + nextRequest.getOdfRequestId() + UUID.randomUUID().toString();
		String runnerId = "DSRunner_" + suffix;
		ThreadManager tm = new ODFInternalFactory().create(ThreadManager.class);
		ServiceRunner serviceRunner = new ServiceRunner(tracker, nextRequest, nextService);
		tm.setExecutorService(executorService);
		tm.startUnmanagedThread(runnerId, serviceRunner);
	}
	
	private void runService(AnalysisRequestTracker tracker, DiscoveryServiceRequest nextRequest, SyncDiscoveryService nextService) throws JSONException {
		DiscoveryServiceResponse response = null;
		String dsID = nextRequest.getDiscoveryServiceId();
		boolean requiresAuxObjects = controlCenter.requiresMetaDataCache(nextService);
		if (nextService instanceof SyncDiscoveryService) {
			SyncDiscoveryService nextServiceSync = (SyncDiscoveryService) nextService;
			logger.log(Level.FINER, "Starting synchronous analysis on service {0}", dsID);
			DiscoveryServiceSyncResponse syncResponse = nextServiceSync.runAnalysis(cloneDSRequestAndAddServiceProps(nextRequest, requiresAuxObjects));
			nextRequest.setFinishedProcessing(System.currentTimeMillis());
			//Even if the analysis was concurrently cancelled we store the results since the service implementation could do this by itself either way.
			long before = System.currentTimeMillis();
			InternalAnnotationStoreUtils.storeDiscoveryServiceResult(syncResponse.getResult(), tracker.getRequest());
			nextRequest.setTimeSpentStoringResults(System.currentTimeMillis() - before);
			// remove result to reduce size of response
			syncResponse.setResult(null);
			response = syncResponse;
		} else {
			throw new RuntimeException("Unknown Java proxy created for service with id " + dsID);
		}

		// process response
		if (response.getCode() == null) {
			response.setCode(DiscoveryServiceResponse.ResponseCode.UNKNOWN_ERROR);
			String origDetails = response.getDetails();
			response.setDetails(MessageFormat.format("Discovery service did not return a response code. Assuming error. Original message: {0}", origDetails));
		}
		switch (response.getCode()) {
		case UNKNOWN_ERROR:
			TrackerUtil.addDiscoveryServiceStartResponse(tracker, response);
			tracker.setStatus(STATUS.ERROR);
			tracker.setStatusDetails(response.getDetails());
			logger.log(Level.WARNING, "Discovery Service ''{2}'' responded with an unknown error ''{0}'', ''{1}''", new Object[] { response.getCode().name(),
					response.getDetails(), dsID });
			break;
		case NOT_AUTHORIZED:
			TrackerUtil.addDiscoveryServiceStartResponse(tracker, response);
			tracker.setStatus(STATUS.ERROR);
			tracker.setStatusDetails(response.getDetails());
			logger.log(Level.WARNING, "Discovery Service ''{2}'' responded with an unauthorized ''{0}'', ''{1}''", new Object[] { response.getCode().name(),
					response.getDetails(), dsID });
			break;
		case TEMPORARILY_UNAVAILABLE:
			tracker.setStatus(STATUS.IN_DISCOVERY_SERVICE_QUEUE);
			logger.log(Level.INFO, "Discovery Service ''{2}'' responded that it is unavailable right now ''{0}'', ''{1}''", new Object[] {
					response.getCode().name(), response.getDetails(), dsID });
			// reqeue and finish immediately
			controlCenter.getQueueManager().enqueue(tracker);
			return;
		case OK:
			TrackerUtil.addDiscoveryServiceStartResponse(tracker, response);
			logger.log(Level.FINER, "Synchronous Discovery Service processed request ''{0}'', ''{1}''", new Object[] { response.getCode().name(), response.getDetails() });
			AnalysisRequestTracker storedTracker = trackerStore.query(tracker.getRequest().getId());
			//A user could've cancelled the analysis concurrently. In this case, ignore the response and don't overwrite the tracker
			if (storedTracker != null && storedTracker.getStatus() != STATUS.CANCELLED) {
				// check for next queue
				controlCenter.advanceToNextDiscoveryService(tracker);
			} else {
				logger.log(Level.FINER, "Not advancing analysis request because it was cancelled!");
			}
			break;
		default:
			tracker.setStatus(STATUS.ERROR);
			tracker.setStatusDetails(response.getDetails());
			logger.log(Level.WARNING, "Discovery Service ''{2}'' responded with an unknown response ''{0}'', ''{1}''", new Object[] {
					response.getCode().name(), response.getDetails(), dsID });
			break;
		}
	}

	private boolean updateTracker(AnalysisRequestTracker tracker) {
		boolean cancelled = false;
		if (tracker != null) {
			AnalysisRequestTracker storedTracker = trackerStore.query(tracker.getRequest().getId());
			//A user could've cancelled the analysis concurrently. In this case, ignore the response and don't overwrite the tracker
			if (storedTracker == null || (! STATUS.CANCELLED.equals(storedTracker.getStatus())) ) {
				Utils.setCurrentTimeAsLastModified(tracker);
				trackerStore.store(tracker);
			} else {
				cancelled = true;
				logger.log(Level.FINER, "Not storing analysis tracker changes because it was cancelled!");
			}
		}
		return cancelled;
	}
	

}
