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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.messaging.DiscoveryServiceQueueManager;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisRequestSummary;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackerStatus.STATUS;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.annotation.AnnotationStoreUtils;
import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;

/**
 * This class is an in-memory store for both request trackers (showing the status of analysis requests) as well as
 * a for annotations. Both trackers and annotations are put on the ODF status queue which 
 * (a) stores as "semi"-persistent store ("semi" because Kafka's retention mechanism will evantuall delete them), and
 * (b) a way to propagate those changes to other ODF nodes.
 * The annotations and trackers themselves are stored in memory in static variables.
 * 
 * This is how it works:
 * 1. A single consumer thread listens on the status topic
 * 2. If an incoming status queue entry is a tracker, it stores it in the in-memory tracker store
 *    If it is an annotation, it stores it in the in-memory annotation store
 * 3. Queries for trackers and annotations only go against the in-memory stores
 * 4. When a check for overaged entries occurs (a check that removes trackers form the store which are older than the queue retention time)
 *    the annotations for overaged and finished requests are also deleted (see removeOveragedEntries())
 *   
 *    
 *
 */
public class DefaultStatusQueueStore implements AnalysisRequestTrackerStore, AnnotationStore {

	static Logger logger = Logger.getLogger(DefaultStatusQueueStore.class.getName());
	
	public static final long IGNORE_SIMILAR_REQUESTS_TIMESPAN_MS = 5000;
	
	static Object globalRequestStoreMapLock = new Object();
	
	/*
	 * http://docs.oracle.com/javase/7/docs/api/java/util/LinkedHashMap.html
	 * 
	 * A structural modification is any operation that adds or deletes one or more mappings or, in the case of access-ordered linked hash maps, affects iteration order. 
	 * In insertion-ordered linked hash maps, merely changing the value associated with a key that is already contained in the map is not a structural modification. 
	 * In access-ordered linked hash maps, merely querying the map with get is a structural modification.) 
	 */
	static LinkedHashMap<String, AnalysisRequestTracker> globalRequestStoreMap = new LinkedHashMap<String, AnalysisRequestTracker>();
	
	/*
	 * This map is only used to track if storing an object was successful
	 *  
	 */
	static ConcurrentHashMap<String, Boolean> globalStoreSuccessMap = new ConcurrentHashMap<String, Boolean>();
		
	private String analysisRun;
	
	// simplest implementation for now: just keep a simple list
	private static List<Annotation> storedAnnotations = new LinkedList<>();
	private static Object storedAnnotationsLock = new Object();

	/**
	 * This processor reads trackers from the queue and stores it in the globalRequestStoreMap.
	 * The thread for this processor is created in the QueueManager implementation.
	 *
	 */
	public static class StatusQueueProcessor implements QueueMessageProcessor {
		Logger logger = Logger.getLogger(StatusQueueProcessor.class.getName());

		@Override
		public void process(ExecutorService executorService, String message, int partition, long offset) {
			StatusQueueEntry sqe = new StatusQueueEntry();
			try {
				sqe = JSONUtils.fromJSON(message, StatusQueueEntry.class);
			} catch (Exception e) {
				logger.log(Level.WARNING, "Entry in status queue could not be processed", e);
			}
			
			// first handle trackers and / or initial cleanup
			synchronized (globalRequestStoreMapLock) {
				if (sqe.getAnalysisRequestTracker() != null) {
					try {
						AnalysisRequestTracker tracker = sqe.getAnalysisRequestTracker();
						String requestID = tracker.getRequest().getId();
						logger.log(Level.FINEST, "Store status queue: found tracker with id ''{0}'', tracker: {1}", new Object[] { requestID, message });
						if (tracker.getStatus() == STATUS.FINISHED) {
							logger.log(Level.INFO, "Request with id ''{0}'' is finished, result: {1}", new Object[] { requestID, message });
						}
						//remove item so that it is added to the end of the list.
						if (globalRequestStoreMap.containsKey(requestID)) {
							globalRequestStoreMap.remove(requestID);
						}

						globalRequestStoreMap.put(requestID, tracker);
						if (tracker != null && tracker.getRevisionId() != null) {
							globalStoreSuccessMap.put(tracker.getRevisionId(), true);
						}

					} catch (Exception e) {
						logger.log(Level.WARNING, "Tracker entry in status queue could not be processed", e);
					}
				} 				
			}
			
			if (sqe.getAnnotation() != null) {
				Annotation annot = sqe.getAnnotation();
				logger.log(Level.FINEST, "Received annotationk over status queue: ''{0}''", annot.getReference().getId());
				synchronized (storedAnnotationsLock) {
					storedAnnotations.add(annot);
					globalStoreSuccessMap.put(annot.getReference().getId(), true);
				}
			}

			removeOveragedEntries();
		}

	}

	/////////////////////////////////////////////
	// AnalysisRequestTrackerStore interface implementation

	
	/*
	 * This store uses the lastModified timestamp to remove overaged trackers. 
	 * Therefore, the lastModified timestamp MUST be set before storing anything and prevent unwanted removal
	 */
	@Override
	public void store(AnalysisRequestTracker tracker) {
		String id = tracker.getRequest().getId();
		logger.fine("Store " + id + " in trackerStore");

		String revId = UUID.randomUUID() + "_" + System.currentTimeMillis();
		tracker.setRevisionId(revId);
		globalStoreSuccessMap.put(revId, false);
		
		ODFInternalFactory factory = new ODFInternalFactory();
		DiscoveryServiceQueueManager qm = factory.create(DiscoveryServiceQueueManager.class);
		// put the tracker onto the status queue, the actual map that is used in query() is filled by the ARTProcessor listening on the status queue
		StatusQueueEntry sqe = new StatusQueueEntry();
		sqe.setAnalysisRequestTracker(tracker);
		qm.enqueueInStatusQueue(sqe);
		waitUntilEntryArrives(revId);
	}

	private void waitUntilEntryArrives(String entryId) {
		boolean found = false;
		int maxNumWaits = 1500;
		int sleepMS = 20;
		while (maxNumWaits > 0) {
			final Boolean storageSuccess = globalStoreSuccessMap.get(entryId);
			if (storageSuccess != null && storageSuccess == true) {
				found = true;
				globalStoreSuccessMap.remove(entryId);
				break;
			}
			try {
				Thread.sleep(sleepMS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			maxNumWaits--;
		}
		if(!found){
			final String message = "The tracker could not be stored in 30 sec!";
			logger.warning(message);
			throw new RuntimeException(message);
		}else{
			logger.fine("Tracker stored after " + ((1500 - maxNumWaits) * sleepMS) + " ms");
		}
	}

	@Override
	public AnalysisRequestTracker query(String analysisRequestId) {
		logger.fine("Querying store for " + analysisRequestId);
		synchronized (globalRequestStoreMapLock) {
			AnalysisRequestTracker tracker = globalRequestStoreMap.get(analysisRequestId);
			return tracker;
		}
	}
	
	@Override
	public void clearCache() {
		logger.fine("Clearing store cache");
		synchronized (globalRequestStoreMapLock) {
			globalRequestStoreMap.clear();
		}
	}
	
	private static void removeOveragedEntries(){
		Set<String> finishedRequests = new HashSet<>();
		logger.fine("Removing overaged entries from store");
		synchronized (globalRequestStoreMapLock) {
			Iterator<Entry<String, AnalysisRequestTracker>> entryIterator = globalRequestStoreMap.entrySet().iterator();
			long maxRetentionMS = new ODFFactory().create().getSettingsManager().getODFSettings().getMessagingConfiguration().getAnalysisRequestRetentionMs();
			long currentTimeMS = System.currentTimeMillis();
			while(entryIterator.hasNext()){
				Entry<String, AnalysisRequestTracker> entry = entryIterator.next();
				AnalysisRequestTracker tracker = entry.getValue();
				if(currentTimeMS - tracker.getLastModified() >= maxRetentionMS){
					if (tracker.getStatus() == STATUS.FINISHED || tracker.getStatus() == STATUS.ERROR) {
						finishedRequests.add(tracker.getRequest().getId());
					}
					entryIterator.remove();
					logger.log(Level.INFO, "Removed overaged status tracker with id ''{0}''", new Object[] { entry.getKey() });
				}else{
					/*
					 * items in a linkedHashMap are ordered in the way they were put into the map.
					 * Because of this, if one item is not overaged, all following won't be either
					*/
					break;
				}
			}
		}
		synchronized (storedAnnotationsLock) {
			ListIterator<Annotation> it = storedAnnotations.listIterator();
			while (it.hasNext()) {
				Annotation annot = it.next();
				if (finishedRequests.contains(annot.getAnalysisRun())) {
					it.remove();
				}
			}
		}
	}

	@Override
	public int getSize() {
		synchronized (globalRequestStoreMapLock) {
			return globalRequestStoreMap.keySet().size();
		}
	}
	
	@Override
	public AnalysisRequestTracker findSimilarQueuedRequest(AnalysisRequest request) {
		synchronized (globalRequestStoreMapLock) {
			for (AnalysisRequestTracker tracker : globalRequestStoreMap.values()) {
				long startedAfterLimit = System.currentTimeMillis() - IGNORE_SIMILAR_REQUESTS_TIMESPAN_MS;
				if (TrackerUtil.isAnalysisWaiting(tracker) || 
						(tracker.getNextDiscoveryServiceRequest() == 0 && tracker.getStatus() == STATUS.DISCOVERY_SERVICE_RUNNING && tracker.getLastModified() >= startedAfterLimit)) {
					AnalysisRequest otherRequest = tracker.getRequest();
					List<MetaDataObjectReference> dataSets = request.getDataSets();
					List<MetaDataObjectReference> otherDataSets = otherRequest.getDataSets();
					
					if (otherDataSets.containsAll(dataSets) && tracker.getDiscoveryServiceRequests().get(0).getDiscoveryServiceId().equals(
							request.getDiscoveryServiceSequence().get(0))) {
						logger.log(Level.FINEST, "Found similar request for request {0}", new Object[] { request.getId()});
						return tracker;
					}
				}
			}
			return null;
		}
	}

	
	@Override
	public List<AnalysisRequestTracker> getRecentTrackers(int offset, int limit) {
		if (offset < 0) {
			throw new RuntimeException("Offset parameter cannot be negative.");
		}
		if (limit < -1) {
			throw new RuntimeException("Limit parameter cannot be smaller than -1.");
		}
		synchronized (globalRequestStoreMapLock) {
			List<AnalysisRequestTracker> arsList = new ArrayList<>();
			Iterator<Map.Entry<String, AnalysisRequestTracker>> it = globalRequestStoreMap.entrySet().iterator();
			// filter out health check requests
			while (it.hasNext()) {
				AnalysisRequestTracker t = it.next().getValue();
				if (!t.getRequest().getDataSets().get(0).getId().startsWith(ControlCenter.HEALTH_TEST_DATA_SET_ID_PREFIX)) {
					arsList.add(t);
				}
			}
			// now pick number many requests from the end
			List<AnalysisRequestTracker> result = new ArrayList<>();
			if (arsList.size() > offset) {
				int startIndex = arsList.size() - offset - limit;
				if (limit == -1 || startIndex < 0) {
					startIndex = 0;
				}
				int endIndex = arsList.size() - offset - 1;
				if (endIndex < 0) {
					endIndex = 0;
				}
				for (int i=endIndex ; i>=startIndex; i--) {
					result.add(arsList.get(i));
				}
			}
			return result;
		}
	}
	
	@Override
	public AnalysisRequestSummary getRequestSummary() {
		synchronized (globalRequestStoreMapLock) {
			try {
				List<AnalysisRequestTracker> recentTrackers = this.getRecentTrackers(0, -1);
				int totalSuccess = 0;
				int totalFailure = 0;
	
				for (AnalysisRequestTracker tracker : recentTrackers) {
					if (STATUS.FINISHED.equals(tracker.getStatus())) {
						totalSuccess++;
					} else if (STATUS.ERROR.equals(tracker.getStatus())) {
						totalFailure++;
					}
				}
				return new AnalysisRequestSummary(totalSuccess, totalFailure);
			} catch (Exception exc) {
				throw new RuntimeException(exc);
			}
		}	
	}

	/////////////////////////////////////////////
	// AnnotationStore interface implementation
	
	@Override
	public Properties getProperties() {
		Properties props = new Properties();
		props.put(STORE_PROPERTY_TYPE, "DefaultAnnotationStore");
		props.put(STORE_PROPERTY_ID, getRepositoryId());
		props.put(STORE_PROPERTY_DESCRIPTION, "A default in-memory implementation of the annotation store storing its results via Kafka");
		return props;
	}

	@Override
	public String getRepositoryId() {
		return "ODFDefaultAnnotationStore";
	}

	@Override
	public ConnectionStatus testConnection() {
		return ConnectionStatus.OK;
	}

	@Override
	public MetaDataObjectReference store(Annotation annotation) {
		// clone object
		try {
			annotation = JSONUtils.cloneJSONObject(annotation);
		} catch (JSONException e) {
			logger.log(Level.SEVERE, "Annotation could not be stored because JSON conversion failed.", e);
			throw new RuntimeException(e);
		}
		
		// create a new reference
		String annotId = "Annot" + UUID.randomUUID() + "_" + System.currentTimeMillis();
		logger.log(Level.FINEST, "Storing annotation with ID ''{0}''", annotId);
		MetaDataObjectReference ref = new MetaDataObjectReference();
		ref.setId(annotId);
		ref.setRepositoryId(getRepositoryId());
		annotation.setReference(ref);
		if (analysisRun != null) {
			annotation.setAnalysisRun(analysisRun);
		}
		
		// re-use mechanism from status queue to wait until message has arrived via Kafka
		globalStoreSuccessMap.put(annotId, false);
		DiscoveryServiceQueueManager qm = new ODFInternalFactory().create(DiscoveryServiceQueueManager.class);
		StatusQueueEntry sqe = new StatusQueueEntry();
		sqe.setAnnotation(annotation);
		qm.enqueueInStatusQueue(sqe);
		waitUntilEntryArrives(annotId);
		return ref;
	}

	@Override
	public List<Annotation> getAnnotations(MetaDataObjectReference object, String analysisRequestId) {
		List<Annotation> results = new ArrayList<>();
		synchronized (storedAnnotationsLock) {
			logger.log(Level.FINEST, "Number of annotations stored: ''{0}''", storedAnnotations.size());
			ListIterator<Annotation> it = storedAnnotations.listIterator();
			while (it.hasNext()) {
				Annotation annot = it.next();
				boolean match = true;
				if (object != null) {
					match = match && object.equals(AnnotationStoreUtils.getAnnotatedObject(annot));
				}
				if (annot.getAnalysisRun() != null) {
					// analysisRun is not set for health check and for some of the tests
					if (analysisRequestId != null) {
						match &= annot.getAnalysisRun().equals(analysisRequestId);
					}
				}
				if (match) {
					results.add(annot);
				}
			}
		}
		logger.log(Level.FINEST, "Number of annotations found for request Id ''{0}'': ''{1}''", new Object[]{analysisRequestId, results.size()});
		return results;
	}

	@Override
	public void setAnalysisRun(String analysisRun) {
		this.analysisRun = analysisRun;
	}

	@Override
	public String getAnalysisRun() {
		return this.analysisRun;
	}

	@Override
	public Annotation retrieveAnnotation(MetaDataObjectReference ref) {
		synchronized (storedAnnotationsLock) {
			logger.log(Level.FINEST, "Number of annotations stored: ''{0}''", storedAnnotations.size());
			ListIterator<Annotation> it = storedAnnotations.listIterator();
			while (it.hasNext()) {
				Annotation annot = it.next();
				if (annot.getReference().equals(ref)) {
					return annot;
				}
			}
		}
		return null;
	}

	@Override
	public void setStatusOfOldRequest(long cutOffTimestamp, STATUS status, String detailsMessage) {
		synchronized (globalRequestStoreMapLock) {
			DiscoveryServiceQueueManager qm = new ODFInternalFactory().create(DiscoveryServiceQueueManager.class);
			for (AnalysisRequestTracker tracker : globalRequestStoreMap.values()) {
				if (tracker.getLastModified() < cutOffTimestamp //
						&& (STATUS.DISCOVERY_SERVICE_RUNNING.equals(tracker.getStatus()) //
								|| STATUS.IN_DISCOVERY_SERVICE_QUEUE.equals(tracker.getStatus()) //
								|| STATUS.INITIALIZED.equals(tracker.getStatus()) //
						)) {
					// set the tracker in-memory to have the result available immediately
					tracker.setStatus(status);
					if (detailsMessage == null) {
						detailsMessage = "Setting request to " + status + " because it was last modified before " + new Date(cutOffTimestamp);
					}
					tracker.setStatusDetails(detailsMessage);
					// put tracker onto queue
					StatusQueueEntry sqe = new StatusQueueEntry();
					sqe.setAnalysisRequestTracker(tracker);
					qm.enqueueInStatusQueue(sqe);
				}
			}
		}
		
	}
}
