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
package org.apache.atlas.odf.core.test.messaging;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.messaging.DiscoveryServiceQueueManager;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.discoveryservice.AnalysisRequestTracker;
import org.apache.atlas.odf.api.engine.MessagingStatus;
import org.apache.atlas.odf.core.controlcenter.AdminMessage;
import org.apache.atlas.odf.core.controlcenter.AdminQueueProcessor;
import org.apache.atlas.odf.core.controlcenter.ConfigChangeQueueProcessor;
import org.apache.atlas.odf.core.controlcenter.DefaultStatusQueueStore;
import org.apache.atlas.odf.core.controlcenter.DiscoveryServiceStarter;
import org.apache.atlas.odf.core.controlcenter.ExecutorServiceFactory;
import org.apache.atlas.odf.core.controlcenter.ODFRunnable;
import org.apache.atlas.odf.core.controlcenter.QueueMessageProcessor;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntime;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntimes;
import org.apache.atlas.odf.core.controlcenter.StatusQueueEntry;
import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.core.controlcenter.ThreadManager.ThreadStartupResult;
import org.apache.atlas.odf.core.controlcenter.TrackerUtil;
import org.apache.atlas.odf.json.JSONUtils;

public class MockQueueManager implements DiscoveryServiceQueueManager {

	static Logger logger = Logger.getLogger(MockQueueManager.class.getName());

	static Object lock = new Object();

	static List<AdminMessage> adminQueue = Collections.synchronizedList(new ArrayList<AdminMessage>());
	static List<StatusQueueEntry> statusQueue = Collections.synchronizedList(new ArrayList<StatusQueueEntry>());
	static Map<String, List<AnalysisRequestTracker>> runtimeQueues = new HashMap<>();

	ThreadManager threadManager;

	public MockQueueManager() {
		ODFInternalFactory factory = new ODFInternalFactory();
		ExecutorServiceFactory esf = factory.create(ExecutorServiceFactory.class);
		threadManager = factory.create(ThreadManager.class);
		threadManager.setExecutorService(esf.createExecutorService());
		//initialize();
	}

	@Override
	public void start() throws TimeoutException {
		logger.info("Initializing MockQueueManager");
		List<ThreadStartupResult> threads = new ArrayList<ThreadStartupResult>();
		ThreadStartupResult startUnmanagedThread = this.threadManager.startUnmanagedThread("MOCKADMIN", createQueueListener("Admin", adminQueue, new AdminQueueProcessor(), false));
		boolean threadCreated = startUnmanagedThread.isNewThreadCreated();
		threads.add(startUnmanagedThread);
		startUnmanagedThread = this.threadManager.startUnmanagedThread("MOCKADMINCONFIGCHANGE",
				createQueueListener("AdminConfig", adminQueue, new ConfigChangeQueueProcessor(), false));
		threadCreated |= startUnmanagedThread.isNewThreadCreated();
		threads.add(startUnmanagedThread);
		startUnmanagedThread = this.threadManager.startUnmanagedThread("MOCKSTATUSSTORE",
				createQueueListener("StatusStore", statusQueue, new DefaultStatusQueueStore.StatusQueueProcessor(), true));
		threadCreated |= startUnmanagedThread
				.isNewThreadCreated();
		threads.add(startUnmanagedThread);

		logger.info("New thread created: " + threadCreated);
		if (threadCreated) {
			try {
				this.threadManager.waitForThreadsToBeReady(5000, threads);
				logger.info("All threads ready");
			} catch (TimeoutException e) {
				final String message = "Not all thrads were created on time";
				logger.warning(message);
			}
		}
	}

	@Override
	public void stop() {
		threadManager.shutdownThreads(Arrays.asList("MOCKADMIN", "MOCKADMINCONFIGCHANGE", "MOCKSTATUSSTORE"));
	}

	<T> T cloneObject(T obj) {
		try {
			return JSONUtils.cloneJSONObject(obj);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void enqueue(AnalysisRequestTracker tracker) {
		tracker = cloneObject(tracker);
		DiscoveryServiceRequest dsRequest = TrackerUtil.getCurrentDiscoveryServiceStartRequest(tracker);
		if (dsRequest == null) {
			throw new RuntimeException("Tracker is finished, should not be enqueued");
		}
		String dsID = dsRequest.getDiscoveryServiceId();
		dsRequest.setPutOnRequestQueue(System.currentTimeMillis());
		synchronized (lock) {
			ServiceRuntime runtime = ServiceRuntimes.getRuntimeForDiscoveryService(dsID);
			if (runtime == null) {
				throw new RuntimeException(MessageFormat.format("Runtime of discovery service ''{0}'' does not exist", dsID));
			}
			String runtimeName = runtime.getName();
			List<AnalysisRequestTracker> mq = runtimeQueues.get(runtimeName);
			if (mq == null) {
				mq = Collections.synchronizedList(new ArrayList<AnalysisRequestTracker>());
				runtimeQueues.put(runtimeName, mq);
			}
			boolean started = this.threadManager.startUnmanagedThread("MOCK" + runtimeName, createQueueListener("Starter" + runtimeName, mq, new DiscoveryServiceStarter(), false))
					.isNewThreadCreated();
			logger.info("New thread created for runtime " + runtimeName + ", started: " + started + ", current queue length: " + mq.size());
			mq.add(tracker);
		}
	}

	static class MockQueueListener implements ODFRunnable {
		String name; 
		QueueMessageProcessor processor;
		List<?> queue;
		boolean cancelled = false;
		ExecutorService service;
		int index = 0;

		public MockQueueListener(String name, List<?> q, QueueMessageProcessor qmp, boolean fromBeginning) {
			this.name = name;
			this.processor = qmp;
			this.queue = q;
			if (fromBeginning) {
				index = 0;
			} else {
				index = q.size();
			}
		}

		long WAITTIMEMS = 100;

		boolean isValidIndex() {
			return index >= 0 && index < queue.size();
		}

		@Override
		public void run() {
			logger.info("MockQueueManager thread " + name + " started");

			while (!cancelled) {
			//	logger.info("Queue consumer " + name + ": checking index " + index + " on queue of size " + queue.size());
				if (!isValidIndex()) {
					try {
						Thread.sleep(WAITTIMEMS);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					Object obj = queue.get(index);
					String msg;
					try {
						msg = JSONUtils.toJSON(obj);
					} catch (JSONException e) {
						e.printStackTrace();
						cancelled = true;
						return;
					}
					this.processor.process(service, msg, 0, index);
					logger.finest("MockQConsumer " + name + ": Processed message: " + msg);
					index++;
				}
			}
			logger.info("MockQueueManager thread finished");

		}


		@Override
		public void setExecutorService(ExecutorService service) {
			this.service = service;
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		@Override
		public boolean isReady() {
			return true;
		}

	}

	ODFRunnable createQueueListener(String name, List<?> queue, QueueMessageProcessor qmp, boolean fromBeginning) {
		return new MockQueueListener(name, queue, qmp, fromBeginning);
	}

	@Override
	public void enqueueInStatusQueue(StatusQueueEntry sqe) {
		sqe = cloneObject(sqe);
		statusQueue.add(sqe);
	}

	@Override
	public void enqueueInAdminQueue(AdminMessage message) {
		message = cloneObject(message);
		adminQueue.add(message);
	}

	public static class MockMessagingStatus extends MessagingStatus {
		String message;

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

	}

	@Override
	public MessagingStatus getMessagingStatus() {
		MockMessagingStatus mms = new MockMessagingStatus();
		mms.setMessage("OK");
		return mms;
	}

}
