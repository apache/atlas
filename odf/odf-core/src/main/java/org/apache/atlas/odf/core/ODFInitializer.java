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

import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.core.controlcenter.AnalysisRequestTrackerStore;
import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.core.messaging.DiscoveryServiceQueueManager;

public class ODFInitializer {

	static Logger logger = Logger.getLogger(ODFInitializer.class.getName());

	static Object initLock = new Object();

	private static boolean running = false;
	private static long lastStopTimestamp = 0;
	private static long lastStartTimestamp = 0;
	private static boolean startStopInProgress = false;
	

	public static long getLastStopTimestamp() {
		synchronized (initLock) {
			return lastStopTimestamp;
		}
	}

	public static long getLastStartTimestamp() {
		synchronized (initLock) {
			return lastStartTimestamp;
		}
	}

	public static boolean isRunning() {
		synchronized (initLock) {
			return running;
		}
	}
	
	public static boolean isStartStopInProgress() {
		return startStopInProgress;
	}

	public static void start() {
		synchronized (initLock) {
			if (!running) {
				startStopInProgress = true;
				DiscoveryServiceQueueManager qm = new ODFInternalFactory().create(DiscoveryServiceQueueManager.class);
				try {
					qm.start();
				} catch (Exception e) {
					logger.log(Level.WARNING, "Timeout occurred while starting ODF", e);
				}
				lastStartTimestamp = System.currentTimeMillis();
				running = true;
				startStopInProgress = false;
			}
		}
	}

	public static void stop() {
		synchronized (initLock) {
			if (running) {
				startStopInProgress = true;
				ODFInternalFactory f = new ODFInternalFactory();
				DiscoveryServiceQueueManager qm = f.create(DiscoveryServiceQueueManager.class);
				try {
					qm.stop();
				} catch (TimeoutException e) {
					logger.log(Level.WARNING, "Timeout occurred while stopping ODF", e);
				}
				ThreadManager tm = f.create(ThreadManager.class);
				tm.shutdownAllUnmanagedThreads();
				AnalysisRequestTrackerStore arts = f.create(AnalysisRequestTrackerStore.class);
				arts.clearCache();
				lastStopTimestamp = System.currentTimeMillis();
				running = false;
				startStopInProgress = false;
			}
		}
	}

}
