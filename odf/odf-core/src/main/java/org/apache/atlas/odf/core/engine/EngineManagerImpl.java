/**
 *
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
package org.apache.atlas.odf.core.engine;

import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.engine.EngineManager;
import org.apache.atlas.odf.api.engine.MessagingStatus;
import org.apache.atlas.odf.api.engine.ODFEngineOptions;
import org.apache.atlas.odf.api.engine.ODFStatus;
import org.apache.atlas.odf.api.engine.ODFVersion;
import org.apache.atlas.odf.api.engine.ServiceRuntimesInfo;
import org.apache.atlas.odf.api.engine.SystemHealth;
import org.apache.atlas.odf.api.engine.ThreadStatus;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.core.ODFInitializer;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.ODFUtils;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.core.controlcenter.AdminMessage;
import org.apache.atlas.odf.core.controlcenter.AdminMessage.Type;
import org.apache.atlas.odf.core.controlcenter.ControlCenter;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntimes;
import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.core.messaging.DiscoveryServiceQueueManager;

/**
*
* External Java API for managing and controlling the ODF engine
*
*/
public class EngineManagerImpl implements EngineManager {

	private Logger logger = Logger.getLogger(EngineManagerImpl.class.getName());

	public EngineManagerImpl() {
	}

	/**
	 * Checks the health status of ODF
	 *
	 * @return Health status of the ODF engine
	 */
	public SystemHealth checkHealthStatus() {
		SystemHealth health = new SystemHealth();
		try {
			AnalysisRequest dummyRequest = new AnalysisRequest();
			String dataSetID = ControlCenter.HEALTH_TEST_DATA_SET_ID_PREFIX + UUID.randomUUID().toString();
			MetaDataObjectReference dataSetRef = new MetaDataObjectReference();
			dataSetRef.setId(dataSetID);
			dummyRequest.setDataSets(Collections.singletonList(dataSetRef));
			List<String> discoveryServiceSequence = new ArrayList<String>();
			discoveryServiceSequence.add(ControlCenter.HEALTH_TEST_DISCOVERY_SERVICE_ID);
			dummyRequest.setDiscoveryServiceSequence(discoveryServiceSequence);

			AnalysisManager analysisManager = new ODFFactory().create().getAnalysisManager();
			AnalysisResponse resp = analysisManager.runAnalysis(dummyRequest);
			String reqId = resp.getId();
			AnalysisRequestStatus status = null;
			final int maxNumberOfTimesToPoll = 500;
			int count = 0;
			int msToSleepBetweenPolls = 20;
			boolean continuePolling = false;
			do {
				status = analysisManager.getAnalysisRequestStatus(reqId);
				continuePolling = (status.getState() == AnalysisRequestStatus.State.QUEUED || status.getState() == AnalysisRequestStatus.State.ACTIVE || status.getState() == AnalysisRequestStatus.State.NOT_FOUND) && count < maxNumberOfTimesToPoll;
				if (continuePolling) {
					count++;
					Thread.sleep(msToSleepBetweenPolls);
				}
			} while (continuePolling);
			logger.log(Level.INFO, "Health check request ''{3}'' has status ''{0}'', time spent: {2}ms details ''{1}''", new Object[] { status.getState(), status.getDetails(),
					count * msToSleepBetweenPolls, reqId });
			health.getMessages().add(MessageFormat.format("Details message: {0}", status.getDetails()));
			if (count >= maxNumberOfTimesToPoll) {
				health.setStatus( SystemHealth.HealthStatus.WARNING);
				String msg = MessageFormat.format("Health test request could not be processed in time ({0}ms)", (maxNumberOfTimesToPoll * msToSleepBetweenPolls));
				logger.log(Level.INFO, msg);
				health.getMessages().add(msg);
			} else {
				switch (status.getState()) {
				case NOT_FOUND:
					health.setStatus(SystemHealth.HealthStatus.ERROR);
					health.getMessages().add(MessageFormat.format("Request ID ''{0}'' got lost", reqId));
					break;
				case ERROR:
					health.setStatus(SystemHealth.HealthStatus.ERROR);
					break;
				case FINISHED:
					health.setStatus(SystemHealth.HealthStatus.OK);
					break;
				default:
					health.setStatus(SystemHealth.HealthStatus.ERROR);
				}
			}
		} catch (Exception exc) {
			logger.log(Level.WARNING, "An unknown error occurred", exc);
			health.setStatus(SystemHealth.HealthStatus.ERROR);
			health.getMessages().add(Utils.getExceptionAsString(exc));
		}
		return health;
	}

	/**
	 * Returns the status of the ODF thread manager
	 *
	 * @return Status of all threads making up the ODF thread manager
	 */
	public List<ThreadStatus> getThreadManagerStatus() {
		ThreadManager tm = new ODFInternalFactory().create(ThreadManager.class);
		return tm.getThreadManagerStatus();
	}

	/**
	 * Returns the status of the ODF messaging subsystem
	 *
	 * @return Status of the ODF messaging subsystem
	 */
	public MessagingStatus getMessagingStatus() {
		return new ODFInternalFactory().create(DiscoveryServiceQueueManager.class).getMessagingStatus();
	}

	/**
	 * Returns the status of the messaging subsystem and the internal thread manager
	 *
	 * @return Combined status of the messaging subsystem and the internal thread manager
	 */
	public ODFStatus getStatus() {
		ODFStatus status = new ODFStatus();
		status.setMessagingStatus(this.getMessagingStatus());
		status.setThreadManagerStatus(this.getThreadManagerStatus());
		return status;
	}

	/**
	 * Returns the current ODF version
	 *
	 * @return ODF version identifier
	 */
	public ODFVersion getVersion() {
		InputStream is = ODFUtils.class.getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/odfversion.txt");
		ODFVersion version = new ODFVersion();
		if (is == null) {
			version.setVersion("NOTFOUND");
		} else {
			version.setVersion(Utils.getInputStreamAsString(is, "UTF-8").trim());
		}
		return version;
	}

	/**
	 * Shuts down the ODF engine, purges all scheduled analysis requests from the queues, and cancels all running analysis requests.
	 * This means that all running jobs will be cancelled or their results will not be reported back.
	 * (for debugging purposes only)
	 *
	 * @param options Option for immediately restarting the engine after shutdown (default is not to restart immediately but only when needed)
	 */
	public void shutdown(ODFEngineOptions options) {
		long currentTime = System.currentTimeMillis();

		ControlCenter controlCenter = new ODFInternalFactory().create(ControlCenter.class);
		AdminMessage shutDownMessage = new AdminMessage();
		Type t = Type.SHUTDOWN;
		if (options.isRestart()) {
			t = Type.RESTART;
		}
		shutDownMessage.setAdminMessageType(t);
		String detailMsg = MessageFormat.format("Shutdown was requested on {0} via ODF API", new Object[] { new Date() });
		shutDownMessage.setDetails(detailMsg);
		logger.log(Level.INFO, detailMsg);
		controlCenter.getQueueManager().enqueueInAdminQueue(shutDownMessage);
		int maxPolls = 60;
		int counter = 0;
		int timeBetweenPollsMs = 1000;
		while (counter < maxPolls && ODFInitializer.getLastStopTimestamp() <= currentTime) {
			try {
				Thread.sleep(timeBetweenPollsMs);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			counter++;
		}
		long timeWaited = ((counter * timeBetweenPollsMs) / 1000);
		logger.log(Level.INFO, "Waited for {0} seconds for shutdown", timeWaited);
		if (counter >= maxPolls) {
			logger.log(Level.WARNING, "Waited for shutdown too long. Continuing." );
		} else {
			logger.log(Level.INFO, "Shutdown issued successfully");
		}
	}

	@Override
	public ServiceRuntimesInfo getRuntimesInfo() {
		return ServiceRuntimes.getRuntimesInfo(ServiceRuntimes.getAllRuntimes());
	}
}
