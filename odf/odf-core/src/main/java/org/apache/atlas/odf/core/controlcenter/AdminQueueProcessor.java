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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.core.ODFInitializer;
import org.apache.atlas.odf.json.JSONUtils;

public class AdminQueueProcessor implements QueueMessageProcessor {

	private Logger logger = Logger.getLogger(AdminQueueProcessor.class.getName());

	@Override
	public void process(ExecutorService executorService, String msg, int partition, long offset) {
		AdminMessage adminMessage;
		try {
			adminMessage = JSONUtils.fromJSON(msg, AdminMessage.class);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
		switch (adminMessage.getAdminMessageType()) {
		case SHUTDOWN:
			initiateShutdown(executorService, false);
			break;
		case RESTART:
			initiateShutdown(executorService, true);
			break;
		default:
			// do nothing
		}
	}

	static Object restartLockObject = new Object();

	private void initiateShutdown(ExecutorService executorService, final boolean restart) {
		logger.log(Level.INFO, "Shutdown of ODF was requested...");
		Runnable shutDownRunnable = new Runnable() {

			@Override
			public void run() {
				logger.log(Level.INFO, "Initiating shutdown");

				// sleep some time before initiating the actual shutdown to give the process() a chance to return
				// before it is itself shut down
				long sleepTimeBeforeShutdown = 1000;
				try {
					Thread.sleep(sleepTimeBeforeShutdown);
				} catch (InterruptedException e) {
					// do nothing
					e.printStackTrace();
				}

				synchronized (restartLockObject) {
					logger.log(Level.INFO, "Shutting down ODF...");
					try {
						ODFInitializer.stop();
						logger.log(Level.INFO, "ODF was shutdown");
											
						if (restart) {
							logger.log(Level.INFO, "Restarting ODF");
							ODFInitializer.start();
							logger.log(Level.INFO, "ODF restarted");
						}
					}  catch (Exception e) {
						logger.log(Level.SEVERE, "An unexpected error occurred when shutting down ODF", e);
					}
				}

			}

		};

		executorService.submit(shutDownRunnable);
	}

}
