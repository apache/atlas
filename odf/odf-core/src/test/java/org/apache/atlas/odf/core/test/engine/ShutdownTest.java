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
package org.apache.atlas.odf.core.test.engine;

import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus.State;
import org.apache.atlas.odf.api.engine.EngineManager;
import org.apache.atlas.odf.api.engine.ODFEngineOptions;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.atlas.odf.core.test.controlcenter.ODFAPITest;

public class ShutdownTest extends ODFTestBase {

	private void runAndTestThreads() throws Exception {
		ODFAPITest.runRequestAndCheckResult("successID", State.FINISHED, -1);
		ThreadManager tm = new ODFInternalFactory().create(ThreadManager.class);
		int numThreads = tm.getNumberOfRunningThreads();
		log.info("--- Number of running threads: " + numThreads);
		Assert.assertTrue(numThreads >= 3);		
	}

	@Test
	public void testShutdown() throws Exception {

		log.info("--- Running some request before shutdown...");
		runAndTestThreads();

		ThreadManager tm = new ODFInternalFactory().create(ThreadManager.class);
		log.info("--- Number of threads before shutdown: " + tm.getNumberOfRunningThreads());

		EngineManager engineManager = new ODFFactory().create().getEngineManager();
		ODFEngineOptions options = new ODFEngineOptions();
		options.setRestart(false);
		int numThreads = tm.getNumberOfRunningThreads();
		log.info("--- Number of threads before restart: " + numThreads);

		engineManager.shutdown(options);
		log.info("--- Shutdown requested...");
		int maxWait = 60;
		int waitCnt = 0;
		log.info("--- Shutdown requested, waiting for max " + maxWait + " seconds");
		while (tm.getNumberOfRunningThreads() > 0 && waitCnt < maxWait) {
			waitCnt++;
			Thread.sleep(1000);
		}
		log.info("--- Shutdown should be done by now, waited for " + waitCnt + " threads: " + tm.getNumberOfRunningThreads());
		Assert.assertNotEquals(waitCnt, maxWait);

	//	log.info("--- Starting ODF again....");

	//	ODFInitializer.start();
		log.info("--- Rerunning request after shutdown...");
		runAndTestThreads();

		int nrOfThreads = tm.getNumberOfRunningThreads();
		options.setRestart(true);
		engineManager.shutdown(options);
		maxWait = nrOfThreads * 2;
		waitCnt = 0;
		log.info("--- Restart requested..., wait for a maximum of " + (nrOfThreads * 2500) + " ms");
		while (tm.getNumberOfRunningThreads() > 0 && waitCnt < maxWait) {
			waitCnt++;
			Thread.sleep(1000);
		}
		log.info("--- Restart should be done by now");
		Thread.sleep(5000);
		numThreads = tm.getNumberOfRunningThreads();
		log.info("--- Number of threads after restart: " + numThreads);
		Assert.assertTrue(numThreads > 2);
		log.info("--- testShutdown finished");

	}

}
