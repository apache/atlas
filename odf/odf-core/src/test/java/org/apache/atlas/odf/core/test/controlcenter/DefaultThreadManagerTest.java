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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.engine.ThreadStatus;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.ExecutorServiceFactory;
import org.apache.atlas.odf.core.controlcenter.ODFRunnable;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.core.controlcenter.ThreadManager;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.TimerTestBase;

public class DefaultThreadManagerTest extends TimerTestBase {

	int threadMS = 100;
	int waitMS = 5000;
	
	Logger logger = ODFTestLogger.get();

	class TestRunnable implements ODFRunnable {

		String id;
		boolean cancelled = false;
		long msToWaitBeforeFinish;
		
		public TestRunnable(String id, long msToWaitBeforeFinish) {
			this.id = id;
			this.msToWaitBeforeFinish = msToWaitBeforeFinish;
		}
		
		public TestRunnable(String id) {
			this(id, threadMS);
		}

		@Override
		public void run() {
			logger.info("Starting thread with ID: " + id);
			try {
				Thread.sleep(msToWaitBeforeFinish);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("Thread finished with ID: " + id);

		}

		@Override
		public void setExecutorService(ExecutorService service) {
			// TODO Auto-generated method stub

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

	@Test
	public void testSimple() throws Exception {
		ODFInternalFactory f = new ODFInternalFactory();
		ThreadManager tm = f.create(ThreadManager.class);
		tm.setExecutorService(f.create(ExecutorServiceFactory.class).createExecutorService());
		assertNotNull(tm);

		String id1 = "id1";
		String id2 = "id2";

		// start id1
		ThreadStatus.ThreadState st = tm.getStateOfUnmanagedThread(id1);
		Assert.assertEquals(ThreadStatus.ThreadState.NON_EXISTENT, st);

		boolean b = tm.startUnmanagedThread(id1, new TestRunnable(id1)).isNewThreadCreated();
		assertTrue(b);
		b = tm.startUnmanagedThread(id1, new TestRunnable(id1)).isNewThreadCreated();
		assertFalse(b);

		st = tm.getStateOfUnmanagedThread(id1);
		Assert.assertEquals(ThreadStatus.ThreadState.RUNNING, st);

		// start id2
		st = tm.getStateOfUnmanagedThread(id2);
		Assert.assertEquals(ThreadStatus.ThreadState.NON_EXISTENT, st);

		b = tm.startUnmanagedThread(id2, new TestRunnable(id2)).isNewThreadCreated();
		assertTrue(b);
		b = tm.startUnmanagedThread(id2, new TestRunnable(id2)).isNewThreadCreated();
		assertFalse(b);

		Thread.sleep(waitMS);
		st = tm.getStateOfUnmanagedThread(id1);
		Assert.assertEquals(ThreadStatus.ThreadState.FINISHED, st);
		b = tm.startUnmanagedThread(id1, new TestRunnable(id1)).isNewThreadCreated();
		assertTrue(b);

		st = tm.getStateOfUnmanagedThread(id2);
		// id2 should be removed from thread list
		Assert.assertTrue(ThreadStatus.ThreadState.FINISHED.equals(st) || ThreadStatus.ThreadState.NON_EXISTENT.equals(st));

		tm.shutdownThreads(Arrays.asList("id1", "id2"));
	}

	@Test
	public void testManyThreads() throws Exception {
		ODFInternalFactory f = new ODFInternalFactory();
		ThreadManager tm = f.create(ThreadManager.class);
		tm.setExecutorService(f.create(ExecutorServiceFactory.class).createExecutorService());

		assertNotNull(tm);

		List<String> threadIds = new ArrayList<>();
		int THREAD_NUM = 20;
		for (int i = 0; i < THREAD_NUM; i++) {
			String id = "ThreadID" + i;
			threadIds.add(id);
			ThreadStatus.ThreadState st = tm.getStateOfUnmanagedThread(id);
			Assert.assertEquals(ThreadStatus.ThreadState.NON_EXISTENT, st);

			boolean b = tm.startUnmanagedThread(id, new TestRunnable(id)).isNewThreadCreated();
			assertTrue(b);
			b = tm.startUnmanagedThread(id, new TestRunnable(id)).isNewThreadCreated();
			assertFalse(b);

			st = tm.getStateOfUnmanagedThread(id);
			Assert.assertEquals(ThreadStatus.ThreadState.RUNNING, st);

		}
		logger.info("All threads scheduled");

		Thread.sleep(waitMS);

		for (int i = 0; i < THREAD_NUM; i++) {
			String id = "ThreadID" + i;
			ThreadStatus.ThreadState st = tm.getStateOfUnmanagedThread(id);
			Assert.assertEquals(ThreadStatus.ThreadState.FINISHED, st);
		}
		tm.shutdownThreads(threadIds);

	}

}
