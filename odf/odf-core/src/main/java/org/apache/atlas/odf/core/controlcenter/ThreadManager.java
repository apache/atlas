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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.atlas.odf.api.engine.ThreadStatus;

public interface ThreadManager {

	void waitForThreadsToBeReady(long waitingLimitMs, List<ThreadStartupResult> startedThreads) throws TimeoutException;

	ThreadStartupResult startUnmanagedThread(String name, ODFRunnable runnable);
	
	ThreadStatus.ThreadState getStateOfUnmanagedThread(String name);
	
	ODFRunnable getRunnable(String name);
	
	void setExecutorService(ExecutorService executorService);
	
	void shutdownAllUnmanagedThreads();
	
	void shutdownThreads(List<String> names);
	
	int getNumberOfRunningThreads();

	List<ThreadStatus> getThreadManagerStatus();

	public abstract class ThreadStartupResult {

		private String threadId;
		private boolean newThreadCreated;

		public ThreadStartupResult(String id) {
			this.threadId = id;
		}

		public String getThreadId() {
			return threadId;
		}

		public boolean isNewThreadCreated() {
			return newThreadCreated;
		}

		public void setNewThreadCreated(boolean newThreadCreated) {
			this.newThreadCreated = newThreadCreated;
		}

		public abstract boolean isReady();

	}


}
