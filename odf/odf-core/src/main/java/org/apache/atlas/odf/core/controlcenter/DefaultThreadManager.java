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

import java.lang.Thread.State;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.engine.ThreadStatus;

public class DefaultThreadManager implements ThreadManager {

	private Logger logger = Logger.getLogger(DefaultThreadManager.class.getName());

	static Object unmanagedThreadLock = new Object();
	static Map<String, Thread> unmanagedThreadMap = new HashMap<String, Thread>();
	static Map<String, ODFRunnable> unmanagedThreadRunnableMap = new HashMap<String, ODFRunnable>();
	
	ExecutorService executorService;

	public DefaultThreadManager() {
	}
	
	private boolean isThreadRunning(Thread thread) {
		return thread.getState() != State.TERMINATED;
	}
	
	private void purgeTerminatedThreads() {
		List<String> entriesToBeRemoved = new ArrayList<String>();
		List<String> entriesToBeKept = new ArrayList<String>();
		for (Map.Entry<String, Thread> entry : unmanagedThreadMap.entrySet()) {
			if (!isThreadRunning(entry.getValue())) {
				entriesToBeRemoved.add(entry.getKey());
			} else {
				entriesToBeKept.add(entry.getKey());
			}
		}
		for (String id : entriesToBeRemoved) {
			unmanagedThreadMap.remove(id);
			unmanagedThreadRunnableMap.remove(id);
		}
		logger.finer("Removed finished threads: " + entriesToBeRemoved.toString());
		logger.finer("Kept unfinished threads: " + entriesToBeKept.toString());
	}
	
	@Override
	public ThreadStartupResult startUnmanagedThread(final String id, final ODFRunnable runnable) {
		ThreadStartupResult result = new ThreadStartupResult(id) {
			@Override
			public boolean isReady() {
				synchronized (unmanagedThreadLock) {
					if (unmanagedThreadRunnableMap.containsKey(id)) {
						return unmanagedThreadRunnableMap.get(id).isReady();
					}
				}
				return false;
			}
		};
		synchronized (unmanagedThreadLock) {
			purgeTerminatedThreads();
			Thread t = unmanagedThreadMap.get(id);
			if (t != null) {
				if (isThreadRunning(t)) {
					return result;
				}
			} 
			runnable.setExecutorService(executorService);

			Thread newThread = new Thread(runnable);
			result.setNewThreadCreated(true);
			newThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

				@Override
				public void uncaughtException(Thread thread, Throwable throwable) {
					logger.log(Level.WARNING, "Uncaught exception in thread " + id + " - Thread will shutdown!", throwable);
					synchronized (unmanagedThreadLock) {
						purgeTerminatedThreads();
					}
				}
			});

			newThread.setDaemon(true); // TODO is it a daemon?
			newThread.start();
			unmanagedThreadMap.put(id, newThread);
			unmanagedThreadRunnableMap.put(id,  runnable);
		}
		return result;
	}

	@Override
	public ThreadStatus.ThreadState getStateOfUnmanagedThread(String id) {
		synchronized (unmanagedThreadLock) {
			Thread t = unmanagedThreadMap.get(id);
			if (t == null) {
				return ThreadStatus.ThreadState.NON_EXISTENT;
			}
			Thread.State ts = t.getState();
			switch (ts) {
			case TERMINATED:
				return ThreadStatus.ThreadState.FINISHED;
			default:
				return ThreadStatus.ThreadState.RUNNING;
			}
		}
	}



	@Override
	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	@Override
	public void shutdownAllUnmanagedThreads() {
		synchronized (unmanagedThreadLock) {
			logger.log(Level.INFO, "Shutting down all ODF threads...");
			for (String id : unmanagedThreadMap.keySet()) {
				shutdownThreadImpl(id, false);
			}
			unmanagedThreadMap.clear();
			unmanagedThreadRunnableMap.clear();
			logger.log(Level.INFO, "All ODF threads shutdown");
			purgeTerminatedThreads();
		}		
	}
	
	public void shutdownThreads(List<String> names) {
		synchronized (unmanagedThreadLock) {
			for (String name : names) {
				shutdownThreadImpl(name, true);
			}
		}		
	}

	private void shutdownThreadImpl(String id, boolean purge) {
		Thread t = unmanagedThreadMap.get(id);
		if (t == null) {
			return;
		}
		ODFRunnable r = unmanagedThreadRunnableMap.get(id);
		r.cancel();
		try {
			Thread.sleep(500);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		int max = 60;
		while (t.getState() != Thread.State.TERMINATED) {
			if (max == 0) {
				break;
			}
			max--;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// do nothing
				e.printStackTrace();
			}
		}
		if (max == 0) {
			logger.log(Level.WARNING, "Thread {0} did not stop on its own, must be interrupted.", id);
			t.interrupt();
		}
		if (purge) {
			purgeTerminatedThreads();
		}
	}

	@Override
	public int getNumberOfRunningThreads() {
		synchronized (unmanagedThreadLock) {
			int result = 0;
			for (Thread t : unmanagedThreadMap.values()) {
				if (isThreadRunning(t)) {
					result++;
				}
			}
			return result;
		}
	}

	@Override
	public List<ThreadStatus> getThreadManagerStatus() {
		synchronized (unmanagedThreadLock) {
			List<ThreadStatus> result = new ArrayList<ThreadStatus>();
			for (Entry<String, Thread> entry : unmanagedThreadMap.entrySet()) {
				ThreadStatus status = new ThreadStatus();
				status.setId(entry.getKey());
				status.setState(getStateOfUnmanagedThread(entry.getKey()));
				ODFRunnable odfRunnable = unmanagedThreadRunnableMap.get(entry.getKey());
				if (odfRunnable != null) {
					status.setType(odfRunnable.getClass().getName());
				}
				result.add(status);
			}

			return result;
		}
	}

	@Override
	public void waitForThreadsToBeReady(long waitingLimitMs, List<ThreadStartupResult> startedThreads) throws TimeoutException {
		Set<String> threadsToWaitFor = new HashSet<String>();
		for (ThreadStartupResult res : startedThreads) {
			//Only if a new thread was created we wait for it to be ready.
			if (res.isNewThreadCreated()) {
				threadsToWaitFor.add(res.getThreadId());
			}
		}
		if (threadsToWaitFor.isEmpty()) {
			return;
		}

		final int msToWait = 200;
		final long maxPolls = waitingLimitMs / msToWait;
		int count = 0;
		while (threadsToWaitFor.size() > 0 && count < maxPolls) {
			List<String> ready = new ArrayList<String>();
			List<String> notReady = new ArrayList<String>();
			for (ThreadStartupResult thr : startedThreads) {
				if (thr.isReady()) {
					ready.add(thr.getThreadId());
					threadsToWaitFor.remove(thr.getThreadId());
				} else {
					notReady.add(thr.getThreadId());
				}
			}

			logger.fine("Ready: " + ready);
			logger.fine("NotReady: " + notReady);

			try {
				Thread.sleep(msToWait);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			count++;
		}
		if (count >= maxPolls) {
			String msg = "Threads: " + threadsToWaitFor + "' are not ready yet after " + waitingLimitMs + " ms, give up to wait for it";
			logger.log(Level.WARNING, msg);
			throw new TimeoutException(msg);
		}
		
		logger.fine("All threads ready after " + (count * msToWait) + "ms");
	}

	@Override
	public ODFRunnable getRunnable(String name) {
		synchronized (unmanagedThreadLock) {
			return unmanagedThreadRunnableMap.get(name);
		}
	}
}
