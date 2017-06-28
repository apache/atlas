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
package org.apache.atlas.odf.admin.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

public class LoggingHandler extends Handler {

	private int LOG_CACHE_SIZE = 1000;
	private static List<LogRecord> cachedLogs = Collections.synchronizedList(new ArrayList<LogRecord>());

	@Override
	public void publish(LogRecord record) {
		cachedLogs.add(record);
		if (cachedLogs.size() >= LOG_CACHE_SIZE) {
			cachedLogs.remove(0);
		}
	}

	@Override
	public void flush() {
		cachedLogs.clear();
	}

	@Override
	public void close() throws SecurityException {
		cachedLogs.clear();
	}

	public List<LogRecord> getCachedLog() {
		return new ArrayList<LogRecord>(cachedLogs);
	}

	public String getFormattedCachedLog(Integer numberOfLogs, Level logLevel) {
		final List<LogRecord> cachedLog = getCachedLog();
		StringBuilder lg = new StringBuilder();
		final SimpleFormatter simpleFormatter = new SimpleFormatter();
		if (numberOfLogs != null) {
			for (int no = numberOfLogs; no > 0; no--) {
				if (no > -1 && no < cachedLog.size() - 1) {
					final LogRecord record = cachedLog.get(cachedLog.size() - no);
					if (record.getLevel().intValue() >= logLevel.intValue()) {
						lg.append(simpleFormatter.format(record));
					}
				}
			}
		} else {
			for (LogRecord record : cachedLog) {
				lg.append(simpleFormatter.format(record));
			}
		}
		return lg.toString();
	}
}
