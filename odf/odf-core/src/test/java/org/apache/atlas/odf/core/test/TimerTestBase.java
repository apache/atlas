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
package org.apache.atlas.odf.core.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.wink.json4j.JSONException;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;

import com.google.common.io.Files;

public class TimerTestBase {
	static final String logFilePath = "/tmp/odf-test-execution-log.csv";
	static Map<String, HashMap<String, Long>> testTimeMap = new HashMap<String, HashMap<String, Long>>();
	final static Logger logger = ODFTestLogger.get();

	@Rule
	public Stopwatch timeWatcher = new Stopwatch() {
		@Override
		protected void finished(long nanos, Description description) {
			HashMap<String, Long> testMap = testTimeMap.get(description.getClassName());
			if (testMap == null) {
				testMap = new HashMap<String, Long>();
				testTimeMap.put(description.getClassName(), testMap);
			}
			testMap.put(description.getMethodName(), (nanos / 1000 / 1000));
		}
	};

	@AfterClass
	public static void tearDownAndLogTimes() throws JSONException {
		try {
			File logFile = new File(logFilePath);
			Set<String> uniqueRows = new HashSet<String>();
			if (logFile.exists()) {
				uniqueRows = new HashSet<String>(Files.readLines(logFile, StandardCharsets.UTF_8));
			}

			for (Entry<String, HashMap<String, Long>> entry : testTimeMap.entrySet()) {
				for (Entry<String, Long> testEntry : entry.getValue().entrySet()) {
					String logRow = new StringBuilder().append(testEntry.getKey()).append(",").append(testEntry.getValue()).append(",").append(entry.getKey()).append(",")
							.append(System.getProperty("odf.build.project.name", "ProjectNameNotDefined")).toString();
					uniqueRows.add(logRow);
				}
			}

			StringBuilder logContent = new StringBuilder();
			Iterator<String> rowIterator = uniqueRows.iterator();
			while (rowIterator.hasNext()) {
				logContent.append(rowIterator.next());
				if (rowIterator.hasNext()) {
					logContent.append("\n");
				}
			}

			logger.info("Total time consumed by succeeded tests:\n" + logContent.toString());
			logFile.createNewFile();
			Files.write(logContent.toString().getBytes("UTF-8"), logFile);
		} catch (IOException e) {
			logger.warning("Error writing test execution log");
			e.printStackTrace();
		}
	}
}
