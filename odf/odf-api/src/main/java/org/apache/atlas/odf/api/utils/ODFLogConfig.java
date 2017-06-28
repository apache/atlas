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
package org.apache.atlas.odf.api.utils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Class to be used for log configuration.
 * It reads the system property odf.logspec which value must be of the form
 * <Level>,<FilePattern>
 * For instance
 *   ALL,/tmp/odf-trace.log
 *
 *
 */
public class ODFLogConfig {

	void log(String s) {
		System.out.println(s);
	}

	public static class ODFFileHandler extends FileHandler {
		public ODFFileHandler(String pattern) throws IOException {
			super(pattern);
		}
	}

	Handler createHandler(String odfLogFilePattern) {
		Handler result = null;
		Formatter formatter = new SimpleFormatter();
		try {
			// 1MB file limit, 3 files
			result = new ODFFileHandler(odfLogFilePattern);
			result.setFormatter(formatter);
		} catch (Exception exc) {
			exc.printStackTrace();
			return null;
		}
		return result;
	}

	private ODFLogConfig() {
		try {
			String logSpec = System.getProperty("odf.logspec");
			log("ODF Logging spec of system property odf.logspec: " + logSpec);
			if (logSpec == null) {
				logSpec = System.getenv("ODFLOGSPEC");
				log("ODF Logging spec of env var ODFLOGSPEC " + logSpec);
				if (logSpec == null) {
					return;
				}
			}
			int ix = logSpec.indexOf(",");
			if (ix == -1) {
				return;
			}
			String levelString = logSpec.substring(0, ix);

			String odfLogFilePattern = logSpec.substring(ix + 1);
			String msg = MessageFormat.format("Configuring ODF logging with level {0} and log file: {1}", new Object[] { levelString, odfLogFilePattern });
			log(msg);

			Handler odfHandler = createHandler(odfLogFilePattern);
			if (odfHandler == null) {
				return;
			}
			Level level = Level.parse(levelString);
			Logger odfRootLogger = Logger.getLogger("org.apache.atlas.odf");

			// remove existing handler
			for (Handler h : odfRootLogger.getHandlers()) {
				if (h instanceof ODFFileHandler) {
					odfRootLogger.removeHandler(h);
				}
			}

			odfRootLogger.setLevel(level);
			odfHandler.setLevel(level);
			odfRootLogger.addHandler(odfHandler);
			log("ODF logger configured.");
		} catch (Exception exc) {
			exc.printStackTrace();
		}
	}

	static Object lockObject = new Object();
	static ODFLogConfig config = null;

	public static void run() {
		synchronized (lockObject) {
			if (config == null) {
				config = new ODFLogConfig();
			}
		}
	}
}
