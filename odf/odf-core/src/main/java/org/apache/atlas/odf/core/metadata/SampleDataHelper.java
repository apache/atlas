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
package org.apache.atlas.odf.core.metadata;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.core.Utils;

public class SampleDataHelper {
	private static Logger logger = Logger.getLogger(SampleDataHelper.class.getName());
	private static final String SAMPLE_DATA_FILE_LIST = "sample-data-toc.properties";
	private static final String SAMPLE_DATA_FILE_FOLDER = "org/apache/atlas/odf/core/metadata/internal/sampledata/";

	public static void copySampleFiles() {
		Properties toc = new Properties();
		ClassLoader cl = SampleDataHelper.class.getClassLoader();
		try {
			toc.load(cl.getResourceAsStream(SAMPLE_DATA_FILE_FOLDER + SAMPLE_DATA_FILE_LIST));

			for (String contentFileName : toc.stringPropertyNames()) {
				logger.log(Level.INFO, "Processing sample file: {0}", contentFileName);
				String url = copySampleDataFileContents(cl.getResourceAsStream(SAMPLE_DATA_FILE_FOLDER + contentFileName), contentFileName);
				logger.log(Level.INFO, "Sample data file ''{0}'' copied to {1}", new Object[] { contentFileName, url });
			}
		} catch(IOException e) {
			logger.log(Level.FINE, "An unexpected exception ocurred while connecting to Atlas", e);
			String messageText = MessageFormat.format("Content file list {0} could not be accessed.", SAMPLE_DATA_FILE_FOLDER + SAMPLE_DATA_FILE_LIST);
			throw new RuntimeException(messageText, e);
		}
		logger.log(Level.INFO, "All sample data files created");
	}

	private static String copySampleDataFileContents(InputStream is, String contentFile) throws IOException {
		String url = null;
		String target = null;
		String os = System.getProperty("os.name").toLowerCase();
		if (os.startsWith("windows")) {
			url = "file://localhost/c:/tmp/" + contentFile;
			target = "c:/tmp/" + contentFile;
		} else {
			url = "file:///tmp/" + contentFile;
			target = "/tmp/" + contentFile;
		}
		String content = Utils.getInputStreamAsString(is, "UTF-8");
		FileOutputStream fos = new FileOutputStream(target);
		fos.write(content.getBytes("UTF-8"));
		fos.close();
		return url;
	}
}
