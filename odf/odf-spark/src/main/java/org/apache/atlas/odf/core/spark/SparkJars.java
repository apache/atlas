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
package org.apache.atlas.odf.core.spark;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import org.apache.atlas.odf.core.Utils;

public class SparkJars {
	private static Logger logger = Logger.getLogger(SparkJars.class.getName());

	public String getResourceAsJarFile(String resource) {
		ClassLoader cl = this.getClass().getClassLoader();
		InputStream inputStream = cl.getResourceAsStream(resource);
		if (inputStream == null) {
        	String msg = MessageFormat.format("Resource {0} was not found.", resource);
        	logger.log(Level.WARNING, msg);
        	throw new RuntimeException(msg);
		}
		String tempFilePath = null;
		try {
		    File tempFile = File.createTempFile("driver", "jar");
		    tempFilePath = tempFile.getAbsolutePath();
		    logger.log(Level.INFO, "Creating temporary file " + tempFilePath);
			IOUtils.copy(inputStream, new FileOutputStream(tempFile));
			inputStream.close();
			Utils.runSystemCommand("chmod 755 " + tempFilePath);
		} catch (IOException e) {
        	String msg = MessageFormat.format("Error creating temporary file from resource {0}: ", resource);
        	logger.log(Level.WARNING, msg, e);
        	throw new RuntimeException(msg + Utils.getExceptionAsString(e));
		}
		return tempFilePath;
	}

	public String getUrlasJarFile(String urlString) {
		try {
		    File tempFile = File.createTempFile("driver", "jar");
	    	logger.log(Level.INFO, "Creating temporary file " + tempFile);
			FileUtils.copyURLToFile(new URL(urlString), tempFile);
			Utils.runSystemCommand("chmod 755 " + tempFile.getAbsolutePath());
			return tempFile.getAbsolutePath();
		} catch (MalformedURLException e) {
			String msg = MessageFormat.format("An invalid Spark application URL {0} was provided: ", urlString);
			logger.log(Level.WARNING, msg, e);
			throw new RuntimeException(msg + Utils.getExceptionAsString(e));
		} catch (IOException e) {
			logger.log(Level.WARNING, "Error processing Spark application jar file.", e);
			throw new RuntimeException("Error processing Spark application jar file: " + Utils.getExceptionAsString(e));
		}
	}

	public byte[] getFileAsByteArray(String resourceOrURL) {
        try {
        	InputStream inputStream;
        	if (isValidUrl(resourceOrURL)) {
            	inputStream = new URL(resourceOrURL).openStream();
        	} else {
        		ClassLoader cl = this.getClass().getClassLoader();
        		inputStream = cl.getResourceAsStream(resourceOrURL);
        		if (inputStream == null) {
                	String msg = MessageFormat.format("Resource {0} was not found.", resourceOrURL);
                	logger.log(Level.WARNING, msg);
                	throw new RuntimeException(msg);
        		}
        	}
        	byte[] bytes = IOUtils.toByteArray(inputStream);
        	return bytes;
        } catch (IOException e) {
        	String msg = MessageFormat.format("Error converting jar file {0} into byte array: ", resourceOrURL);
        	logger.log(Level.WARNING, msg, e);
        	throw new RuntimeException(msg + Utils.getExceptionAsString(e));
        }
	}

	public static boolean isValidUrl(String urlString) {
		try {
			new URL(urlString);
			return true;
		} catch (java.net.MalformedURLException exc) {
			// Expected exception if URL is not valid
			return false;
		}
	}
}
