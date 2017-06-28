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

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DataSetCheckResult;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.api.spark.SparkServiceExecutor;
import org.apache.spark.sql.SparkSession;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.settings.SparkConfig;
import org.apache.atlas.odf.json.JSONUtils;

/**
 * Calls the appropriate implementation (local vs. remote) of the @link SparkServiceExecutor depending on the current @SparkConfig.
 * Prepares the local Spark cluster to be used in unit and integration tests.
 * 
 *
 */

public class SparkServiceExecutorImpl implements SparkServiceExecutor {
	private Logger logger = Logger.getLogger(SparkServiceExecutorImpl.class.getName());

	@Override
	public DataSetCheckResult checkDataSet(DiscoveryServiceProperties dsri, DataSetContainer dataSetContainer) {
		return this.getExecutor(dsri).checkDataSet(dsri, dataSetContainer);
	};

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceProperties dsri, DiscoveryServiceRequest request) {
		return this.getExecutor(dsri).runAnalysis(dsri, request);
	}

	private SparkServiceExecutor getExecutor(DiscoveryServiceProperties dsri) {
		SettingsManager config = new ODFFactory().create().getSettingsManager();
		DiscoveryServiceSparkEndpoint endpoint;
		try {
			endpoint = JSONUtils.convert(dsri.getEndpoint(), DiscoveryServiceSparkEndpoint.class);
		} catch (JSONException e1) {
			throw new RuntimeException(e1);
		}

		SparkConfig sparkConfig = config.getODFSettings().getSparkConfig();
		if (sparkConfig == null) {
			String msg = "No Spark service is configured. Please manually register Spark service or bind a Spark service to your ODF Bluemix app.";
			logger.log(Level.SEVERE, msg);
			throw new RuntimeException(msg);
		} else {
			logger.log(Level.INFO, "Using local Spark cluster {0}.", sparkConfig.getClusterMasterUrl());
			SparkSession spark = SparkSession.builder().master(sparkConfig.getClusterMasterUrl()).appName(dsri.getName()).getOrCreate();
			SparkJars sparkJars = new SparkJars();
			try {
			    // Load jar file containing the Spark job to be started
			    URLClassLoader classLoader = (URLClassLoader)ClassLoader.getSystemClassLoader();
				Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
			    method.setAccessible(true);
			    String applicationJarFile;
				if (SparkJars.isValidUrl(endpoint.getJar())) {
					applicationJarFile = sparkJars.getUrlasJarFile(endpoint.getJar());
				} else {
					applicationJarFile = sparkJars.getResourceAsJarFile(endpoint.getJar());
				}
				logger.log(Level.INFO, "Using application jar file {0}.", applicationJarFile);
			    method.invoke(classLoader, new URL("file:" + applicationJarFile));
			} catch (Exception e) {
				String msg = MessageFormat.format("Error loading jar file {0} implementing the Spark discovery service: ", endpoint.getJar());
				logger.log(Level.WARNING, msg, e);
				spark.close();
				spark.stop();
				throw new RuntimeException(msg, e);
			}
			LocalSparkServiceExecutor executor = new LocalSparkServiceExecutor();
			executor.setSparkSession(spark);
			executor.setMetadataStore(new ODFFactory().create().getMetadataStore());
		    return executor;
		}
	}
}
