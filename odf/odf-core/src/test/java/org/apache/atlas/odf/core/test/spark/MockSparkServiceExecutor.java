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
package org.apache.atlas.odf.core.test.spark;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DataSetCheckResult;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;
import org.apache.atlas.odf.api.spark.SparkServiceExecutor;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;

public class MockSparkServiceExecutor implements SparkServiceExecutor {
	Logger logger = Logger.getLogger(MockSparkServiceExecutor.class.getName());

	public DataSetCheckResult checkDataSet(DiscoveryServiceProperties dsri, DataSetContainer dataSetContainer) {
		DataSetCheckResult checkResult = new DataSetCheckResult();
		checkResult.setDataAccess(DataSetCheckResult.DataAccess.Possible);
		return checkResult;
	}

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceProperties dsri, DiscoveryServiceRequest request) {
		logger.log(Level.INFO, "Starting Spark mock application.");
		DiscoveryServiceSparkEndpoint sparkEndpoint;
		try {
			sparkEndpoint = JSONUtils.convert(dsri.getEndpoint(), DiscoveryServiceSparkEndpoint.class);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
		if (sparkEndpoint.getJar() == null) {
			throw new RuntimeException("Spark application is not set in Spark endpoint.");
		}
		logger.log(Level.INFO, "Application name is {0}.", sparkEndpoint.getJar());
		logger.log(Level.INFO, "Spark application finished.");
		DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
		response.setCode(DiscoveryServiceResponse.ResponseCode.OK);
		response.setDetails("Discovery service completed successfully.");
		return  response;
	}
}
