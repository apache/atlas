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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DataSetCheckResult;
import org.apache.atlas.odf.api.spark.SparkDiscoveryServiceBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.atlas.odf.api.spark.SparkDiscoveryService;
import org.apache.atlas.odf.api.spark.SparkUtils;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse.ResponseCode;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;

public class SparkDiscoveryServiceExample extends SparkDiscoveryServiceBase implements SparkDiscoveryService {
	static Logger logger = Logger.getLogger(SparkDiscoveryServiceExample.class.getName());

	@Override
	public DataSetCheckResult checkDataSet(DataSetContainer dataSetContainer) {
		logger.log(Level.INFO, "Checking data set access.");
		DataSetCheckResult checkResult = new DataSetCheckResult();
		checkResult.setDataAccess(DataSetCheckResult.DataAccess.Possible);
		Dataset<Row> df = SparkUtils.createDataFrame(this.spark, dataSetContainer, this.mds);
		// Print first rows to check whether data frame can be accessed
		df.show(10);
		return checkResult;
	}

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
		logger.log(Level.INFO, "Starting discovery service.");
		Dataset<Row> df = SparkUtils.createDataFrame(spark, request.getDataSetContainer(), this.mds);
		Map<String,Dataset<Row>> annotationDataFrameMap = SummaryStatistics.processDataFrame(this.spark, df, null);
		DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
		response.setCode(ResponseCode.OK);
		response.setDetails("Discovery service successfully completed.");
		response.setResult(SparkUtils.createAnnotationsFromDataFrameMap(request.getDataSetContainer(), annotationDataFrameMap, this.mds));
		return response;
	}
}
