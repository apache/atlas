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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.text.MessageFormat;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DataSetCheckResult;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.spark.SparkDiscoveryService;
import org.apache.atlas.odf.api.spark.SparkServiceExecutor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint.SERVICE_INTERFACE_TYPE;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.spark.SparkUtils;
import org.apache.atlas.odf.json.JSONUtils;

/**
 * This class calls the actual Spark discovery services depending on the type of interface they implement.
 * The class is used to run a Spark discovery service either on a local Spark cluster ({@link SparkServiceExecutorImpl})
 * or on a remote Spark cluster ({@link SparkApplicationStub}).
 * 
 *
 */

public class LocalSparkServiceExecutor implements SparkServiceExecutor {
	private Logger logger = Logger.getLogger(LocalSparkServiceExecutor.class.getName());
	private SparkSession spark;
	private MetadataStore mds;

	void setSparkSession(SparkSession spark) {
		this.spark = spark;
	}

	void setMetadataStore(MetadataStore mds) {
		this.mds = mds;
	}

	@Override
	public DataSetCheckResult checkDataSet(DiscoveryServiceProperties dsProp, DataSetContainer container) {
		DiscoveryServiceSparkEndpoint endpoint;
		try {
			endpoint = JSONUtils.convert(dsProp.getEndpoint(), DiscoveryServiceSparkEndpoint.class);
		} catch (JSONException e1) {
			throw new RuntimeException(e1);
		}
		DataSetCheckResult checkResult = new DataSetCheckResult();
		try {
			SERVICE_INTERFACE_TYPE inputMethod = endpoint.getInputMethod();
			if (inputMethod.equals(SERVICE_INTERFACE_TYPE.DataFrame)) {
				MetaDataObject dataSet = container.getDataSet();
				if (!(dataSet instanceof RelationalDataSet)) {
					checkResult.setDataAccess(DataSetCheckResult.DataAccess.NotPossible);
					checkResult.setDetails("This service can only process relational data sets.");
				} else {
					checkResult.setDataAccess(DataSetCheckResult.DataAccess.Possible);
					Dataset<Row> df = SparkUtils.createDataFrame(this.spark, container, this.mds);
					// Print first rows to check whether data frame can be accessed
					df.show(10);
				}
			} else if (inputMethod.equals(SERVICE_INTERFACE_TYPE.Generic)) {
				Class<?> clazz = Class.forName(endpoint.getClassName());
				Constructor<?> cons = clazz.getConstructor();
				SparkDiscoveryService service = (SparkDiscoveryService) cons.newInstance();
				service.setMetadataStore(this.mds);
				service.setSparkSession(this.spark);
				checkResult = service.checkDataSet(container);
			}
		} catch (Exception e) {
			logger.log(Level.WARNING,"Access to data set not possible.", e);
			checkResult.setDataAccess(DataSetCheckResult.DataAccess.NotPossible);
			checkResult.setDetails(getExceptionAsString(e));
		} finally {
			this.spark.close();
			this.spark.stop();
		}
		return checkResult;
	}

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceProperties dsProp, DiscoveryServiceRequest request) {
		DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
		response.setDetails("Annotations created successfully");
		response.setCode(DiscoveryServiceResponse.ResponseCode.OK);
		try {
			DiscoveryServiceSparkEndpoint endpoint = JSONUtils.convert(dsProp.getEndpoint(), DiscoveryServiceSparkEndpoint.class);
			Class<?> clazz = Class.forName(endpoint.getClassName());
			DataSetContainer container = request.getDataSetContainer();
			String[] optionalArgs = {}; // For future use
			SERVICE_INTERFACE_TYPE inputMethod = endpoint.getInputMethod();

			if (inputMethod.equals(SERVICE_INTERFACE_TYPE.DataFrame)) {
				if (!(container.getDataSet() instanceof RelationalDataSet)) {
					throw new RuntimeException("This service can only process relational data sets (DataFile or Table).");
				}
				Dataset<Row> df = SparkUtils.createDataFrame(this.spark, container, this.mds);
				@SuppressWarnings("unchecked")
				Map<String, Dataset<Row>> annotationDataFrameMap = (Map<String, Dataset<Row>>) clazz.getMethod("processDataFrame", SparkSession.class, Dataset.class, String[].class).invoke(null, this.spark, df, (Object[]) optionalArgs);
				response.setResult(SparkUtils.createAnnotationsFromDataFrameMap(container, annotationDataFrameMap, this.mds));
			} else if (inputMethod.equals(SERVICE_INTERFACE_TYPE.Generic)) {
				Constructor<?> cons = clazz.getConstructor();
				SparkDiscoveryService service = (SparkDiscoveryService) cons.newInstance();
				service.setMetadataStore(this.mds);
				service.setSparkSession(this.spark);
				response = service.runAnalysis(request);
			} else {
				throw new RuntimeException(MessageFormat.format("Unsupported interface type {0}.", inputMethod));
			}
		} catch(Exception e) {
			logger.log(Level.WARNING,"Error running discovery service.", e);
			response.setDetails(getExceptionAsString(e));
			response.setCode(DiscoveryServiceResponse.ResponseCode.UNKNOWN_ERROR);
		} finally {
			this.spark.close();
			this.spark.stop();
		}
		return response;
	}

	public static String getExceptionAsString(Throwable exc) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		exc.printStackTrace(pw);
		String st = sw.toString();
		return st;
	}
}
