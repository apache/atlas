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

import java.text.MessageFormat;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DataSetCheckResult;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.spark.SparkServiceExecutor;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint;
import org.apache.atlas.odf.core.Utils;

/**
 * Proxy for calling any type of Spark discovery services.
 * 
 *
 */

public class SparkDiscoveryServiceProxy implements SyncDiscoveryService {
	Logger logger = Logger.getLogger(SparkDiscoveryServiceProxy.class.getName());

	protected MetadataStore metadataStore;
	protected AnnotationStore annotationStore;
	protected ExecutorService executorService;
	private DiscoveryServiceProperties dsri;

	public SparkDiscoveryServiceProxy(DiscoveryServiceProperties dsri) {
		this.dsri = dsri;
	}

	@Override
	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	@Override
	public void setMetadataStore(MetadataStore metadataStore) {
		this.metadataStore = metadataStore;
	}

	@Override
	public DataSetCheckResult checkDataSet(DataSetContainer dataSetContainer) {
		DataSetCheckResult checkResult = new DataSetCheckResult();
		checkResult.setDataAccess(DataSetCheckResult.DataAccess.NotPossible);
		try {
			SparkServiceExecutor executor = new ODFInternalFactory().create(SparkServiceExecutor.class);
			checkResult = executor.checkDataSet(this.dsri, dataSetContainer);
		} catch (Exception e) {
			logger.log(Level.WARNING,"Error running discovery service.", e);
			checkResult.setDetails(Utils.getExceptionAsString(e));
		}
		return checkResult;
	}

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
		logger.log(Level.INFO,MessageFormat.format("Starting Spark discovery service ''{0}'', id {1}.", new Object[]{ dsri.getName(), dsri.getId() }));
		DiscoveryServiceSyncResponse response = new DiscoveryServiceSyncResponse();
		DiscoveryServiceSparkEndpoint endpoint;
		try {
			endpoint = JSONUtils.convert(dsri.getEndpoint(),  DiscoveryServiceSparkEndpoint.class);
		} catch (JSONException e1) {
			throw new RuntimeException(e1);
		}
		if ((endpoint.getJar() == null) || (endpoint.getJar().isEmpty())) {
			response.setDetails("No jar file  was provided that implements the Spark application.");
		} else try {
			SparkServiceExecutor executor = new ODFInternalFactory().create(SparkServiceExecutor.class);
			response = executor.runAnalysis(this.dsri, request);
			logger.log(Level.FINER, "Spark discovery service response: " + response.toString());
			logger.log(Level.INFO,"Spark discover service finished.");
			return response;
		} catch (Exception e) {
			logger.log(Level.WARNING,"Error running Spark application: ", e);
			response.setDetails(Utils.getExceptionAsString(e));
		}
		response.setCode(DiscoveryServiceResponse.ResponseCode.UNKNOWN_ERROR);
		return response;
	}

	@Override
	public void setAnnotationStore(AnnotationStore annotationStore) {
		this.annotationStore = annotationStore;
	}
}
