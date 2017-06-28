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

import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.json.JSONUtils;

public class SparkServiceRuntime implements ServiceRuntime {

	public static final String SPARK_RUNTIME_NAME = "Spark";
	
	@Override
	public String getName() {
		return SPARK_RUNTIME_NAME;
	}

	@Override
	public long getWaitTimeUntilAvailable() {
		return 0;
	}

	@Override
	public DiscoveryService createDiscoveryServiceProxy(DiscoveryServiceProperties props) {
		return new SparkDiscoveryServiceProxy(props);
	}

	@Override
	public String getDescription() {
		return "The default Spark runtime";
	}

	@Override
	public void validate(DiscoveryServiceProperties props) throws ValidationException {
		try {
			JSONUtils.convert(props.getEndpoint(),  DiscoveryServiceSparkEndpoint.class);
		} catch (JSONException e1) {
			throw new ValidationException("Endpoint definition for Spark service is not correct: " + Utils.getExceptionAsString(e1));
		}
	}

}
