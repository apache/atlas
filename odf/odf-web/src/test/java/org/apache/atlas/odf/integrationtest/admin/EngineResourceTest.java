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
package org.apache.atlas.odf.integrationtest.admin;

import java.io.InputStream;
import java.util.Collection;

import org.apache.atlas.odf.rest.test.RestTestBase;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.engine.ServiceRuntimeInfo;
import org.apache.atlas.odf.api.engine.ServiceRuntimesInfo;
import org.apache.atlas.odf.api.engine.SystemHealth;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.json.JSONUtils;

public class EngineResourceTest extends RestTestBase {

	@Test
	public void testHealth() throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Request req = Request.Get(RestTestBase.getBaseURI() + "/engine/health");
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();

		String s = Utils.getInputStreamAsString(is, "UTF-8");
		logger.info("Health check request returned: " + s);
		checkResult(httpResp, HttpStatus.SC_OK);
		SystemHealth health = JSONUtils.fromJSON(s, SystemHealth.class);
		Assert.assertNotNull(health);
	}
	
	boolean containsRuntimeWithName(Collection<ServiceRuntimeInfo> runtimes, String name) {
		for (ServiceRuntimeInfo sri : runtimes) {
			if (name.equals(sri.getName())) {
				return true;
			}
		}
		return false;
	}
	
	@Test
	public void testRuntimesInfo() throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Request req = Request.Get(RestTestBase.getBaseURI() + "/engine/runtimes");
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();

		String s = Utils.getInputStreamAsString(is, "UTF-8");
		logger.info("Runtime Info returned: " + s);
		checkResult(httpResp, HttpStatus.SC_OK);
		ServiceRuntimesInfo sri = JSONUtils.fromJSON(s, ServiceRuntimesInfo.class);
		Assert.assertNotNull(sri);
		Assert.assertTrue(sri.getRuntimes().size() > 2);
		Assert.assertTrue(containsRuntimeWithName(sri.getRuntimes(), "Java"));
		Assert.assertTrue(containsRuntimeWithName(sri.getRuntimes(), "Spark"));
		Assert.assertTrue(containsRuntimeWithName(sri.getRuntimes(), "HealthCheck"));

	}
}
