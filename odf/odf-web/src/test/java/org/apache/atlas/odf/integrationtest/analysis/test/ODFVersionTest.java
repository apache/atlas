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
package org.apache.atlas.odf.integrationtest.analysis.test;

import java.io.InputStream;

import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.rest.test.RestTestBase;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.wink.json4j.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class ODFVersionTest extends RestTestBase {

	@Test
	public void testVersion() throws Exception {
		Executor exec = getRestClientManager().getAuthenticatedExecutor();
		Request req = Request.Get(RestTestBase.getBaseURI() + "/engine/version");
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();

		String s = Utils.getInputStreamAsString(is, "UTF-8");
		logger.info("Version request returned: " + s);

		JSONObject jo = new JSONObject(s);
		String version = jo.getString("version");
		Assert.assertNotNull(version);
		Assert.assertTrue(version.startsWith("1.2.0-"));
	}

}
