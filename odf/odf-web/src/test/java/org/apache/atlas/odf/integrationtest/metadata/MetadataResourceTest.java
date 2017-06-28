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
package org.apache.atlas.odf.integrationtest.metadata;

import java.io.InputStream;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.rest.test.RestTestBase;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.metadata.models.BusinessTerm;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.json.JSONUtils;

public class MetadataResourceTest extends RestTestBase {

	static Logger logger = Logger.getLogger(MetadataResourceTest.class.getName());

	@Before
	public void createSampleData() throws Exception {
		Executor exec = getRestClientManager().getAuthenticatedExecutor();
		Request req = Request.Get(getBaseURI() + "/metadata/sampledata");
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		checkResult(httpResp, HttpStatus.SC_OK);
	}

	public static String getAllMetadataObjectsOfType(String dataType) throws Exception {
		MetadataStore mdsForQueryGeneration = new ODFFactory().create().getMetadataStore();
		String query = mdsForQueryGeneration.newQueryBuilder().objectType(dataType).build();
		logger.info("Metadata search query metadata " + query);

		URIBuilder builder = new URIBuilder(getBaseURI() + "/metadata/search").addParameter("query", query);
		String uri = builder.build().toString();
		logger.info("Searching against URL: " + uri);
		Request req = Request.Get(uri);
		Response response = getRestClientManager().getAuthenticatedExecutor().execute(req);
		HttpResponse httpResp = response.returnResponse();
		Assert.assertEquals(HttpStatus.SC_OK, httpResp.getStatusLine().getStatusCode());
		InputStream is = httpResp.getEntity().getContent();
		String s = JSONUtils.getInputStreamAsString(is, "UTF-8");
		is.close();
		logger.info("Response: " + s);
		return s;
	}

	@Test
	public void testMetadataResourceSearchOMDataFile() throws Exception {
		String s = getAllMetadataObjectsOfType("DataFile");
		Assert.assertTrue(s.contains("DataFile")); // minimal checking that JSON contains something useful and specific to this type
		JSONUtils.fromJSONList(s, DataFile.class);
	}

	@Test
	public void testMetadataResourceSearchOMBusinessTerm() throws Exception {
		String s = getAllMetadataObjectsOfType("BusinessTerm");
		Assert.assertTrue(s.contains("BusinessTerm")); // minimal checking that JSON contains something useful and specific to this type
		JSONUtils.fromJSONList(s, BusinessTerm.class);
	}
}
