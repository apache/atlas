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
package org.apache.atlas.odf.rest.test;

import java.io.InputStream;
import java.net.URI;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.BeforeClass;

import org.apache.atlas.odf.core.Encryption;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.api.analysis.AnalysisRequestTrackers;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus;
import org.apache.atlas.odf.api.analysis.AnalysisRequestSummary;
import org.apache.atlas.odf.api.analysis.AnalysisResponse;
import org.apache.atlas.odf.api.annotation.Annotations;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus.State;
import org.apache.atlas.odf.api.connectivity.RESTClientManager;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.api.utils.ODFLogConfig;
import org.apache.atlas.odf.core.test.TestEnvironment;
import org.apache.atlas.odf.json.JSONUtils;

public class RestTestBase {

	protected static Logger logger = Logger.getLogger(RestTestBase.class.getName());

	@BeforeClass
	public static void setup() throws Exception {
		ODFLogConfig.run();
		TestEnvironment.startMessaging();
	}
	
	protected static void checkResult(HttpResponse httpResponse, int expectedCode) {
		StatusLine sl = httpResponse.getStatusLine();
		int code = sl.getStatusCode();
		logger.info("Http request returned: " + code + ", message: " + sl.getReasonPhrase());
		Assert.assertEquals(expectedCode, code);
	}

	public static RESTClientManager getRestClientManager() {
		return new RESTClientManager(URI.create(getOdfUrl()), getOdfUser(), Encryption.decryptText(getOdfPassword()));
	}

	public static String getOdfBaseUrl() {
		String odfBaseURL = System.getProperty("odf.test.base.url");
		return odfBaseURL;
	}

	public static String getOdfUrl() {
		String odfURL = System.getProperty("odf.test.webapp.url");
		return odfURL;
	}

	public static String getOdfUser() {
		String odfUser = System.getProperty("odf.test.user");
		return odfUser;
	}

	public static String getOdfPassword() {
		String odfPassword = System.getProperty("odf.test.password");
		return odfPassword;
	}

	public static String getBaseURI() {
		return getOdfBaseUrl() + "/odf/api/v1";
	}

	public String runAnalysis(AnalysisRequest request, State expectedState) throws Exception {
		Executor exec = getRestClientManager().getAuthenticatedExecutor();
		String json = JSONUtils.toJSON(request);
		logger.info("Starting analysis via POST request: " + json);

		Header header = new BasicHeader("Content-Type", "application/json");
		Request req = Request.Post(getBaseURI() + "/analyses").bodyString(json, ContentType.APPLICATION_JSON).addHeader(header);

		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		checkResult(httpResp, HttpStatus.SC_OK);

		InputStream is = httpResp.getEntity().getContent();
		String jsonResponse = JSONUtils.getInputStreamAsString(is, "UTF-8");
		logger.info("Analysis response: " + jsonResponse);
		AnalysisResponse analysisResponse = JSONUtils.fromJSON(jsonResponse, AnalysisResponse.class);
		Assert.assertNotNull(analysisResponse);
		String requestId = analysisResponse.getId();
		Assert.assertNotNull(requestId);
		logger.info("Request Id: " + requestId);

		Assert.assertTrue(! analysisResponse.isInvalidRequest());
		
		AnalysisRequestStatus status = null;
		int maxPolls = 400;
		do {
			Request statusRequest = Request.Get(getBaseURI() + "/analyses/" + requestId);
			logger.info("Getting analysis status");
			resp = exec.execute(statusRequest);
			httpResp = resp.returnResponse();
			checkResult(httpResp, HttpStatus.SC_OK);

			String statusResponse = JSONUtils.getInputStreamAsString(httpResp.getEntity().getContent(), "UTF-8");
			logger.info("Analysis status: " + statusResponse);
			status = JSONUtils.fromJSON(statusResponse, AnalysisRequestStatus.class);

			logger.log(Level.INFO, "Poll request for request ID ''{0}'' (expected state: ''{1}'', details: ''{2}''", new Object[] { requestId, status.getState(), status.getDetails(), State.FINISHED });
			maxPolls--;
			Thread.sleep(1000);
		} while (maxPolls > 0 && (status.getState() == State.ACTIVE || status.getState() == State.QUEUED));
		Assert.assertEquals(State.FINISHED, status.getState());
		return requestId;
	}

	public void createService(String serviceJSON, int expectedCode) throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Header header = new BasicHeader("Content-Type", "application/json");

		Request req = Request.Post(RestTestBase.getBaseURI() + "/services")//
				.bodyString(serviceJSON, ContentType.APPLICATION_JSON) //
		.addHeader(header);
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();
		String s = Utils.getInputStreamAsString(is, "UTF-8");
		is.close();
		logger.info("Create service request return code: " + httpResp.getStatusLine().getStatusCode() + ", content: " + s);
		checkResult(httpResp, expectedCode);
	}
	
	public void checkServiceExists(String serviceId) throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Header header = new BasicHeader("Content-Type", "application/json");

		Request req = Request.Get(RestTestBase.getBaseURI() + "/services/" + serviceId).addHeader(header);
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();
		String s = Utils.getInputStreamAsString(is, "UTF-8");
		is.close();
		logger.info("Get service request return code: " + httpResp.getStatusLine().getStatusCode() + ", content: " + s);
		checkResult(httpResp, 200);
		
	}

	public void deleteService(String serviceId, int expectedCode) throws Exception {
		checkResult(this.deleteService(serviceId), expectedCode);
	}

	public HttpResponse deleteService(String serviceId) throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Header header = new BasicHeader("Content-Type", "application/json");
		URIBuilder uri = new URIBuilder(RestTestBase.getBaseURI() + "/services/" + serviceId + "/cancel");
		Request req = Request.Post(uri.build())//
				.addHeader(header);
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();
		String s = Utils.getInputStreamAsString(is, "UTF-8");
		is.close();
		logger.info("Delete service request returned: " + s);
		return httpResp;
	}

	public ODFSettings settingsRead() throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Request req = Request.Get(RestTestBase.getBaseURI() + "/settings");
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();

		String s = Utils.getInputStreamAsString(is, "UTF-8");
		logger.info("Settings read request returned: " + s);
		is.close();
		checkResult(httpResp, HttpStatus.SC_OK);
		return JSONUtils.fromJSON(s, ODFSettings.class);
	}

	public void settingsWrite(String configSnippet, int expectedCode) throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Header header = new BasicHeader("Content-Type", "application/json");

		Request req = Request.Put(RestTestBase.getBaseURI() + "/settings")//
				.bodyString(configSnippet, ContentType.APPLICATION_JSON) //
		.addHeader(header);
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();
		String s = Utils.getInputStreamAsString(is, "UTF-8");
		is.close();
		logger.info("Settings write request returned: " + s);
		checkResult(httpResp, expectedCode);
	}

	public void settingsReset() throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Header header = new BasicHeader("Content-Type", "application/json");
		Request req = Request.Post(RestTestBase.getBaseURI() + "/settings/reset")//
		.addHeader(header);
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();
		String s = Utils.getInputStreamAsString(is, "UTF-8");
		is.close();
		logger.info("Config reset request returned: " + s);
		checkResult(httpResp, HttpStatus.SC_OK);
	}

	public void cancelAnalysisRequest(String requestId, int expectedCode) throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Header header = new BasicHeader("Content-Type", "application/json");

		Request req = Request.Post(RestTestBase.getBaseURI() + "/analyses/" + requestId + "/cancel").addHeader(header);
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();
		String s = Utils.getInputStreamAsString(is, "UTF-8");
		is.close();
		logger.info("Cancel analyses request returned: " + s);
		checkResult(httpResp, expectedCode);
	}

	public AnalysisRequestTrackers getAnalysesRequests(int offset, int limit) throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Request req = Request.Get(MessageFormat.format("{0}/analyses?offset={1}&limit={2}", RestTestBase.getBaseURI(), offset, limit));
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();

		String s = Utils.getInputStreamAsString(is, "UTF-8");
		logger.info("Analyses read request returned: " + s);
		is.close();
		checkResult(httpResp, HttpStatus.SC_OK);
		return JSONUtils.fromJSON(s, AnalysisRequestTrackers.class);
	}

	public AnalysisRequestSummary getAnalysesStats() throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		Request req = Request.Get(RestTestBase.getBaseURI() + "/analyses/stats");
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();

		String s = Utils.getInputStreamAsString(is, "UTF-8");
		logger.info("Analyses statistics request returned: " + s);
		is.close();
		checkResult(httpResp, HttpStatus.SC_OK);
		return JSONUtils.fromJSON(s, AnalysisRequestSummary.class);
	}

	public Annotations getAnnotations(String analysisRequestId) throws Exception {
		Executor exec = RestTestBase.getRestClientManager().getAuthenticatedExecutor();
		URIBuilder uri = new URIBuilder(RestTestBase.getBaseURI() + "/annotations").addParameter("analysisRequestId", analysisRequestId);
		Request req = Request.Get(uri.build());
		Response resp = exec.execute(req);
		HttpResponse httpResp = resp.returnResponse();
		InputStream is = httpResp.getEntity().getContent();

		String s = Utils.getInputStreamAsString(is, "UTF-8");
		logger.info("Settings read request returned: " + s);
		is.close();
		checkResult(httpResp, HttpStatus.SC_OK);
		return JSONUtils.fromJSON(s, Annotations.class);
	}
}
