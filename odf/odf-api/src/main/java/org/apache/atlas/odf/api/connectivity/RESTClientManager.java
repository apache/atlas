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
package org.apache.atlas.odf.api.connectivity;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;

import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.fluent.Executor;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;

/**
 * 
 * This is a helper class to authenticate http requests
 *
 */
public class RESTClientManager {

	Logger logger = Logger.getLogger(RESTClientManager.class.getName());

	private Executor executor = null;

	private URI baseUrl;
	private String user;
	private String password;

	public RESTClientManager(URI baseUrl, String user, String password) {
		this.baseUrl = baseUrl;
		this.user = user;
		this.password = password;
	}

	public RESTClientManager(URI baseUrl) {
		this(baseUrl, null, null);
	}

	public Executor getAuthenticatedExecutor() throws GeneralSecurityException {
		if (executor != null) {
			return executor;
		}
		// TODO always accept the certificate for now but do proper certificate stuff in the future 
		TrustStrategy acceptAllTrustStrategy = new TrustStrategy() {
			@Override
			public boolean isTrusted(X509Certificate[] certificate, String authType) {
				return true;
			}
		};
		SSLContextBuilder contextBuilder = new SSLContextBuilder();
		SSLContext context = contextBuilder.loadTrustMaterial(null, acceptAllTrustStrategy).build();
		SSLConnectionSocketFactory scsf = new SSLConnectionSocketFactory(context, new NoopHostnameVerifier());

		HttpClient httpClient = HttpClientBuilder.create() //
				.setSSLSocketFactory(scsf) //
				.build();

		if (this.user != null) {
			if (this.baseUrl == null) {
				executor = Executor.newInstance(httpClient).auth(new UsernamePasswordCredentials(this.user, this.password));
			} else {
				executor = Executor.newInstance(httpClient).auth(this.baseUrl.getHost(), new UsernamePasswordCredentials(this.user, this.password));
			}
		} else {
			executor = Executor.newInstance(httpClient);
		}
		return executor;
	}

}
