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
package org.apache.atlas.odf.api.metadata;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.connectivity.RESTClientManager;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;

public class RESTMetadataStoreHelper {

	static Logger logger = Logger.getLogger(RESTMetadataStoreHelper.class.getName());

	/**
	 * Return a ConnectionStatus object assuming that the URI is static in the sense that
	 * the metadata store is unreachable if the URI cannot be reached.
	 */
	public static MetadataStore.ConnectionStatus testConnectionForStaticURL(RESTClientManager client, String uri) {
		try {
			Response resp = client.getAuthenticatedExecutor().execute(Request.Get(uri));
			HttpResponse httpResponse = resp.returnResponse();
			switch (httpResponse.getStatusLine().getStatusCode()) {
			case HttpStatus.SC_NOT_FOUND:
				return MetadataStore.ConnectionStatus.UNREACHABLE;
			case HttpStatus.SC_OK:
				return MetadataStore.ConnectionStatus.OK;
			default:
				;
			}
		} catch (Exception e) {
			logger.log(Level.INFO, "Connection failed", e);
		}
		return MetadataStore.ConnectionStatus.UNKOWN_ERROR;
	}

}
