/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.regression.request;

import org.apache.atlas.regression.security.AtlasAuthenticationToken;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class BaseRequest {
    private static final Logger LOGGER = Logger.getLogger(BaseRequest.class);

    private String method;
    private String url;
    private List<Header> headers;
    private String requestData;
    private String user;
    private URI uri;
    private HttpHost target;
    private static final SSLSocketFactory SSL_SOCKET_FACTORY;

    static {
        try {
            SSLContext ssl = getSslContext();
            SSL_SOCKET_FACTORY = new SSLSocketFactory(ssl, new AllowAllHostnameVerifier());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static SSLContext getSslContext() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(
                null,
                new TrustManager[]{TrustManagerUtils.getValidateServerCertificateTrustManager()},
                new SecureRandom());
        return sslContext;
    }

    public BaseRequest(String url) throws URISyntaxException {
        this(url, "get", null, null);
    }

    public BaseRequest(String url, String method, String user) throws URISyntaxException {
        this(url, method, user, null);
    }

    public BaseRequest(String url, String method, String user, String data)
            throws URISyntaxException {
        this.method = method;
        this.url = url;
        this.requestData = null;
        this.user = (null == user) ? RequestKeys.CURRENT_USER : user;
        this.uri = new URI(url);
        target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
        this.headers = new ArrayList<Header>();
        this.requestData = data;
    }

    public void addHeader(String name, String value) {
        headers.add(new BasicHeader(name, value));
    }

    public HttpResponse run() throws URISyntaxException, IOException, AuthenticationException,
            InterruptedException {
        URIBuilder uriBuilder = new URIBuilder(this.url);

        /*falcon now reads a user.name parameter in the request.
        by default we will add it to every request.*/
        uriBuilder.addParameter(PseudoAuthenticator.USER_NAME, this.user);
        uri = uriBuilder.build();
        this.url=uri.toString();
        // process the get
        if (this.method.equalsIgnoreCase("get")) {
            return execute(new HttpGet(this.url));
        } else if (this.method.equalsIgnoreCase("delete")) {
            return execute(new HttpDelete(this.url));
        }

        HttpEntityEnclosingRequest request = null;
        if (this.method.equalsIgnoreCase("post")) {
            request = new HttpPost(new URI(this.url));
        } else if (this.method.equalsIgnoreCase("put")) {
            request = new HttpPut(new URI(this.url));
        } else {
            throw new IOException("Unknown method: " + method);
        }
        if (this.requestData != null) {
            request.setEntity(new StringEntity(requestData));
        }
        return execute(request);
    }

    private HttpResponse execute(HttpRequest request)
            throws IOException, AuthenticationException, InterruptedException {
        // add headers to the request
        if (null != headers && headers.size() > 0) {
            for (Header header : headers) {
                request.addHeader(header);
            }
        }

        HttpClient client;
        if (uri.toString().startsWith("https")) {
            SchemeRegistry schemeRegistry = new SchemeRegistry();
            schemeRegistry.register(new Scheme("https", uri.getPort(), SSL_SOCKET_FACTORY));
            BasicClientConnectionManager cm = new BasicClientConnectionManager(schemeRegistry);
            client = new DefaultHttpClient(cm);
        } else {
            client = new DefaultHttpClient();
        }
        LOGGER.info("Request Url: " + request.getRequestLine().getUri());
        LOGGER.info("Request Method: " + request.getRequestLine().getMethod());

        for (Header header : request.getAllHeaders()) {
            LOGGER.info(String.format("Request Header: Name=%s Value=%s", header.getName(),
                    header.getValue()));
        }
        HttpResponse response = client.execute(target, request);
        // add token to the request in case we get a 401 back with negotiate.
        if ((response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED)) {
            Header[] wwwAuthHeaders = response.getHeaders(RequestKeys.WWW_AUTHENTICATE);
            if (wwwAuthHeaders != null && wwwAuthHeaders.length != 0
                    && wwwAuthHeaders[0].getValue().trim().startsWith(RequestKeys.NEGOTIATE)) {
                AuthenticatedURL.Token token = AtlasAuthenticationToken.getToken(user, uri
                                .getScheme(),
                        uri.getHost(), uri.getPort(), true);
                request.removeHeaders(RequestKeys.COOKIE);
                request.addHeader(RequestKeys.COOKIE, RequestKeys.AUTH_COOKIE_EQ + token);
                LOGGER.info("Request Url: " + request.getRequestLine().getUri());
                LOGGER.info("Request Method: " + request.getRequestLine().getMethod());
                for (Header header : request.getAllHeaders()) {
                    LOGGER.info(String.format("Request Header: Name=%s Value=%s", header.getName(),
                            header.getValue()));
                }
                response = client.execute(target, request);
            }
        }
        LOGGER.info("Response Status: " + response.getStatusLine());
        for (Header header : response.getAllHeaders()) {
            LOGGER.info(String.format("Response Header: Name=%s Value=%s", header.getName(),
                    header.getValue()));
        }
        return response;
    }
}
