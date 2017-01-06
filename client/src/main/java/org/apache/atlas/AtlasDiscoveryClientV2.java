/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import static org.apache.atlas.AtlasClient.LIMIT;
import static org.apache.atlas.AtlasClient.OFFSET;
import static org.apache.atlas.AtlasClient.QUERY;

public class AtlasDiscoveryClientV2 extends AtlasBaseClient {

    private static final String DISCOVERY_URI = BASE_URI + "v2/search";
    private static final String DSL_URI       = DISCOVERY_URI + "/dsl";
    private static final String FULL_TEXT_URI = DISCOVERY_URI + "/fulltext";

    private static final APIInfo DSL_SEARCH       = new APIInfo(DSL_URI, HttpMethod.GET, Response.Status.OK);
    private static final APIInfo FULL_TEXT_SEARCH = new APIInfo(FULL_TEXT_URI, HttpMethod.GET, Response.Status.OK);

    public AtlasDiscoveryClientV2(String[] baseUrl, String[] basicAuthUserNamePassword) {
        super(baseUrl, basicAuthUserNamePassword);
    }

    public AtlasDiscoveryClientV2(String... baseUrls) throws AtlasException {
        super(baseUrls);
    }

    public AtlasDiscoveryClientV2(UserGroupInformation ugi, String doAsUser, String... baseUrls) {
        super(ugi, doAsUser, baseUrls);
    }

    protected AtlasDiscoveryClientV2() {
        super();
    }

    @VisibleForTesting
    AtlasDiscoveryClientV2(WebResource service, Configuration configuration) {
        super(service, configuration);
    }

    public AtlasSearchResult dslSearch(final String query) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(QUERY, query);

        return callAPI(DSL_SEARCH, AtlasSearchResult.class, queryParams);
    }

    public AtlasSearchResult dslSearchWithParams(final String query, final int limit, final int offset) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(QUERY, query);
        queryParams.add(LIMIT, String.valueOf(limit));
        queryParams.add(OFFSET, String.valueOf(offset));

        return callAPI(DSL_SEARCH, AtlasSearchResult.class, queryParams);
    }

    public AtlasSearchResult fullTextSearch(final String query) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(QUERY, query);

        return callAPI(FULL_TEXT_SEARCH, AtlasSearchResult.class, queryParams);
    }

    public AtlasSearchResult fullTextSearchWithParams(final String query, final int limit, final int offset) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(QUERY, query);
        queryParams.add(LIMIT, String.valueOf(limit));
        queryParams.add(OFFSET, String.valueOf(offset));

        return callAPI(FULL_TEXT_SEARCH, AtlasSearchResult.class, queryParams);
    }
}