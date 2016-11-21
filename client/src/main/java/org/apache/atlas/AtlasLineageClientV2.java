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
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

public class AtlasLineageClientV2 extends AtlasBaseClient {

    private static final String  LINEAGE_URI  = BASE_URI + "v2/lineage/%s/";
    private static final APIInfo LINEAGE_INFO = new APIInfo(LINEAGE_URI, HttpMethod.GET, Response.Status.OK);

    public AtlasLineageClientV2(String[] baseUrl, String[] basicAuthUserNamePassword) {
        super(baseUrl, basicAuthUserNamePassword);
    }

    public AtlasLineageClientV2(String... baseUrls) throws AtlasException {
        super(baseUrls);
    }

    public AtlasLineageClientV2(UserGroupInformation ugi, String doAsUser, String... baseUrls) {
        super(ugi, doAsUser, baseUrls);
    }

    protected AtlasLineageClientV2() {
        super();
    }

    @VisibleForTesting
    AtlasLineageClientV2(WebResource service, Configuration configuration) {
        super(service, configuration);
    }

    public AtlasLineageInfo getLineageInfo(final String guid, final LineageDirection direction, final int depth) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("direction", direction.toString());
        queryParams.add("depth", String.valueOf(depth));

        return callAPI(formatPathForPathParams(LINEAGE_INFO, guid), AtlasLineageInfo.class, queryParams);
    }
}