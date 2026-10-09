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

import javax.ws.rs.core.Response;

public class AtlasServiceException extends Exception {
    private final Response.Status status;

    public AtlasServiceException(AtlasBaseClient.API api, Exception e) {
        super("Metadata service API " + api.getMethod() + " : " + api.getNormalizedPath() + " failed", e);

        this.status = Response.Status.BAD_REQUEST;
    }

    private AtlasServiceException(AtlasBaseClient.API api, Response.Status status, String response) {
        super("Metadata service API " + api + " failed with status " + (status != null ? status.getStatusCode() : -1) + " (" + status + ") Response Body (" + response + ")");

        this.status = status;
    }

    public AtlasServiceException(AtlasBaseClient.API api, Response response) {
        this(api, Response.Status.fromStatusCode(response.getStatus()), response.readEntity(String.class));
    }

    public AtlasServiceException(Exception e) {
        super(e);

        this.status = Response.Status.BAD_REQUEST;
    }

    public AtlasServiceException(AtlasServiceException e) {
        super(e);

        this.status = e.status;
    }

    public Response.Status getStatus() {
        return status;
    }
}
