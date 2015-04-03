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

package org.apache.hadoop.metadata;

import com.sun.jersey.api.client.ClientResponse;

public class MetadataServiceException extends Exception {
    public MetadataServiceException(MetadataServiceClient.API api, Exception e) {
        super("Metadata service API " + api + " failed", e);
    }

    public MetadataServiceException(MetadataServiceClient.API api, ClientResponse.Status status) {
        super("Metadata service API " + api + " failed with status " + status.getStatusCode()
                + "(" + status.getReasonPhrase() + ")");
    }

    public MetadataServiceException(Exception e) {
        super(e);
    }
}
