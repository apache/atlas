// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.opensearch;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static org.janusgraph.diskstorage.opensearch.OpenSearchConstants.OS_UPSERT_KEY;

public class OpenSearchMutation {

    public enum RequestType {

        INDEX,

        UPDATE,

        DELETE

    }

    private final RequestType requestType;

    private final String index;

    private final String type;

    private final String id;

    private final Map source;

    private OpenSearchMutation(RequestType requestType, String index, String type, String id, Map source) {
        this.requestType = requestType;
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
    }

    public static OpenSearchMutation createDeleteRequest(String index, String type, String id) {
        return new OpenSearchMutation(RequestType.DELETE, index, type, id, null);
    }

    public static OpenSearchMutation createIndexRequest(String index, String type, String id, Map source) {
        return new OpenSearchMutation(RequestType.INDEX, index, type, id, source);
    }

    public static OpenSearchMutation createUpdateRequest(String index, String type, String id, Map source) {
        return new OpenSearchMutation(RequestType.UPDATE, index, type, id, source);
    }

    public static OpenSearchMutation createUpdateRequest(String index, String type, String id, ImmutableMap.Builder builder, Map upsert) {
        final Map source = upsert == null ? builder.build() : builder.put(OS_UPSERT_KEY, upsert).build();
        return new OpenSearchMutation(RequestType.UPDATE, index, type, id, source);
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Map getSource() {
        return source;
    }

}
