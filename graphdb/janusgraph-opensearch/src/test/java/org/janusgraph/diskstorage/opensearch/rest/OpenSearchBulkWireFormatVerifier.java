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
package org.janusgraph.diskstorage.opensearch.rest;

import com.google.common.collect.ImmutableMap;
import org.apache.hc.core5.http.HttpHost;
import org.janusgraph.diskstorage.opensearch.OpenSearchMutation;
import org.opensearch.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * C4.1 bulk wire-format checks: typeless metadata and retry_on_conflict on updates.
 */
public final class OpenSearchBulkWireFormatVerifier {

    private OpenSearchBulkWireFormatVerifier() {
    }

    public static void verify(String host, int port) throws Exception {
        try (RestClient restClient = RestClient.builder(new HttpHost("http", host, port)).build();
             RestOpenSearchClient client = new RestOpenSearchClient(
                     restClient, 60, false, 0, Collections.emptySet(), 1L, 1000L, 100_000_000)) {

            client.setRetryOnConflict(3);

            Map<String, Object> source = ImmutableMap.of("name", "Alice", "age", 30);
            OpenSearchMutation indexMutation = OpenSearchMutation.createIndexRequest(
                    "c4janus_c4mixed", "c4mixed", "doc-1", source);
            RestOpenSearchClient.RequestBytes indexBytes = client.new RequestBytes(indexMutation);
            String indexLine = new String(indexBytes.requestBytes, StandardCharsets.UTF_8);
            assertNoType(indexLine, "index");
            if (!indexLine.contains("\"_id\":\"doc-1\"")) {
                throw new IllegalStateException("Index bulk metadata missing _id: " + indexLine);
            }
            System.out.println("[OK] C4.1 bulk index metadata is typeless: " + indexLine.trim());

            OpenSearchMutation updateMutation = OpenSearchMutation.createUpdateRequest(
                    "c4janus_c4mixed", "c4mixed", "doc-1", ImmutableMap.of("doc", ImmutableMap.of("age", 31)));
            RestOpenSearchClient.RequestBytes updateBytes = client.new RequestBytes(updateMutation);
            String updateLine = new String(updateBytes.requestBytes, StandardCharsets.UTF_8);
            assertNoType(updateLine, "update");
            if (!updateLine.contains("retry_on_conflict")) {
                throw new IllegalStateException("Update bulk metadata missing retry_on_conflict: " + updateLine);
            }
            if (updateLine.contains("_retry_on_conflict")) {
                throw new IllegalStateException("Update bulk metadata uses legacy _retry_on_conflict: " + updateLine);
            }
            System.out.println("[OK] C4.1 bulk update uses retry_on_conflict: " + updateLine.trim());

            OpenSearchMutation deleteMutation = OpenSearchMutation.createDeleteRequest(
                    "c4janus_c4mixed", "c4mixed", "doc-1");
            RestOpenSearchClient.RequestBytes deleteBytes = client.new RequestBytes(deleteMutation);
            String deleteLine = new String(deleteBytes.requestBytes, StandardCharsets.UTF_8);
            assertNoType(deleteLine, "delete");
            System.out.println("[OK] C4.1 bulk delete metadata is typeless: " + deleteLine.trim());
        }
    }

    private static void assertNoType(String bulkMetadataLine, String operation) {
        if (bulkMetadataLine.contains("_type")) {
            throw new IllegalStateException(
                    "Bulk " + operation + " metadata must not contain _type: " + bulkMetadataLine);
        }
    }
}
