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
package org.janusgraph.diskstorage.opensearch.smoke;

import org.apache.hc.core5.http.HttpHost;
import org.janusgraph.diskstorage.opensearch.OpenSearchMajorVersion;
import org.janusgraph.diskstorage.opensearch.rest.RestOpenSearchClient;
import org.opensearch.client.RestClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * Standalone smoke driver for OpenSearch REST client initialization (2.x and 3.x).
 *
 * Run after starting OpenSearch on localhost:9200:
 * <pre>
 *   mvn -pl graphdb/janusgraph-opensearch test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=org.janusgraph.diskstorage.opensearch.smoke.OpenSearchInitSmokeDriver
 * </pre>
 */
public final class OpenSearchInitSmokeDriver {

    private static final String OPENSEARCH_HOST = System.getProperty("opensearch.host", "localhost");
    private static final int    OPENSEARCH_PORT = Integer.getInteger("opensearch.port", 9200);

    private OpenSearchInitSmokeDriver() {
    }

    public static void main(String[] args) throws Exception {
        verifyOpenSearchReachable();
        String versionNumber = verifyVersionParsing();

        try (RestClient restClient = RestClient.builder(new HttpHost("http", OPENSEARCH_HOST, OPENSEARCH_PORT)).build();
             RestOpenSearchClient client = new RestOpenSearchClient(
                     restClient, 60, false, 0, Collections.emptySet(), 1L, 1000L, 100_000_000)) {

            OpenSearchMajorVersion version = client.getMajorVersion();
            if (OpenSearchMajorVersion.TWO != version && OpenSearchMajorVersion.THREE != version) {
                throw new IllegalStateException("RestOpenSearchClient detected " + version + " but expected TWO or THREE");
            }
            System.out.println("[OK] RestOpenSearchClient.getMajorVersion(): " + version);

            client.clusterHealthRequest("30s");
            System.out.println("[OK] GET /_cluster/health?wait_for_status=yellow&timeout=30s");
        }

        System.out.println("OpenSearch " + versionNumber + " runtime initialization smoke test passed.");
    }

    private static void verifyOpenSearchReachable() throws IOException {
        URL url = new URL("http://" + OPENSEARCH_HOST + ":" + OPENSEARCH_PORT + "/");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() != 200) {
            throw new IOException("OpenSearch GET / returned HTTP " + connection.getResponseCode());
        }
        System.out.println("[OK] OpenSearch GET / reachable at " + OPENSEARCH_HOST + ":" + OPENSEARCH_PORT);
    }

    private static String verifyVersionParsing() throws IOException {
        URL url = new URL("http://" + OPENSEARCH_HOST + ":" + OPENSEARCH_PORT + "/");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("GET");
        try (java.io.InputStream in = connection.getInputStream()) {
            String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            int numberStart = body.indexOf("\"number\" : \"") + 12;
            int numberEnd = body.indexOf('"', numberStart);
            String number = body.substring(numberStart, numberEnd);
            OpenSearchMajorVersion version = OpenSearchMajorVersion.parse(number);
            if (OpenSearchMajorVersion.TWO != version && OpenSearchMajorVersion.THREE != version) {
                throw new IllegalStateException("Expected TWO or THREE from version.number but got " + version);
            }
            System.out.println("[OK] OpenSearchMajorVersion.parse(\"" + number + "\") -> " + version);
            return number;
        }
    }
}
