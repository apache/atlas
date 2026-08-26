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
package org.apache.atlas.discovery.smoke;

import com.google.common.collect.ImmutableMap;
import org.apache.atlas.ApplicationProperties;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import java.io.IOException;
import java.util.Properties;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.io.OutputStream;
import java.lang.reflect.Field;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.opensearch.OpenSearchMajorVersion;
import sun.misc.Unsafe;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared helpers for C5.2 Atlas quick-search validation against a live OpenSearch cluster.
 */
final class OpenSearchQuickSearchSmokeSupport {

    static final String GRAPH_INDEX_NAME   = "c5janus";
    static final String BACKING_INDEX_NAME = "search";
    static final String VERTEX_INDEX       = "vertex_index";
    static final String PHYSICAL_INDEX     = GRAPH_INDEX_NAME + "_" + VERTEX_INDEX;

    /** Must match {@link org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase} registration. */
    static final String OPENSEARCH_INDEX_PROVIDER =
            "org.janusgraph.diskstorage.opensearch.AtlasOpenSearchIndex";

    private OpenSearchQuickSearchSmokeSupport() {
    }

    static String getOpenSearchHost() {
        return System.getProperty("opensearch.host", "localhost");
    }

    static int getOpenSearchPort() {
        return Integer.getInteger("opensearch.port", 9200);
    }

    static void bootstrapApplicationProperties() throws Exception {
        System.setProperty(ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY, "src/test/resources");
        System.setProperty(ApplicationProperties.ATLAS_PROPERTIES_FILENAME_SYSTEM_CONF, "atlas-opensearch-c5-application.properties");
        System.setProperty("atlas.graph.index.search.hostname", getOpenSearchHost());
        System.setProperty("atlas.graph.index.search.port", String.valueOf(getOpenSearchPort()));
        ApplicationProperties.forceReload();
    }

    static void verifyOpenSearchReachable() throws IOException {
        URL url = new URL("http://" + getOpenSearchHost() + ":" + getOpenSearchPort() + "/");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() != 200) {
            throw new IOException("OpenSearch GET / returned HTTP " + connection.getResponseCode());
        }
    }

    static String readOpenSearchVersion() throws IOException {
        URL url = new URL("http://" + getOpenSearchHost() + ":" + getOpenSearchPort() + "/");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("GET");
        try (java.io.InputStream in = connection.getInputStream()) {
            String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            int numberStart = body.indexOf("\"number\" : \"") + 12;
            int numberEnd   = body.indexOf('"', numberStart);
            return body.substring(numberStart, numberEnd);
        }
    }

    static OpenSearchMajorVersion readOpenSearchMajorVersion() throws IOException {
        return OpenSearchMajorVersion.parse(readOpenSearchVersion());
    }

    /**
     * JanusGraph configuration aligned with {@code atlas-opensearch-c5-application.properties}.
     * Avoids {@link org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase} static init
     * so the driver can run on JDK versions where that init requires extra {@code --add-opens} flags.
     */
    static ReadConfiguration buildJanusGraphConfiguration() throws IOException {
        java.io.File dataDir = java.io.File.createTempFile("c5-quicksearch-", "");
        if (!dataDir.delete() || !dataDir.mkdir()) {
            throw new IOException("Failed to create temp data directory for JanusGraph");
        }
        dataDir.deleteOnExit();

        Properties properties = new Properties();
        properties.setProperty("storage.backend", "berkeleyje");
        properties.setProperty("storage.directory", new java.io.File(dataDir, "berkeley").getAbsolutePath());
        properties.setProperty("index.search.backend", "opensearch");
        properties.setProperty("index." + BACKING_INDEX_NAME + ".backend", OPENSEARCH_INDEX_PROVIDER);
        properties.setProperty("index." + BACKING_INDEX_NAME + ".hostname", getOpenSearchHost());
        properties.setProperty("index." + BACKING_INDEX_NAME + ".port", String.valueOf(getOpenSearchPort()));
        properties.setProperty("index." + BACKING_INDEX_NAME + ".index-name", GRAPH_INDEX_NAME);
        properties.setProperty("index." + BACKING_INDEX_NAME + ".opensearch.setup-max-open-scroll-contexts", "false");
        properties.setProperty("index." + BACKING_INDEX_NAME + ".opensearch.bulk-refresh", "wait_for");
        properties.setProperty("index." + BACKING_INDEX_NAME + ".opensearch.retry_on_conflict", "3");
        properties.setProperty("index." + BACKING_INDEX_NAME + ".max-result-set-size", "500");

        org.apache.commons.configuration2.Configuration commons =
                org.apache.commons.configuration2.ConfigurationConverter.getConfiguration(properties);
        return new CommonsConfiguration(commons);
    }

    static void registerAtlasOpenSearchIndex() throws Exception {
        Class.forName(StandardIndexProvider.class.getName(), true, StandardIndexProvider.class.getClassLoader());
        Field field = StandardIndexProvider.class.getDeclaredField("ALL_MANAGER_CLASSES");
        Unsafe unsafe = obtainUnsafe();
        Object base   = unsafe.staticFieldBase(field);
        long offset   = unsafe.staticFieldOffset(field);
        @SuppressWarnings("unchecked")
        Map<String, String> current = (Map<String, String>) unsafe.getObject(base, offset);
        if (current == null) {
            throw new IllegalStateException("StandardIndexProvider.ALL_MANAGER_CLASSES is null");
        }
        Map<String, String> updated = new HashMap<>(current);
        updated.put("opensearch", OPENSEARCH_INDEX_PROVIDER);
        unsafe.putObject(base, offset, ImmutableMap.copyOf(updated));
    }

    static void deletePhysicalIndexIfPresent() throws IOException {
        URL url = new URL("http://" + getOpenSearchHost() + ":" + getOpenSearchPort() + "/" + PHYSICAL_INDEX);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("DELETE");
        connection.getResponseCode();
    }

    static long openSearchDocumentCount() throws IOException {
        String body = httpGet("/" + PHYSICAL_INDEX + "/_count");
        int valueStart = body.indexOf("\"count\":") + 8;
        int valueEnd   = body.indexOf(',', valueStart);
        if (valueEnd < 0) {
            valueEnd = body.indexOf('}', valueStart);
        }
        return Long.parseLong(body.substring(valueStart, valueEnd).trim());
    }

    static void debugOpenSearchSearch(String queryJson) throws IOException {
        System.out.println("[DEBUG] OpenSearch POST /" + PHYSICAL_INDEX + "/_search");
        System.out.println("[DEBUG] request body: " + queryJson);
        String response = httpPost("/" + PHYSICAL_INDEX + "/_search", queryJson);
        System.out.println("[DEBUG] response (truncated): "
                + (response.length() > 500 ? response.substring(0, 500) + "..." : response));
    }

    static String httpGet(String path) throws IOException {
        URL url = new URL("http://" + getOpenSearchHost() + ":" + getOpenSearchPort() + path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(15000);
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() != 200) {
            throw new IOException("GET " + path + " returned HTTP " + connection.getResponseCode());
        }
        try (java.io.InputStream in = connection.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static String httpPost(String path, String jsonBody) throws IOException {
        URL url = new URL("http://" + getOpenSearchHost() + ":" + getOpenSearchPort() + path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(15000);
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        byte[] bytes = jsonBody.getBytes(StandardCharsets.UTF_8);
        connection.setRequestProperty("Content-Length", String.valueOf(bytes.length));
        try (OutputStream out = connection.getOutputStream()) {
            out.write(bytes);
        }
        if (connection.getResponseCode() != 200) {
            throw new IOException("POST " + path + " returned HTTP " + connection.getResponseCode());
        }
        try (java.io.InputStream in = connection.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Unsafe obtainUnsafe() throws Exception {
        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        return (Unsafe) unsafeField.get(null);
    }
}
