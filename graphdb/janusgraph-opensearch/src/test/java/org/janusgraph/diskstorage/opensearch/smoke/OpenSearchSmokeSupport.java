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

import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

final class OpenSearchSmokeSupport {

    private OpenSearchSmokeSupport() {
    }

    static String getOpenSearchHost() {
        return System.getProperty("opensearch.host", "localhost");
    }

    static int getOpenSearchPort() {
        return Integer.getInteger("opensearch.port", 9200);
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
        System.out.println("[OK] OpenSearch reachable at " + getOpenSearchHost() + ":" + getOpenSearchPort());
    }

    static void deletePhysicalIndex(String physicalIndex) throws IOException {
        URL url = new URL("http://" + getOpenSearchHost() + ":" + getOpenSearchPort() + "/" + physicalIndex);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("DELETE");
        connection.getResponseCode();
        System.out.println("[INFO] Cleared physical index (if present): " + physicalIndex);
    }

    static String httpGet(String path) throws IOException {
        URL url = new URL("http://" + getOpenSearchHost() + ":" + getOpenSearchPort() + path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(10000);
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() != 200) {
            throw new IOException("GET " + path + " returned HTTP " + connection.getResponseCode());
        }
        try (java.io.InputStream in = connection.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static void registerOpenSearchBackend() throws Exception {
        Class.forName(StandardIndexProvider.class.getName(), true, StandardIndexProvider.class.getClassLoader());
        Field field = StandardIndexProvider.class.getDeclaredField("ALL_MANAGER_CLASSES");
        Unsafe unsafe = obtainUnsafe();
        Object base = unsafe.staticFieldBase(field);
        long offset = unsafe.staticFieldOffset(field);
        @SuppressWarnings("unchecked")
        Map<String, String> current = (Map<String, String>) unsafe.getObject(base, offset);
        if (current == null) {
            throw new IllegalStateException("StandardIndexProvider.ALL_MANAGER_CLASSES is null after class init");
        }
        Map<String, String> updated = new HashMap<>(current);
        updated.put("opensearch", "org.janusgraph.diskstorage.opensearch.OpenSearchIndex");
        unsafe.putObject(base, offset, ImmutableMap.copyOf(updated));
        @SuppressWarnings("unchecked")
        Map<String, String> verified = (Map<String, String>) unsafe.getObject(base, offset);
        if (!"org.janusgraph.diskstorage.opensearch.OpenSearchIndex".equals(verified.get("opensearch"))) {
            System.out.println("[WARN] opensearch shorthand registration did not stick; using fully-qualified backend class in config");
        }
    }

    static ReadConfiguration buildConfiguration(
            String graphIndexName, String backingIndexName, int maxResultSetSize) {
        Properties properties = new Properties();
        properties.setProperty("storage.backend", "inmemory");
        properties.setProperty("index." + backingIndexName + ".backend",
                "org.janusgraph.diskstorage.opensearch.OpenSearchIndex");
        properties.setProperty("index." + backingIndexName + ".hostname", getOpenSearchHost());
        properties.setProperty("index." + backingIndexName + ".port", String.valueOf(getOpenSearchPort()));
        properties.setProperty("index." + backingIndexName + ".index-name", graphIndexName);
        properties.setProperty("index." + backingIndexName + ".opensearch.setup-max-open-scroll-contexts", "false");
        properties.setProperty("index." + backingIndexName + ".opensearch.bulk-refresh", "wait_for");
        properties.setProperty("index." + backingIndexName + ".opensearch.retry_on_conflict", "3");
        properties.setProperty("index." + backingIndexName + ".max-result-set-size",
                String.valueOf(maxResultSetSize));

        org.apache.commons.configuration2.Configuration commons =
                org.apache.commons.configuration2.ConfigurationConverter.getConfiguration(properties);
        return new CommonsConfiguration(commons);
    }

    private static Unsafe obtainUnsafe() throws Exception {
        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        return (Unsafe) unsafeField.get(null);
    }
}
