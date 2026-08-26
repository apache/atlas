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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;

import java.util.Properties;

/**
 * Builds JanusGraph {@link Configuration} objects for OpenSearch integration tests using the
 * same flat {@code index.search.*} property layout as the janusgraph-opensearch smoke drivers.
 */
final class OpenSearchAtlasJanusTestSupport {

    private OpenSearchAtlasJanusTestSupport() {
    }

    static Configuration buildJanusGraphConfiguration(Configuration atlasJanusSubset, String host, int port, String indexName) {
        Properties properties = new Properties();

        properties.setProperty("storage.backend", atlasJanusSubset.getString("storage.backend", "berkeleyje"));
        properties.setProperty("storage.directory", atlasJanusSubset.getString("storage.directory"));
        properties.setProperty("storage.transactions", String.valueOf(atlasJanusSubset.getBoolean("storage.transactions", true)));
        properties.setProperty("index.search.backend", "opensearch");
        properties.setProperty("index.search.hostname", host);
        properties.setProperty("index.search.port", String.valueOf(port));
        properties.setProperty("index.search.max-result-set-size",
                String.valueOf(atlasJanusSubset.getInt("index.search.max-result-set-size", 150)));
        properties.setProperty("index.search.opensearch.setup-max-open-scroll-contexts", "false");

        if (indexName != null) {
            properties.setProperty("index.search.index-name", indexName);
        }

        return ConfigurationConverter.getConfiguration(properties);
    }
}
