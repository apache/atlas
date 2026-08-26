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
package org.janusgraph.diskstorage.opensearch;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * Thin Atlas/JanusGraph integration wrapper around {@link OpenSearchIndex} that exposes the
 * underlying {@link OpenSearchClient} for Atlas discovery-layer operations.
 */
@PreInitializeConfigOptions
public class AtlasOpenSearchIndex extends OpenSearchIndex {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasOpenSearchIndex.class);

    private static AtlasOpenSearchIndex instance;

    private final OpenSearchClient client;

    public AtlasOpenSearchIndex(Configuration config) throws BackendException {
        super(config);

        OpenSearchClient openSearchClient = null;

        try {
            Field fld = OpenSearchIndex.class.getDeclaredField("client");

            fld.setAccessible(true);

            openSearchClient = (OpenSearchClient) fld.get(this);
        } catch (Exception excp) {
            LOG.warn("Failed to get OpenSearchClient", excp);
        }

        this.client = openSearchClient;

        AtlasOpenSearchIndex.instance = this;
    }

    public static OpenSearchClient getOpenSearchClient() {
        AtlasOpenSearchIndex index = AtlasOpenSearchIndex.instance;

        return index != null ? index.client : null;
    }
}
