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
package org.janusgraph.diskstorage.es;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * NOTE: Class to get access to ElasticSearchIndex.client
 */

@PreInitializeConfigOptions
public class ElasticSearch7Index extends ElasticSearchIndex {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearch7Index.class);

    private static ElasticSearch7Index INSTANCE;

    private final ElasticSearchClient client;

    public ElasticSearch7Index(Configuration config) throws BackendException {
        super(config);

        ElasticSearchClient client = null;

        try {
            Field fld = ElasticSearchIndex.class.getDeclaredField("client");

            fld.setAccessible(true);

            client = (ElasticSearchClient) fld.get(this);
        } catch (Exception excp) {
            LOG.warn("Failed to get SolrClient", excp);
        }

        this.client = client;

        INSTANCE = this;
    }

    public static ElasticSearchClient getElasticSearchClient() {
        ElasticSearch7Index index = INSTANCE;

        return index != null ? index.client : null;
    }
}