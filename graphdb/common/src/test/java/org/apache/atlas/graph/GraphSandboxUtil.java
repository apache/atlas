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
package org.apache.atlas.graph;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;

public class GraphSandboxUtil {
    private static final Logger LOG = LoggerFactory.getLogger(GraphSandboxUtil.class);

    public static void create(String sandboxName) {
        Configuration configuration;
        try {
            configuration = ApplicationProperties.get();

            String newStorageDir = System.getProperty("atlas.data") +
                    File.separatorChar + "storage" +
                    File.separatorChar + sandboxName;

            configuration.setProperty("atlas.graph.storage.directory", newStorageDir);


            String newIndexerDir = System.getProperty("atlas.data") +
                    File.separatorChar + "index" +
                    File.separatorChar + sandboxName;

            configuration.setProperty("atlas.graph.index.search.directory", newIndexerDir);


            LOG.debug("New Storage dir : {}", newStorageDir);
            LOG.debug("New Indexer dir : {}", newIndexerDir);
        } catch (AtlasException ignored) {}
    }

    public static void create() {
        // Append a suffix to isolate the database for each instance
        UUID uuid = UUID.randomUUID();
        create(uuid.toString());
    }

    // Need to start local Solr Cloud for JanusGraph 0.2.0
    public static boolean useLocalSolr() {
        boolean ret = false;
        
        try {
            Configuration conf     = ApplicationProperties.get();
            Object        property = conf.getProperty("atlas.graph.index.search.solr.embedded");

            if (property != null && property instanceof String) {
                ret = Boolean.valueOf((String) property);
            }
        } catch (AtlasException ignored) {}

        return ret;
    }
}
