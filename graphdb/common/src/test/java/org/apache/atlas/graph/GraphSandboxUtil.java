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

public class GraphSandboxUtil {
    private static final Logger LOG = LoggerFactory.getLogger(GraphSandboxUtil.class);

    public static void create() {
        Configuration configuration;
        try {
            configuration = ApplicationProperties.get();
            // Append a suffix to isolate the database for each instance
            long currentMillisecs = System.currentTimeMillis();

            String newStorageDir = System.getProperty("atlas.data") +
                    File.pathSeparator + "storage" +
                    File.pathSeparator + currentMillisecs;
            configuration.setProperty("atlas.graph.storage.directory", newStorageDir);

            String newIndexerDir = System.getProperty("atlas.data") +
                    File.pathSeparator + "index" +
                    File.pathSeparator + currentMillisecs;
            configuration.setProperty("atlas.graph.index.search.directory", newIndexerDir);

            LOG.debug("New Storage dir : {}", newStorageDir);
            LOG.debug("New Indexer dir : {}", newIndexerDir);
        } catch (AtlasException ignored) {}
    }
}
