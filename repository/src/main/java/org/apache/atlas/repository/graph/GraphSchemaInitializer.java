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

package org.apache.atlas.repository.graph;

import org.apache.atlas.setup.SetupException;
import org.apache.atlas.setup.SetupStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * A {@link SetupStep} that initializes the Graph backend for Atlas.
 *
 * This class will initialize the specific backend implementation specified in
 * the Atlas configuration for the key atlas.graph.storage.backend.
 */
@Component
public class GraphSchemaInitializer implements SetupStep {

    private static final Logger LOG = LoggerFactory.getLogger(GraphSchemaInitializer.class);

    @Override
    public void run() throws SetupException {
        LOG.info("Initializing graph schema backend.");
        try {
            // The implementation of this method internally creates the schema.
            AtlasGraphProvider.getGraphInstance();
            LOG.info("Completed initializing graph schema backend.");
        } catch (Exception e) {
            LOG.error("Could not initialize graph schema backend due to exception, {}", e.getMessage(), e);
            throw new SetupException("Could not initialize graph schema due to exception", e);
        }
    }
}
