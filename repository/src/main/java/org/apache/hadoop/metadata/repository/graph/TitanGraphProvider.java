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

package org.apache.hadoop.metadata.repository.graph;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import javax.inject.Singleton;

public class TitanGraphProvider implements GraphProvider<TitanGraph> {
    private static final String SYSTEM_PROP = "";
    private static final String DEFAULT_PATH = "graph.properties";

    private final String configPath;

    public TitanGraphProvider() {
        configPath = System.getProperties().getProperty(SYSTEM_PROP,
                DEFAULT_PATH);
    }

    public Configuration getConfiguration() throws ConfigurationException {
        return new PropertiesConfiguration(configPath);
    }

    @Override
    @Singleton
    public TitanGraph get() throws ConfigurationException {
        TitanGraph graph = null;

        Configuration config;
        try {
            config = getConfiguration();
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        graph = TitanFactory.open(config);

        return graph;
    }
}
