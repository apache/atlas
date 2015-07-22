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

import com.google.inject.Provides;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.atlas.AtlasException;
import org.apache.atlas.PropertiesUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.Iterator;
import java.util.Properties;

/**
 * Default implementation for Graph Provider that doles out Titan Graph.
 */
public class TitanGraphProvider implements GraphProvider<TitanGraph> {

    private static final Logger LOG = LoggerFactory.getLogger(TitanGraphProvider.class);

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String ATLAS_PREFIX = "atlas.graph.";

    private static TitanGraph graphInstance;

    private static Configuration getConfiguration() throws AtlasException {
        PropertiesConfiguration configProperties = PropertiesUtil.getApplicationProperties();

        Configuration graphConfig = new PropertiesConfiguration();

        Properties sysProperties = System.getProperties();
        LOG.info("System properties: ");
        LOG.info(sysProperties.toString());

        final Iterator<String> iterator = configProperties.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (key.startsWith(ATLAS_PREFIX)) {
                String value = (String) configProperties.getProperty(key);
                key = key.substring(ATLAS_PREFIX.length());
                graphConfig.setProperty(key, value);
                LOG.info("Using graph property {}={}", key, value);
            }
        }

        return graphConfig;
    }

    @Override
    @Singleton
    @Provides
    public TitanGraph get() {
        if(graphInstance == null) {
            synchronized (TitanGraphProvider.class) {
                if(graphInstance == null) {
                    Configuration config;
                    try {
                        config = getConfiguration();
                    } catch (AtlasException e) {
                        throw new RuntimeException(e);
                    }

                    graphInstance = TitanFactory.open(config);
                }
            }
        }
        return graphInstance;
    }
}
