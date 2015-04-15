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
import org.apache.hadoop.metadata.PropertiesUtil;

import javax.inject.Singleton;
import java.util.Iterator;

/**
 * Default implementation for Graph Provider that doles out Titan Graph.
 */
public class TitanGraphProvider implements GraphProvider<TitanGraph> {

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String METADATA_PREFIX = "metadata.graph.";

    private static Configuration getConfiguration() throws ConfigurationException {
        PropertiesConfiguration configProperties = PropertiesUtil.getApplicationProperties();

        Configuration graphConfig = new PropertiesConfiguration();

        final Iterator<String> iterator = configProperties.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (key.startsWith(METADATA_PREFIX)) {
                String value = (String) configProperties.getProperty(key);
                key = key.substring(METADATA_PREFIX.length());
                graphConfig.setProperty(key, value);
            }
        }

        return graphConfig;
    }

    @Override
    @Singleton
    public TitanGraph get() {
        Configuration config;
        try {
            config = getConfiguration();
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }

        return TitanFactory.open(config);
    }
}
