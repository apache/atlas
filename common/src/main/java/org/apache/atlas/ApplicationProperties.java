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
package org.apache.atlas;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Iterator;

/**
 * Application properties used by Atlas.
 */
public final class ApplicationProperties extends PropertiesConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProperties.class);

    public static final String APPLICATION_PROPERTIES = "atlas-application.properties";

    private static volatile Configuration instance = null;

    private ApplicationProperties(URL url) throws ConfigurationException {
        super(url);
    }

    public static void forceReload() {
        if (instance != null) {
            synchronized (ApplicationProperties.class) {
                if (instance != null) {
                    instance = null;
                }
            }
        }
    }

    public static Configuration get() throws AtlasException {
        if (instance == null) {
            synchronized (ApplicationProperties.class) {
                if (instance == null) {
                    instance = get(APPLICATION_PROPERTIES);
                }
            }
        }
        return instance;
    }

    public static Configuration get(String fileName) throws AtlasException {
        String confLocation = System.getProperty("atlas.conf");
        try {
            URL url = confLocation == null ? ApplicationProperties.class.getResource("/" + fileName)
                    : new File(confLocation, fileName).toURI().toURL();
            LOG.info("Loading {} from {}", fileName, url);

            Configuration configuration = new ApplicationProperties(url).interpolatedConfiguration();
            logConfiguration(configuration);
            return configuration;
        } catch (Exception e) {
            throw new AtlasException("Failed to load application properties", e);
        }
    }

    private static void logConfiguration(Configuration configuration) {
        if (LOG.isDebugEnabled()) {
            Iterator<String> keys = configuration.getKeys();
            LOG.debug("Configuration loaded:");
            while (keys.hasNext()) {
                String key = keys.next();
                LOG.debug("{} = {}", key, configuration.getProperty(key));
            }
        }
    }

    public static Configuration getSubsetConfiguration(Configuration inConf, String prefix) {
        return inConf.subset(prefix);
    }

    public static Class getClass(String propertyName, String defaultValue) {
        try {
            Configuration configuration = get();
            String propertyValue = configuration.getString(propertyName, defaultValue);
            return Class.forName(propertyValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
