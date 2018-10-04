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

import org.apache.atlas.security.InMemoryJAASConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Properties;

/**
 * Application properties used by Atlas.
 */
public final class ApplicationProperties extends PropertiesConfiguration {
    public static final String ATLAS_CONFIGURATION_DIRECTORY_PROPERTY = "atlas.conf";
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProperties.class);

    public static final String  APPLICATION_PROPERTIES     = "atlas-application.properties";

    public static final SimpleEntry<String, String> DB_CACHE_CONF               = new SimpleEntry<>("atlas.graph.cache.db-cache", "true");
    public static final SimpleEntry<String, String> DB_CACHE_CLEAN_WAIT_CONF    = new SimpleEntry<>("atlas.graph.cache.db-cache-clean-wait", "20");
    public static final SimpleEntry<String, String> DB_CACHE_SIZE_CONF          = new SimpleEntry<>("atlas.graph.cache.db-cache-size", "0.5");
    public static final SimpleEntry<String, String> DB_TX_CACHE_SIZE_CONF       = new SimpleEntry<>("atlas.graph.cache.tx-cache-size", "15000");
    public static final SimpleEntry<String, String> DB_CACHE_TX_DIRTY_SIZE_CONF = new SimpleEntry<>("atlas.graph.cache.tx-dirty-size", "120");

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
                    set(get(APPLICATION_PROPERTIES));
                }
            }
        }

        return instance;
    }

    public static Configuration set(Configuration configuration) throws AtlasException {
        synchronized (ApplicationProperties.class) {
            instance = configuration;

            InMemoryJAASConfiguration.init(instance);
        }

        return instance;
    }

    public static Configuration get(String fileName) throws AtlasException {
        String confLocation = System.getProperty(ATLAS_CONFIGURATION_DIRECTORY_PROPERTY);
        try {
            URL url = null;

            if (confLocation == null) {
                LOG.info("Looking for {} in classpath", fileName);

                url = ApplicationProperties.class.getClassLoader().getResource(fileName);

                if (url == null) {
                    LOG.info("Looking for /{} in classpath", fileName);

                    url = ApplicationProperties.class.getClassLoader().getResource("/" + fileName);
                }
            } else {
                url = new File(confLocation, fileName).toURI().toURL();
            }

            LOG.info("Loading {} from {}", fileName, url);

            ApplicationProperties appProperties = new ApplicationProperties(url);

            appProperties.setDefaults();

            Configuration configuration = appProperties.interpolatedConfiguration();

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

    public static Properties getSubsetAsProperties(Configuration inConf, String prefix) {
        Configuration subset = inConf.subset(prefix);
        Properties   ret     = ConfigurationConverter.getProperties(subset);

        return ret;
    }

    public static Class getClass(Configuration configuration, String propertyName, String defaultValue,
                                 Class assignableClass) throws AtlasException {
        try {
            String propertyValue = configuration.getString(propertyName, defaultValue);
            Class<?> clazz = Class.forName(propertyValue);
            if (assignableClass == null || assignableClass.isAssignableFrom(clazz)) {
                return clazz;
            } else {
                String message = "Class " + clazz.getName() + " specified in property " + propertyName
                        + " is not assignable to class " + assignableClass.getName();
                LOG.error(message);
                throw new AtlasException(message);
            }
        } catch (Exception e) {
            throw new AtlasException(e);
        }
    }

    public static Class getClass(String fullyQualifiedClassName, Class assignableClass) throws AtlasException {
        try {
            Class<?> clazz = Class.forName(fullyQualifiedClassName);
            if (assignableClass == null || assignableClass.isAssignableFrom(clazz)) {
                return clazz;
            } else {
                String message = "Class " + clazz.getName() + " is not assignable to class " + assignableClass.getName();
                LOG.error(message);
                throw new AtlasException(message);
            }
        } catch (Exception e) {
            throw new AtlasException(e);
        }
    }

    /**
     * Get the specified property as an {@link InputStream}.
     * If the property is not set, then the specified default filename
     * is searched for in the following locations, in order of precedence:
     * 1. Atlas configuration directory specified by the {@link #ATLAS_CONFIGURATION_DIRECTORY_PROPERTY} system property
     * 2. relative to the working directory if {@link #ATLAS_CONFIGURATION_DIRECTORY_PROPERTY} is not set
     * 3. as a classloader resource
     *
     * @param configuration
     * @param propertyName
     * @param defaultFileName name of file to use by default if specified property is not set in the configuration- if null,
     * an {@link AtlasException} is thrown if the property is not set
     * @return an {@link InputStream}
     * @throws AtlasException if no file was found or if there was an error loading the file
     */
    public static InputStream getFileAsInputStream(Configuration configuration, String propertyName, String defaultFileName) throws AtlasException {
        File   fileToLoad = null;
        String fileName   = configuration.getString(propertyName);

        if (fileName == null) {
            if (defaultFileName == null) {
                throw new AtlasException(propertyName + " property not set and no default value specified");
            }

            LOG.info("{} property not set; defaulting to {}", propertyName, defaultFileName);

            fileName = defaultFileName;

            String atlasConfDir = System.getProperty(ATLAS_CONFIGURATION_DIRECTORY_PROPERTY);

            if (atlasConfDir != null) {
                // Look for default filename in Atlas config directory
                fileToLoad = new File(atlasConfDir, fileName);
            } else {
                // Look for default filename under the working directory
                fileToLoad = new File(fileName);
            }
        } else {
            // Look for configured filename
            fileToLoad = new File(fileName);
        }

        InputStream inStr = null;

        if (fileToLoad.exists()) {
            try {
                LOG.info("Loading file {} from {}", fileName, fileToLoad.getPath());

                inStr = new FileInputStream(fileToLoad);
            } catch (FileNotFoundException e) {
                throw new AtlasException("Error loading file " + fileName, e);
            }
        } else {
            // Look for file as class loader resource
            inStr = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);

            if (inStr == null) {
                String msg = fileName + " not found in file system or as class loader resource";

                LOG.error(msg);

                throw new AtlasException(msg);
            }

            LOG.info("Loaded {} as resource from {}", fileName, Thread.currentThread().getContextClassLoader().getResource(fileName).toString());
        }

        return inStr;
    }

    private void setDefaults() {
        setDbCacheConfDefaults();
    }

    void setDefault(SimpleEntry<String, String> keyValueDefault, String currentValue) {
        if (StringUtils.isNotEmpty(currentValue)) {
            return;
        }

        clearPropertyDirect(keyValueDefault.getKey());
        addPropertyDirect(keyValueDefault.getKey(), keyValueDefault.getValue());
        LOG.info("Property (set to default) {} = {}", keyValueDefault.getKey(), keyValueDefault.getValue());
    }

    private void setDbCacheConfDefaults() {
        SimpleEntry<String, String> keyValues[] = new SimpleEntry[]{ DB_CACHE_CONF, DB_CACHE_CLEAN_WAIT_CONF,
                                                                     DB_CACHE_SIZE_CONF, DB_TX_CACHE_SIZE_CONF,
                                                                     DB_CACHE_TX_DIRTY_SIZE_CONF };

        for(SimpleEntry<String, String> kv : keyValues) {
            String currentValue = getString(kv.getKey());

            setDefault(kv, currentValue);
        }
    }
}
