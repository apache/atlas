/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApplicationProperties extends PropertiesConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProperties.class);

    public static final String APPLICATION_PROPERTIES = "application.properties";
    public static final String CLIENT_PROPERTIES = "client.properties";

    private static Configuration INSTANCE = null;

    private ApplicationProperties(URL url) throws ConfigurationException {
        super(url);
    }

    public static Configuration get() throws AtlasException {
        if (INSTANCE == null) {
            synchronized (ApplicationProperties.class) {
                if (INSTANCE == null) {
                   INSTANCE = get(APPLICATION_PROPERTIES);
                }
            }
        }
        return INSTANCE;
    }

    public static Configuration get(String fileName) throws AtlasException {
        String confLocation = System.getProperty("atlas.conf");
        try {
            URL url = confLocation == null ? ApplicationProperties.class.getResource("/" + fileName)
                    : new File(confLocation, fileName).toURI().toURL();
            LOG.info("Loading {} from {}", fileName, url);

            ApplicationProperties configuration = new ApplicationProperties(url);
            Iterator<String> keys = configuration.getKeys();
            LOG.debug("Configuration loaded:");
            while(keys.hasNext()) {
                String key = keys.next();
                LOG.debug("{} = {}", key, configuration.getProperty(key));
            }
            return configuration;
        } catch (Exception e) {
            throw new AtlasException("Failed to load application properties", e);
        }
    }

    public static final Configuration getSubsetConfiguration(Configuration inConf, String prefix) {
        return inConf.subset(prefix);
    }

    @Override
    public Object getProperty(String key) {
        Object value = super.getProperty(key);
        if (value instanceof String) {
            value = substituteVars((String) value);
        }
        return value;
    }

    private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");

    private static final int MAX_SUBST = 20;

    private String substituteVars(String expr) {
        if (expr == null) {
            return null;
        }
        Matcher match = VAR_PATTERN.matcher("");
        String eval = expr;

        for(int s = 0; s < MAX_SUBST; s++) {
            match.reset(eval);
            if (!match.find()) {
                return eval;
            }
            String var = match.group();
            var = var.substring(2, var.length() - 1); // remove ${ .. }
            String val = null;
            try {
                val = System.getProperty(var);
            } catch(SecurityException se) {
                LOG.warn("Unexpected SecurityException in Configuration", se);
            }
            if (val == null) {
                val = getString(var);
            }
            if (val == null) {
                return eval; // return literal ${var}: var is unbound
            }

            // substitute
            eval = eval.substring(0, match.start()) + val + eval.substring(match.end());
        }
        throw new IllegalStateException("Variable substitution depth too large: " + MAX_SUBST + " " + expr);
    }
}
