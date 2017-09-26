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

/**
 * Enum that encapsulated each property name and its default value.
 */
public enum AtlasConfiguration {
    //web server configuration
    WEBSERVER_MIN_THREADS("atlas.webserver.minthreads", 10),
    WEBSERVER_MAX_THREADS("atlas.webserver.maxthreads", 100),
    WEBSERVER_KEEPALIVE_SECONDS("atlas.webserver.keepalivetimesecs", 60),
    WEBSERVER_QUEUE_SIZE("atlas.webserver.queuesize", 100),
    WEBSERVER_REQUEST_BUFFER_SIZE("atlas.jetty.request.buffer.size", 16192),

    QUERY_PARAM_MAX_LENGTH("atlas.query.param.max.length", 4*1024),

    //search configuration
    SEARCH_MAX_LIMIT("atlas.search.maxlimit", 10000),
    SEARCH_DEFAULT_LIMIT("atlas.search.defaultlimit", 100);

    private static final Configuration APPLICATION_PROPERTIES;

    static {
        try {
            APPLICATION_PROPERTIES = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    private final String propertyName;
    private final Object defaultValue;

    AtlasConfiguration(String propertyName, Object defaultValue) {
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
    }

    public int getInt() {
        return APPLICATION_PROPERTIES.getInt(propertyName, Integer.valueOf(defaultValue.toString()).intValue());
    }

    public long getLong() {
        return APPLICATION_PROPERTIES.getLong(propertyName, Long.valueOf(defaultValue.toString()).longValue());
    }

    public String getString() {
        return APPLICATION_PROPERTIES.getString(propertyName, defaultValue.toString());
    }

    public Object get() {
        Object value = APPLICATION_PROPERTIES.getProperty(propertyName);
        return value == null ? defaultValue : value;
    }
}
