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
 * Utility for reading properties in atlas-application.properties.
 */
public final class AtlasProperties {
    private static final Configuration APPLICATION_PROPERTIES;

    private AtlasProperties() { }

    static {
        try {
            APPLICATION_PROPERTIES = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Enum that encapsulated each property name and its default value.
     */
    public enum AtlasProperty {
        SEARCH_MAX_LIMIT("atlas.search.maxlimit", 10000),
        SEARCH_DEFAULT_LIMIT("atlas.search.defaultlimit", 100);

        private final String propertyName;
        private final Object defaultValue;

        AtlasProperty(String propertyName, Object defaultValue) {
            this.propertyName = propertyName;
            this.defaultValue = defaultValue;
        }
    }

    public static <T> T getProperty(AtlasProperty property) {
        Object value = APPLICATION_PROPERTIES.getProperty(property.propertyName);
        if (value == null) {
            return (T) property.defaultValue;
        } else {
            return (T) value;

        }
    }
}
