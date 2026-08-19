/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.atlas.web.filters;

import org.apache.commons.configuration2.MapConfiguration;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasHeaderAuthConfigurationTest {
    @Test
    public void testDisabledWhenNotConfigured() {
        AtlasHeaderAuthConfiguration config = AtlasHeaderAuthConfiguration.load(new MapConfiguration(new HashMap<>()));

        assertFalse(config.isEnabled());
        assertTrue(config.getKeys().isEmpty());
    }

    @Test
    public void testEnabledWithoutMappingsHasNoHeaders() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(AtlasHeaderAuthConfiguration.PROP_HEADER_AUTH_ENABLED, true);

        AtlasHeaderAuthConfiguration config = AtlasHeaderAuthConfiguration.load(new MapConfiguration(properties));

        assertTrue(config.isEnabled());
        assertTrue(config.getKeys().isEmpty());
    }

    @Test
    public void testMethodHeaderMappings() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(AtlasHeaderAuthConfiguration.PROP_HEADER_AUTH_ENABLED, true);
        properties.put(AtlasHeaderAuthConfiguration.PROP_METHOD_HEADER_PREFIX + ".username", "username");
        properties.put(AtlasHeaderAuthConfiguration.PROP_METHOD_HEADER_PREFIX + ".roles", "roles");
        properties.put(AtlasHeaderAuthConfiguration.PROP_METHOD_HEADER_PREFIX + ".request-id", "requestid");

        AtlasHeaderAuthConfiguration config = AtlasHeaderAuthConfiguration.load(new MapConfiguration(properties));

        assertEquals(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_USERNAME), "username");
        assertEquals(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_ROLES), "roles");
        assertEquals(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_REQUEST_ID), "requestid");
    }

    @Test
    public void testCustomProxyHeaderNames() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(AtlasHeaderAuthConfiguration.PROP_HEADER_AUTH_ENABLED, true);
        properties.put(AtlasHeaderAuthConfiguration.PROP_METHOD_HEADER_PREFIX + ".username", "X-Forwarded-User");
        properties.put(AtlasHeaderAuthConfiguration.PROP_METHOD_HEADER_PREFIX + ".roles", "X-Forwarded-Groups");
        properties.put(AtlasHeaderAuthConfiguration.PROP_METHOD_HEADER_PREFIX + ".request-id", "X-Request-Id");

        AtlasHeaderAuthConfiguration config = AtlasHeaderAuthConfiguration.load(new MapConfiguration(properties));

        assertEquals(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_USERNAME), "X-Forwarded-User");
        assertEquals(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_ROLES), "X-Forwarded-Groups");
        assertEquals(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_REQUEST_ID), "X-Request-Id");
    }

    @Test
    public void testOnlyConfiguredHeadersAreMapped() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(AtlasHeaderAuthConfiguration.PROP_HEADER_AUTH_ENABLED, true);
        properties.put(AtlasHeaderAuthConfiguration.PROP_METHOD_HEADER_PREFIX + ".username", "X-Forwarded-User");

        AtlasHeaderAuthConfiguration config = AtlasHeaderAuthConfiguration.load(new MapConfiguration(properties));

        assertEquals(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_USERNAME), "X-Forwarded-User");
        assertNull(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_ROLES));
        assertNull(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_REQUEST_ID));
    }

    @Test
    public void testBlankPropertyRemovesMapping() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(AtlasHeaderAuthConfiguration.PROP_HEADER_AUTH_ENABLED, true);
        properties.put(AtlasHeaderAuthConfiguration.PROP_METHOD_HEADER_PREFIX + ".username", "username");
        properties.put(AtlasHeaderAuthConfiguration.PROP_METHOD_HEADER_PREFIX + ".roles", " ");

        AtlasHeaderAuthConfiguration config = AtlasHeaderAuthConfiguration.load(new MapConfiguration(properties));

        assertEquals(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_USERNAME), "username");
        assertNull(config.getHeaderName(AtlasHeaderAuthConfiguration.KEY_ROLES));
    }
}
