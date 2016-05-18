/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for InstanceRequest.
 */
public class InstanceRequestTest {
    @Test
    public void testRequestProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("foo", "fooValue");
        properties.put("someBoolean", true);
        Request request = new InstanceRequest(properties);

        assertEquals(Request.Cardinality.INSTANCE, request.getCardinality());
        assertEquals(properties, request.getProperties());
        assertEquals("fooValue", request.getProperty("foo"));
        assertTrue(request.<Boolean>getProperty("someBoolean"));
        assertNull(request.getProperty("other"));
        assertTrue(request.getAdditionalSelectProperties().isEmpty());
    }

    @Test
    public void testSelectProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("foo", "fooValue");
        properties.put("someBoolean", true);
        Request request = new InstanceRequest(properties);

        Collection<String> additionalSelectProps = new ArrayList<>();
        additionalSelectProps.add("prop1");
        additionalSelectProps.add("prop2");
        request.addAdditionalSelectProperties(additionalSelectProps);
        Collection<String> requestAdditionalSelectProps = request.getAdditionalSelectProperties();
        assertEquals(2, requestAdditionalSelectProps.size());
        assertTrue(requestAdditionalSelectProps.contains("prop1"));
        assertTrue(requestAdditionalSelectProps.contains("prop2"));
    }
}
