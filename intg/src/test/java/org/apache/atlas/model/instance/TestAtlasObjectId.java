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
package org.apache.atlas.model.instance;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestAtlasObjectId {

    @Test
    public void testEqualsDifferentIdShouldNotEqual()
    {
        AtlasObjectId one = new AtlasObjectId("one");
        AtlasObjectId different = new AtlasObjectId("different");

        assertNotEquals(one, different);
    }

    @Test
    public void testEqualsSameIdShouldEqual()
    {
        AtlasObjectId one = new AtlasObjectId("one");
        AtlasObjectId same = new AtlasObjectId("one");

        assertEquals(one, same);
    }

    @Test
    public void testEqualsSameIdButDifferentTypeShouldEqual()
    {
        AtlasObjectId one = new AtlasObjectId("one", "onetype");
        AtlasObjectId same = new AtlasObjectId("one", "anothertype");

        assertEquals(one, same);
    }

    @Test
    public void testEqualsDifferentIdButSameTypeShouldNotEqual()
    {
        AtlasObjectId one = new AtlasObjectId("one", "onetype");
        AtlasObjectId different = new AtlasObjectId("different", "onetype");

        assertNotEquals(one, different);
    }

    @Test
    public void testEqualsNoGuidOnOneShouldNotEqual()
    {
        AtlasObjectId one = new AtlasObjectId("one", "onetype");
        AtlasObjectId different = new AtlasObjectId("onetype", new HashMap<>());

        assertNotEquals(one, different);
    }

    @Test
    public void testEqualsNoGuidsButSameTypesShouldEqual()
    {
        AtlasObjectId one = new AtlasObjectId("onetype", new HashMap<>());
        AtlasObjectId same = new AtlasObjectId("onetype", new HashMap<>());

        assertEquals(one, same);
    }

    @Test
    public void testEqualsNoGuidsAndDifferentUniqueAttributesShouldNotEqual()
    {
        AtlasObjectId one = new AtlasObjectId("onetype", new HashMap<>());

        Map<String, Object> attrs = new HashMap<>();
        attrs.put("attr1", 1);
        AtlasObjectId different = new AtlasObjectId("onetype", attrs);

        assertNotEquals(one, different);
    }

    @Test
    public void testEqualsDifferentUniqueAttrsAndSameGuidsShouldEqual()
    {
        AtlasObjectId one = new AtlasObjectId("one", "onetype", new HashMap<>());

        Map<String, Object> attrs = new HashMap<>();
        attrs.put("attr1", 1);
        AtlasObjectId same = new AtlasObjectId("one", "onetype", attrs);

        assertEquals(one, same);
    }
}
