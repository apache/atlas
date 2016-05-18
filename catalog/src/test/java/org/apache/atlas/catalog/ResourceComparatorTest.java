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

import java.util.*;

import static org.testng.Assert.assertEquals;

/**
 * Unit tests for ResourceComparator.
 */
public class ResourceComparatorTest {
    @Test
    public void testCompare() {
        Map<String, Object> map = new TreeMap<>(new ResourceComparator());
        map.put("a", "zzzzz");
        map.put("name", 1);
        map.put("z", "fdsfdsds");
        map.put("d", new ArrayList<>());
        map.put("id", 1);
        map.put("e", false);
        map.put("c", 1);
        map.put("href", "dfdfgdf");
        map.put("b", new HashMap<>());
        map.put("description", 1);
        map.put("f", 20);
        map.put("type", 1);

        Iterator<String> iter = map.keySet().iterator();
        assertEquals(iter.next(), "href");
        assertEquals(iter.next(), "name");
        assertEquals(iter.next(), "id");
        assertEquals(iter.next(), "description");
        assertEquals(iter.next(), "type");
        assertEquals(iter.next(), "a");
        assertEquals(iter.next(), "b");
        assertEquals(iter.next(), "c");
        assertEquals(iter.next(), "d");
        assertEquals(iter.next(), "e");
        assertEquals(iter.next(), "f");
        assertEquals(iter.next(), "z");
    }
}
