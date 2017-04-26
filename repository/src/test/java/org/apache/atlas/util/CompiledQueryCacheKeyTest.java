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

package org.apache.atlas.util;

import org.apache.atlas.query.QueryParams;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

/**
 * Tests hashcode/equals behavior of CompiledQueryCacheKey
 *
 *
 */
public class CompiledQueryCacheKeyTest {

    @Test
    public void testNoQueryParams() {


        CompiledQueryCacheKey e1 = new CompiledQueryCacheKey("query 1");
        CompiledQueryCacheKey e2 = new CompiledQueryCacheKey("query 1");
        CompiledQueryCacheKey e3 = new CompiledQueryCacheKey("query 2");

        assertKeysEqual(e1, e2);
        assertKeysDifferent(e2, e3);
    }


    @Test
    public void testWithQueryParams() {

        CompiledQueryCacheKey e1 = new CompiledQueryCacheKey("query 1", new QueryParams(10,10));
        CompiledQueryCacheKey e2 = new CompiledQueryCacheKey("query 1", new QueryParams(10,10));
        CompiledQueryCacheKey e3 = new CompiledQueryCacheKey("query 2", new QueryParams(10,10));

        assertKeysEqual(e1, e2);
        assertKeysDifferent(e2, e3);
    }

    @Test
    public void testOnlyQueryParamsDifferent() {


        CompiledQueryCacheKey e1 = new CompiledQueryCacheKey("query 1", new QueryParams(10,10));
        CompiledQueryCacheKey e2 = new CompiledQueryCacheKey("query 1", new QueryParams(20,10));

        assertKeysDifferent(e1, e2);
    }

    @Test
    public void testOnlyDslDifferent() {


        CompiledQueryCacheKey e1 = new CompiledQueryCacheKey("query 1", new QueryParams(10,10));
        CompiledQueryCacheKey e2 = new CompiledQueryCacheKey("query 2", new QueryParams(10,10));

        assertKeysDifferent(e1, e2);
    }


    @Test
    public void testMixOfQueryParamsAndNone() {


        CompiledQueryCacheKey e1 = new CompiledQueryCacheKey("query 1", new QueryParams(10,10));
        CompiledQueryCacheKey e2 = new CompiledQueryCacheKey("query 1");

        assertKeysDifferent(e1, e2);
    }


    private void assertKeysEqual(CompiledQueryCacheKey e1, CompiledQueryCacheKey e2) {

        assertEquals(e1.hashCode(), e2.hashCode());
        assertEquals(e1, e2);
        assertEquals(e2, e1);
    }

    private void assertKeysDifferent(CompiledQueryCacheKey e1, CompiledQueryCacheKey e2) {

        assertNotSame(e1.hashCode(), e2.hashCode());
        assertNotSame(e1, e2);
        assertNotSame(e2, e1);
    }

}
