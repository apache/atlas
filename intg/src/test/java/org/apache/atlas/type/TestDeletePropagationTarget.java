/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.type;

import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDeletePropagationTarget {
    @Test
    public void testConstructorAndGetters() {
        AtlasAttribute relAttr = mock(AtlasAttribute.class);

        DeletePropagationTarget target = new DeletePropagationTarget("trino_table", relAttr);

        assertEquals(target.getTargetTypeName(), "trino_table");
        assertEquals(target.getRelAttr(), relAttr);
    }

    @Test
    public void testConstructorWithNullValues() {
        DeletePropagationTarget target = new DeletePropagationTarget(null, null);

        assertNull(target.getTargetTypeName());
        assertNull(target.getRelAttr());
    }

    @Test
    public void testEqualsSameInstance() {
        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        DeletePropagationTarget target = new DeletePropagationTarget("trino_table", relAttr);

        assertTrue(target.equals(target));
    }

    @Test
    public void testEqualsSameValues() {
        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        DeletePropagationTarget target1 = new DeletePropagationTarget("trino_table", relAttr);
        DeletePropagationTarget target2 = new DeletePropagationTarget("trino_table", relAttr);

        assertEquals(target1, target2);
        assertEquals(target1.hashCode(), target2.hashCode());
    }

    @Test
    public void testEqualsIdentityOnRelAttr() {
        AtlasAttribute relAttr1 = mock(AtlasAttribute.class);
        AtlasAttribute relAttr2 = mock(AtlasAttribute.class);
        DeletePropagationTarget target1 = new DeletePropagationTarget("trino_table", relAttr1);
        DeletePropagationTarget target2 = new DeletePropagationTarget("trino_table", relAttr2);

        assertNotEquals(target1, target2);
    }

    @Test
    public void testEqualsDifferentTypeName() {
        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        DeletePropagationTarget target1 = new DeletePropagationTarget("trino_table", relAttr);
        DeletePropagationTarget target2 = new DeletePropagationTarget("trino_schema", relAttr);

        assertNotEquals(target1, target2);
    }

    @Test
    public void testEqualsNull() {
        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        DeletePropagationTarget target = new DeletePropagationTarget("trino_table", relAttr);

        assertFalse(target.equals(null));
    }

    @Test
    public void testEqualsDifferentClass() {
        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        DeletePropagationTarget target = new DeletePropagationTarget("trino_table", relAttr);

        assertFalse(target.equals("not a target"));
    }

    @Test
    public void testHashCodeConsistency() {
        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        DeletePropagationTarget target = new DeletePropagationTarget("trino_table", relAttr);

        int hash1 = target.hashCode();
        int hash2 = target.hashCode();

        assertEquals(hash1, hash2);
    }

    @Test
    public void testHashCodeDifferentForDifferentTargets() {
        AtlasAttribute relAttr1 = mock(AtlasAttribute.class);
        AtlasAttribute relAttr2 = mock(AtlasAttribute.class);
        DeletePropagationTarget target1 = new DeletePropagationTarget("trino_table", relAttr1);
        DeletePropagationTarget target2 = new DeletePropagationTarget("trino_schema", relAttr2);

        assertNotEquals(target1.hashCode(), target2.hashCode());
    }
}
