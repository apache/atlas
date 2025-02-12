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

package org.apache.atlas.notification;

import org.apache.atlas.model.notification.MessageVersion;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * MessageVersion tests.
 */
public class MessageVersionTest {
    @Test
    public void testConstructor() {
        new MessageVersion("1.0.0");

        try {
            new MessageVersion("foo");

            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new MessageVersion("A.0.0");

            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new MessageVersion("1.0.0a");

            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testCompareTo() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.0.0");
        MessageVersion version3 = new MessageVersion("2.0.0");
        MessageVersion version4 = new MessageVersion("1");
        MessageVersion version5 = new MessageVersion("1.5");
        MessageVersion version6 = new MessageVersion("1.0.5");

        assertEquals(version1.compareTo(version2), 0);
        assertEquals(version2.compareTo(version1), 0);
        assertTrue(version1.compareTo(version3) < 0);
        assertTrue(version3.compareTo(version1) > 0);
        assertEquals(version1.compareTo(version4), 0);
        assertEquals(version4.compareTo(version1), 0);
        assertTrue(version1.compareTo(version5) < 0);
        assertTrue(version5.compareTo(version1) > 0);
        assertTrue(version1.compareTo(version6) < 0);
        assertTrue(version6.compareTo(version1) > 0);
    }

    @Test
    public void testEquals() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.0.0");
        MessageVersion version3 = new MessageVersion("2.0.0");
        MessageVersion version4 = new MessageVersion("1");
        MessageVersion version5 = new MessageVersion("1.5");
        MessageVersion version6 = new MessageVersion("1.0.5");

        assertEquals(version2, version1);
        assertEquals(version1, version2);
        assertNotEquals(version3, version1);
        assertNotEquals(version1, version3);
        assertEquals(version4, version1);
        assertEquals(version1, version4);
        assertNotEquals(version5, version1);
        assertNotEquals(version1, version5);
        assertNotEquals(version6, version1);
        assertNotEquals(version1, version6);
    }

    @Test
    public void testHashCode() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.0.0");
        MessageVersion version3 = new MessageVersion("1");

        assertEquals(version1.hashCode(), version2.hashCode());
        assertEquals(version1.hashCode(), version3.hashCode());
    }

    @Test
    public void testGetVersionParts() {
        MessageVersion version = new MessageVersion("1.0.0");

        assertTrue(Arrays.equals(new Integer[] {1}, version.getVersionParts()));

        version = new MessageVersion("1.0");

        assertTrue(Arrays.equals(new Integer[] {1}, version.getVersionParts()));

        version = new MessageVersion("1");

        assertTrue(Arrays.equals(new Integer[] {1}, version.getVersionParts()));

        version = new MessageVersion("1.0.2");

        assertTrue(Arrays.equals(new Integer[] {1, 0, 2}, version.getVersionParts()));
    }
}
