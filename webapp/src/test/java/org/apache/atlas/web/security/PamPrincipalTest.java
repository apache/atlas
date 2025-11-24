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

package org.apache.atlas.web.security;

import org.jvnet.libpam.UnixUser;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class PamPrincipalTest {
    @Mock
    private UnixUser mockUnixUser;

    private PamPrincipal pamPrincipal;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testConstructor_AllFields() {
        // Setup
        Set<String> testGroups = new HashSet<>();
        testGroups.add("group1");
        testGroups.add("group2");

        when(mockUnixUser.getUserName()).thenReturn("testuser");
        when(mockUnixUser.getGecos()).thenReturn("Test User,,,,");
        when(mockUnixUser.getDir()).thenReturn("/home/testuser");
        when(mockUnixUser.getShell()).thenReturn("/bin/bash");
        when(mockUnixUser.getUID()).thenReturn(1001);
        when(mockUnixUser.getGID()).thenReturn(1001);
        when(mockUnixUser.getGroups()).thenReturn(testGroups);

        // Execute
        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Verify
        assertEquals(pamPrincipal.getName(), "testuser");
        assertEquals(pamPrincipal.getGecos(), "Test User,,,,");
        assertEquals(pamPrincipal.getHomeDir(), "/home/testuser");
        assertEquals(pamPrincipal.getShell(), "/bin/bash");
        assertEquals(pamPrincipal.getUid(), 1001);
        assertEquals(pamPrincipal.getGid(), 1001);

        Set<String> returnedGroups = pamPrincipal.getGroups();
        assertEquals(returnedGroups.size(), 2);
        assertTrue(returnedGroups.contains("group1"));
        assertTrue(returnedGroups.contains("group2"));

        // Verify groups set is unmodifiable
        try {
            returnedGroups.add("newgroup");
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // Expected - groups set should be unmodifiable
        }
    }

    @Test
    public void testConstructor_WithNullValues() {
        // Setup with null values
        Set<String> emptyGroups = Collections.emptySet();

        when(mockUnixUser.getUserName()).thenReturn(null);
        when(mockUnixUser.getGecos()).thenReturn(null);
        when(mockUnixUser.getDir()).thenReturn(null);
        when(mockUnixUser.getShell()).thenReturn(null);
        when(mockUnixUser.getUID()).thenReturn(0);
        when(mockUnixUser.getGID()).thenReturn(0);
        when(mockUnixUser.getGroups()).thenReturn(emptyGroups);

        // Execute
        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Verify
        assertNull(pamPrincipal.getName());
        assertNull(pamPrincipal.getGecos());
        assertNull(pamPrincipal.getHomeDir());
        assertNull(pamPrincipal.getShell());
        assertEquals(pamPrincipal.getUid(), 0);
        assertEquals(pamPrincipal.getGid(), 0);
        assertTrue(pamPrincipal.getGroups().isEmpty());
    }

    @Test
    public void testConstructor_WithEmptyStrings() {
        // Setup with empty strings
        Set<String> singleGroup = Collections.singleton("onlygroup");

        when(mockUnixUser.getUserName()).thenReturn("");
        when(mockUnixUser.getGecos()).thenReturn("");
        when(mockUnixUser.getDir()).thenReturn("");
        when(mockUnixUser.getShell()).thenReturn("");
        when(mockUnixUser.getUID()).thenReturn(-1);
        when(mockUnixUser.getGID()).thenReturn(-1);
        when(mockUnixUser.getGroups()).thenReturn(singleGroup);

        // Execute
        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Verify
        assertEquals(pamPrincipal.getName(), "");
        assertEquals(pamPrincipal.getGecos(), "");
        assertEquals(pamPrincipal.getHomeDir(), "");
        assertEquals(pamPrincipal.getShell(), "");
        assertEquals(pamPrincipal.getUid(), -1);
        assertEquals(pamPrincipal.getGid(), -1);
        assertEquals(pamPrincipal.getGroups().size(), 1);
        assertTrue(pamPrincipal.getGroups().contains("onlygroup"));
    }

    @Test
    public void testGetName() {
        // Setup
        when(mockUnixUser.getUserName()).thenReturn("admin");
        when(mockUnixUser.getGecos()).thenReturn("Administrator");
        when(mockUnixUser.getDir()).thenReturn("/home/admin");
        when(mockUnixUser.getShell()).thenReturn("/bin/zsh");
        when(mockUnixUser.getUID()).thenReturn(1000);
        when(mockUnixUser.getGID()).thenReturn(1000);
        when(mockUnixUser.getGroups()).thenReturn(Collections.emptySet());

        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Execute & Verify
        assertEquals(pamPrincipal.getName(), "admin");

        // Verify getName is consistent with Principal interface
        assertTrue(pamPrincipal instanceof Principal);
        Principal principal = pamPrincipal;
        assertEquals(principal.getName(), "admin");
    }

    @Test
    public void testGetGecos() {
        // Setup
        when(mockUnixUser.getUserName()).thenReturn("jdoe");
        when(mockUnixUser.getGecos()).thenReturn("John Doe,Room 123,555-1234,555-5678");
        when(mockUnixUser.getDir()).thenReturn("/home/jdoe");
        when(mockUnixUser.getShell()).thenReturn("/bin/bash");
        when(mockUnixUser.getUID()).thenReturn(1002);
        when(mockUnixUser.getGID()).thenReturn(1002);
        when(mockUnixUser.getGroups()).thenReturn(Collections.emptySet());

        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Execute & Verify
        assertEquals(pamPrincipal.getGecos(), "John Doe,Room 123,555-1234,555-5678");
    }

    @Test
    public void testGetHomeDir() {
        // Setup
        when(mockUnixUser.getUserName()).thenReturn("serviceuser");
        when(mockUnixUser.getGecos()).thenReturn("Service User");
        when(mockUnixUser.getDir()).thenReturn("/var/lib/service");
        when(mockUnixUser.getShell()).thenReturn("/usr/sbin/nologin");
        when(mockUnixUser.getUID()).thenReturn(999);
        when(mockUnixUser.getGID()).thenReturn(999);
        when(mockUnixUser.getGroups()).thenReturn(Collections.emptySet());

        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Execute & Verify
        assertEquals(pamPrincipal.getHomeDir(), "/var/lib/service");
    }

    @Test
    public void testGetShell() {
        // Setup
        when(mockUnixUser.getUserName()).thenReturn("developer");
        when(mockUnixUser.getGecos()).thenReturn("Developer Account");
        when(mockUnixUser.getDir()).thenReturn("/home/developer");
        when(mockUnixUser.getShell()).thenReturn("/bin/fish");
        when(mockUnixUser.getUID()).thenReturn(2000);
        when(mockUnixUser.getGID()).thenReturn(2000);
        when(mockUnixUser.getGroups()).thenReturn(Collections.emptySet());

        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Execute & Verify
        assertEquals(pamPrincipal.getShell(), "/bin/fish");
    }

    @Test
    public void testGetUid() {
        // Setup
        when(mockUnixUser.getUserName()).thenReturn("testuser");
        when(mockUnixUser.getGecos()).thenReturn("Test User");
        when(mockUnixUser.getDir()).thenReturn("/home/testuser");
        when(mockUnixUser.getShell()).thenReturn("/bin/bash");
        when(mockUnixUser.getUID()).thenReturn(12345);
        when(mockUnixUser.getGID()).thenReturn(54321);
        when(mockUnixUser.getGroups()).thenReturn(Collections.emptySet());

        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Execute & Verify
        assertEquals(pamPrincipal.getUid(), 12345);
    }

    @Test
    public void testGetGid() {
        // Setup
        when(mockUnixUser.getUserName()).thenReturn("testuser");
        when(mockUnixUser.getGecos()).thenReturn("Test User");
        when(mockUnixUser.getDir()).thenReturn("/home/testuser");
        when(mockUnixUser.getShell()).thenReturn("/bin/bash");
        when(mockUnixUser.getUID()).thenReturn(12345);
        when(mockUnixUser.getGID()).thenReturn(54321);
        when(mockUnixUser.getGroups()).thenReturn(Collections.emptySet());

        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Execute & Verify
        assertEquals(pamPrincipal.getGid(), 54321);
    }

    @Test
    public void testGetGroups_MultipleGroups() {
        // Setup
        Set<String> testGroups = new HashSet<>();
        testGroups.add("admin");
        testGroups.add("users");
        testGroups.add("developers");

        when(mockUnixUser.getUserName()).thenReturn("poweruser");
        when(mockUnixUser.getGecos()).thenReturn("Power User");
        when(mockUnixUser.getDir()).thenReturn("/home/poweruser");
        when(mockUnixUser.getShell()).thenReturn("/bin/bash");
        when(mockUnixUser.getUID()).thenReturn(1500);
        when(mockUnixUser.getGID()).thenReturn(1500);
        when(mockUnixUser.getGroups()).thenReturn(testGroups);

        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Execute & Verify
        Set<String> returnedGroups = pamPrincipal.getGroups();
        assertEquals(returnedGroups.size(), 3);
        assertTrue(returnedGroups.contains("admin"));
        assertTrue(returnedGroups.contains("users"));
        assertTrue(returnedGroups.contains("developers"));

        // Verify it's the same instance returned each time (cached)
        assertSame(pamPrincipal.getGroups(), pamPrincipal.getGroups());
    }

    @Test
    public void testGetGroups_EmptyGroups() {
        // Setup
        Set<String> emptyGroups = Collections.emptySet();

        when(mockUnixUser.getUserName()).thenReturn("isolateduser");
        when(mockUnixUser.getGecos()).thenReturn("Isolated User");
        when(mockUnixUser.getDir()).thenReturn("/home/isolateduser");
        when(mockUnixUser.getShell()).thenReturn("/bin/bash");
        when(mockUnixUser.getUID()).thenReturn(3000);
        when(mockUnixUser.getGID()).thenReturn(3000);
        when(mockUnixUser.getGroups()).thenReturn(emptyGroups);

        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Execute & Verify
        Set<String> returnedGroups = pamPrincipal.getGroups();
        assertTrue(returnedGroups.isEmpty());
        assertEquals(returnedGroups.size(), 0);
    }

    @Test
    public void testInstanceofPrincipal() {
        // Setup
        when(mockUnixUser.getUserName()).thenReturn("testuser");
        when(mockUnixUser.getGecos()).thenReturn("");
        when(mockUnixUser.getDir()).thenReturn("");
        when(mockUnixUser.getShell()).thenReturn("");
        when(mockUnixUser.getUID()).thenReturn(0);
        when(mockUnixUser.getGID()).thenReturn(0);
        when(mockUnixUser.getGroups()).thenReturn(Collections.emptySet());

        pamPrincipal = new PamPrincipal(mockUnixUser);

        // Execute & Verify
        assertTrue(pamPrincipal instanceof Principal);
        assertTrue(pamPrincipal instanceof PamPrincipal);

        // Verify Principal interface contract
        Principal principal = pamPrincipal;
        assertNotNull(principal.getName());
    }
}
