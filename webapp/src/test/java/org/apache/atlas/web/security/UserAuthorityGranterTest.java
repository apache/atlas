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

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Set;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class UserAuthorityGranterTest {
    @Mock
    private Principal mockPrincipal;

    private UserAuthorityGranter userAuthorityGranter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        userAuthorityGranter = new UserAuthorityGranter();
    }

    @Test
    public void testGrant_ComprehensiveCoverage() {
        // Test 1: Basic functionality with mock principal
        when(mockPrincipal.getName()).thenReturn("testuser");

        Set<String> authorities = userAuthorityGranter.grant(mockPrincipal);

        // Verify the returned set contains exactly "DATA_SCIENTIST"
        assertNotNull(authorities);
        assertEquals(authorities.size(), 1);
        assertTrue(authorities.contains("DATA_SCIENTIST"));
        String[] authArray = authorities.toArray(new String[0]);
        assertEquals(authArray.length, 1);
        assertEquals(authArray[0], "DATA_SCIENTIST");
    }
}
