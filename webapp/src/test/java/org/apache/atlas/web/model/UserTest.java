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

package org.apache.atlas.web.model;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class UserTest {
    private User user;
    private List<GrantedAuthority> authorities;

    @BeforeClass
    public void setUp() {
        authorities = new ArrayList<>();
        authorities.add(new SimpleGrantedAuthority("ROLE_USER"));
        authorities.add(new SimpleGrantedAuthority("ROLE_ADMIN"));

        user = new User("testuser", "testpassword", authorities);
    }

    @Test
    public void testUserFunctionality() {
        // Test default constructor
        User defaultUser = new User();
        assertNotNull(defaultUser);

        // Test parameterized constructor
        assertEquals(user.getUsername(), "testuser");
        assertEquals(user.getPassword(), "testpassword");
        assertEquals(user.getAuthorities(), authorities);

        // Test UserDetails interface methods
        assertTrue(user.isAccountNonExpired());
        assertTrue(user.isAccountNonLocked());
        assertTrue(user.isCredentialsNonExpired());
        assertTrue(user.isEnabled());

        // Test getter and setter methods
        user.setUsername("newusername");
        assertEquals(user.getUsername(), "newusername");

        user.setPassword("newpassword");
        assertEquals(user.getPassword(), "newpassword");

        List<GrantedAuthority> newAuthorities = new ArrayList<>();
        newAuthorities.add(new SimpleGrantedAuthority("ROLE_GUEST"));
        user.setAuthorities(newAuthorities);
        assertEquals(user.getAuthorities(), newAuthorities);

        // Test boolean flag setters
        user.setAccountNonExpired(false);
        assertFalse(user.isAccountNonExpired());

        user.setAccountNonLocked(false);
        assertFalse(user.isAccountNonLocked());

        user.setCredentialsNonExpired(false);
        assertFalse(user.isCredentialsNonExpired());

        user.setEnabled(false);
        assertFalse(user.isEnabled());

        // Test edge cases
        user.setUsername(null);
        assertNull(user.getUsername());

        user.setUsername("");
        assertEquals(user.getUsername(), "");

        user.setPassword(null);
        assertNull(user.getPassword());

        user.setPassword("");
        assertEquals(user.getPassword(), "");

        user.setAuthorities(null);
        assertNull(user.getAuthorities());

        // Test toString method
        String userString = user.toString();
        assertNotNull(userString);
        assertTrue(userString.contains("accountNonExpired=false"));
        assertTrue(userString.contains("accountNonLocked=false"));
        assertTrue(userString.contains("credentialsNonExpired=false"));
        assertTrue(userString.contains("enabled=false"));

        // Test with empty authorities list
        User emptyAuthUser = new User("emptyuser", "emptypass", new ArrayList<>());
        assertNotNull(emptyAuthUser.getAuthorities());
        assertEquals(emptyAuthUser.getAuthorities().size(), 0);

        // Test with null authorities in constructor
        User nullAuthUser = new User("nulluser", "nullpass", null);
        assertNull(nullAuthUser.getAuthorities());
    }
}
