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

package org.apache.atlas.web.service;

import org.apache.atlas.web.dao.UserDao;
import org.apache.atlas.web.model.User;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class UserServiceTest {
    @Mock
    private UserDao userDao;

    @Mock
    private User user;

    private UserService userService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        userService = new UserService(userDao);
    }

    @Test
    public void testConstructor() {
        assertNotNull(userService);

        // Verify that userDao field is set
        Field userDaoField = getField(UserService.class, "userDao");
        UserDao actualUserDao = (UserDao) getFieldValue(userDaoField, userService);
        assertEquals(actualUserDao, userDao);
    }

    @Test
    public void testLoadUserByUsername() {
        String username = "testuser";
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        User result = userService.loadUserByUsername(username);

        assertEquals(result, user);
        verify(userDao).loadUserByUsername(username);
    }

    @Test
    public void testLoadUserByUsernameWithDifferentUsername() {
        String username = "anotheruser";
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        User result = userService.loadUserByUsername(username);

        assertEquals(result, user);
        verify(userDao).loadUserByUsername(username);
    }

    @Test
    public void testLoadUserByUsernameWithEmptyUsername() {
        String username = "";
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        User result = userService.loadUserByUsername(username);

        assertEquals(result, user);
        verify(userDao).loadUserByUsername(username);
    }

    @Test
    public void testLoadUserByUsernameWithNullUsername() {
        String username = null;
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        User result = userService.loadUserByUsername(username);

        assertEquals(result, user);
        verify(userDao).loadUserByUsername(username);
    }

    @Test(expectedExceptions = UsernameNotFoundException.class)
    public void testLoadUserByUsernameThrowsException() {
        String username = "nonexistentuser";
        when(userDao.loadUserByUsername(username)).thenThrow(new UsernameNotFoundException("User not found"));

        userService.loadUserByUsername(username);
    }

    @Test
    public void testLoadUserByUsernameWithSpecialCharacters() {
        String username = "user@domain.com";
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        User result = userService.loadUserByUsername(username);

        assertEquals(result, user);
        verify(userDao).loadUserByUsername(username);
    }

    @Test
    public void testLoadUserByUsernameWithNumericUsername() {
        String username = "12345";
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        User result = userService.loadUserByUsername(username);

        assertEquals(result, user);
        verify(userDao).loadUserByUsername(username);
    }

    @Test
    public void testLoadUserByUsernameWithLongUsername() {
        String username = "verylongusernamethatexceedsnormallimits";
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        User result = userService.loadUserByUsername(username);

        assertEquals(result, user);
        verify(userDao).loadUserByUsername(username);
    }

    @Test
    public void testLoadUserByUsernameWithWhitespace() {
        String username = "user name";
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        User result = userService.loadUserByUsername(username);

        assertEquals(result, user);
        verify(userDao).loadUserByUsername(username);
    }

    @Test
    public void testLoadUserByUsernameWithLeadingTrailingSpaces() {
        String username = " testuser ";
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        User result = userService.loadUserByUsername(username);

        assertEquals(result, user);
        verify(userDao).loadUserByUsername(username);
    }

    @Test
    public void testMultipleCallsToLoadUserByUsername() {
        String username1 = "user1";
        String username2 = "user2";

        User user1 = user;
        User user2 = user;

        when(userDao.loadUserByUsername(username1)).thenReturn(user1);
        when(userDao.loadUserByUsername(username2)).thenReturn(user2);

        User result1 = userService.loadUserByUsername(username1);
        User result2 = userService.loadUserByUsername(username2);

        assertEquals(result1, user1);
        assertEquals(result2, user2);
        verify(userDao).loadUserByUsername(username1);
        verify(userDao).loadUserByUsername(username2);
    }

    @Test
    public void testUserServiceImplementsUserDetailsService() {
        // Verify that UserService implements UserDetailsService interface
        assertTrue(userService instanceof org.springframework.security.core.userdetails.UserDetailsService);
    }

    @Test
    public void testUserDaoFieldAccess() {
        Field userDaoField = getField(UserService.class, "userDao");
        UserDao actualUserDao = (UserDao) getFieldValue(userDaoField, userService);

        assertNotNull(actualUserDao);
        assertEquals(actualUserDao, userDao);
    }

    @Test
    public void testLoadUserByUsernameReturnType() {
        String username = "testuser";
        when(userDao.loadUserByUsername(username)).thenReturn(user);

        Object result = userService.loadUserByUsername(username);

        assertTrue(result instanceof User);
        assertEquals(result, user);
    }

    @Test
    public void testServiceAnnotation() {
        // Verify that the class has the @Service annotation
        assertTrue(UserService.class.isAnnotationPresent(org.springframework.stereotype.Service.class));
    }

    // Helper methods for reflection
    private Field getField(Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private Object getFieldValue(Field field, Object instance) {
        try {
            return field.get(instance);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertTrue(boolean condition) {
        org.testng.Assert.assertTrue(condition);
    }
}
