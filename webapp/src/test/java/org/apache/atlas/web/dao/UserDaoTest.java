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

package org.apache.atlas.web.dao;

import org.apache.atlas.web.model.User;
import org.apache.atlas.web.security.AtlasAuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

public class UserDaoTest {
    private UserDao userDao;

    @BeforeClass
    public void setUp() {
        userDao = new UserDao();
    }

    @Test
    public void testEncryptAndCheckEncrypted() {
        String rawPassword = "secure123";
        String encrypted = UserDao.encrypt(rawPassword);

        assertNotNull(encrypted);
        assertTrue(UserDao.checkEncrypted(rawPassword, encrypted, "user1"));
    }

    @Test
    public void testEncodePassword() {
        String password = "mypassword";
        String salt = "1234";

        String encoded = UserDao.encodePassword(password, salt);
        assertNotNull(encoded);
    }

    @Test
    public void testLoadUserByUsername_Success() {
        Properties mockProps = new Properties();
        mockProps.setProperty("johndoe", "ROLE_USER::" + UserDao.encrypt("john123"));

        userDao.setUserLogins(mockProps);

        User user = userDao.loadUserByUsername("johndoe");

        assertEquals(user.getUsername(), "johndoe");
        assertTrue(user.getAuthorities().stream()
                .anyMatch(auth -> auth.getAuthority().equals("ROLE_USER")));
    }

    @Test
    public void testLoadUserByUsername_UserNotFound() {
        userDao.setUserLogins(new Properties());

        expectThrows(UsernameNotFoundException.class, () -> {
            userDao.loadUserByUsername("unknown");
        });
    }

    @Test
    public void testLoadUserByUsername_InvalidDataFormat() {
        String username = "baduser";
        Properties props = new Properties();
        props.setProperty(username, "MALFORMEDDATA");

        userDao.setUserLogins(props);

        AtlasAuthenticationException exception = expectThrows(AtlasAuthenticationException.class, () -> {
            userDao.loadUserByUsername("baduser");
        });
        assertEquals(exception.getMessage(), "User role credentials is not set properly for " + username);
    }

    // Additional test cases for better coverage

    @Test
    public void testLoadUserByUsername_EmptyRole() {
        String username = "emptyrole";
        Properties props = new Properties();
        props.setProperty(username, "::" + UserDao.encrypt("password123"));

        userDao.setUserLogins(props);

        AtlasAuthenticationException exception = expectThrows(AtlasAuthenticationException.class, () -> {
            userDao.loadUserByUsername("emptyrole");
        });
        assertEquals(exception.getMessage(), "User role credentials is not set properly for " + username);
    }

    @Test
    public void testLoadUserByUsername_EmptyUserDetails() {
        String username = "emptyuser";
        Properties props = new Properties();
        props.setProperty(username, "");

        userDao.setUserLogins(props);

        expectThrows(UsernameNotFoundException.class, () -> {
            userDao.loadUserByUsername("emptyuser");
        });
    }

    @Test
    public void testCheckEncrypted_BCryptFailureButSHA256WithSaltSuccess() {
        String password = "testpass";
        String salt = "testsalt";
        String sha256WithSaltHash = UserDao.encodePassword(password, salt);

        // Test when BCrypt fails but SHA256 with salt succeeds
        assertTrue(UserDao.checkEncrypted(password, sha256WithSaltHash, salt));
    }

    @Test
    public void testCheckEncrypted_BCryptAndSHA256WithSaltFailureButSHA256Success() {
        String password = "testpass";
        String sha256Hash = getSha256Hash(password);

        // Test when both BCrypt and SHA256 with salt fail but SHA256 succeeds
        assertTrue(UserDao.checkEncrypted(password, sha256Hash, "testsalt"));
    }

    @Test
    public void testCheckEncrypted_AllMethodsFail() {
        String password = "testpass";
        String wrongHash = "wronghash";

        // Test when all validation methods fail
        assertFalse(UserDao.checkEncrypted(password, wrongHash, "testsalt"));
    }

    @Test
    public void testEncodePassword_EmptyPassword() {
        String password = "";
        String salt = "testsalt";

        String encoded = UserDao.encodePassword(password, salt);
        assertNotNull(encoded);
    }

    @Test
    public void testEncodePassword_EmptySalt() {
        String password = "testpass";
        String salt = "";

        String encoded = UserDao.encodePassword(password, salt);
        assertNotNull(encoded);
    }

    @Test
    public void testMergePasswordAndSalt_EmptySalt() {
        String result = UserDao.mergePasswordAndSalt("password", "", false);
        assertEquals("password", result);
    }

    @Test
    public void testMergePasswordAndSalt_StrictModeWithValidSalt() {
        String result = UserDao.mergePasswordAndSalt("password", "salt", true);
        assertEquals("password{salt}", result);
    }

    @Test
    public void testGetSha256Hash_Success() {
        String password = "testpassword";
        String hash = getSha256Hash(password);
        assertNotNull(hash);
        assertTrue(hash.length() > 0);
    }

    @Test
    public void testGetSha256Hash_EmptyString() {
        String password = "";
        String hash = getSha256Hash(password);
        assertNotNull(hash);
        assertTrue(hash.length() > 0);
    }

    @Test
    public void testCheckPasswordBCrypt_Failure() {
        // Test BCrypt failure scenario
        String password = "testpass";
        String wrongHash = "wronghash";

        // This should not throw an exception, just return false
        // The method is private, so we test it indirectly through checkEncrypted
        assertFalse(UserDao.checkEncrypted(password, wrongHash, "testsalt"));
    }

    @Test
    public void testCheckPasswordSHA256WithSalt_Failure() {
        // Test SHA256 with salt failure scenario
        String password = "testpass";
        String wrongHash = "wronghash";
        String salt = "testsalt";

        // This should not throw an exception, just return false
        // The method is private, so we test it indirectly through checkEncrypted
        assertFalse(UserDao.checkEncrypted(password, wrongHash, salt));
    }

    @Test
    public void testCheckPasswordSHA256_Failure() {
        // Test SHA256 failure scenario
        String password = "testpass";
        String wrongHash = "wronghash";

        // This should not throw an exception, just return false
        // The method is private, so we test it indirectly through checkEncrypted
        assertFalse(UserDao.checkEncrypted(password, wrongHash, "testsalt"));
    }

    // Helper method to access private method for testing
    private String getSha256Hash(String base) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(base.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();

            for (byte aHash : hash) {
                String hex = Integer.toHexString(0xff & aHash);

                if (hex.length() == 1) {
                    hexString.append('0');
                }

                hexString.append(hex);
            }

            return hexString.toString();
        } catch (Exception ex) {
            throw new RuntimeException("Exception while encoding password.", ex);
        }
    }
}
