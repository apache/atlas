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
package org.apache.atlas.utils;

import org.apache.atlas.security.SecurityProperties;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class SSLUtilTest {
    private SSLUtil sslUtil = new SSLUtil();

    @Test
    public void testSSLUtilConstants() {
        assertEquals(SSLUtil.ATLAS_KEYSTORE_FILE_TYPE_DEFAULT, "jks");
        assertEquals(SSLUtil.ATLAS_TRUSTSTORE_FILE_TYPE_DEFAULT, "jks");
        assertEquals(SSLUtil.ATLAS_TLS_CONTEXT_ALGO_TYPE, "TLS");
        assertNotNull(SSLUtil.ATLAS_TLS_KEYMANAGER_DEFAULT_ALGO_TYPE);
        assertNotNull(SSLUtil.ATLAS_TLS_TRUSTMANAGER_DEFAULT_ALGO_TYPE);
    }

    @Test
    public void testSetSSLContextWithoutTLS() {
        // Create a mock configuration with TLS disabled
        Configuration mockConfig = new BaseConfiguration();
        mockConfig.setProperty(SecurityProperties.TLS_ENABLED, false);
        // Create a SSLUtil instance with mocked configuration
        SSLUtil testSSLUtil = new SSLUtil() {
            @Override
            protected Configuration getConfiguration() {
                return mockConfig;
            }
        };
        // Should complete without throwing exception when TLS is disabled
        testSSLUtil.setSSLContext();
    }

    @Test
    public void testGetFileInputStreamWithExistingFile() throws Exception {
        // Create a temporary file for testing
        Path tempFile = Files.createTempFile("test-file", ".txt");
        Files.write(tempFile, "test content".getBytes());
        try {
            Method getFileInputStreamMethod = SSLUtil.class.getDeclaredMethod("getFileInputStream", String.class);
            getFileInputStreamMethod.setAccessible(true);
            InputStream result = (InputStream) getFileInputStreamMethod.invoke(sslUtil, tempFile.toString());
            assertNotNull(result);
            result.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    public void testGetFileInputStreamWithNonExistentFile() throws Exception {
        try {
            Method getFileInputStreamMethod = SSLUtil.class.getDeclaredMethod("getFileInputStream", String.class);
            getFileInputStreamMethod.setAccessible(true);
            InputStream result = (InputStream) getFileInputStreamMethod.invoke(sslUtil, "nonexistent-file.txt");
            // Should try to load from classpath and may return null if not found
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetFileInputStreamWithEmptyFileName() throws Exception {
        try {
            Method getFileInputStreamMethod = SSLUtil.class.getDeclaredMethod("getFileInputStream", String.class);
            getFileInputStreamMethod.setAccessible(true);
            InputStream result = (InputStream) getFileInputStreamMethod.invoke(sslUtil, "");
            assertNull(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetFileInputStreamWithNullFileName() throws Exception {
        try {
            Method getFileInputStreamMethod = SSLUtil.class.getDeclaredMethod("getFileInputStream", String.class);
            getFileInputStreamMethod.setAccessible(true);
            InputStream result = (InputStream) getFileInputStreamMethod.invoke(sslUtil, (String) null);
            assertNull(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCloseInputStream() throws Exception {
        InputStream mockInputStream = mock(InputStream.class);
        try {
            Method closeMethod = SSLUtil.class.getDeclaredMethod("close", InputStream.class, String.class);
            closeMethod.setAccessible(true);
            closeMethod.invoke(sslUtil, mockInputStream, "test-file.txt");
            // Should not throw exception
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCloseInputStreamWithIOException() throws Exception {
        InputStream mockInputStream = mock(InputStream.class);
        doThrow(new IOException("Test IOException")).when(mockInputStream).close();
        try {
            Method closeMethod = SSLUtil.class.getDeclaredMethod("close", InputStream.class, String.class);
            closeMethod.setAccessible(true);
            closeMethod.invoke(sslUtil, mockInputStream, "test-file.txt");
            // Should handle IOException gracefully
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCloseNullInputStream() throws Exception {
        try {
            Method closeMethod = SSLUtil.class.getDeclaredMethod("close", InputStream.class, String.class);
            closeMethod.setAccessible(true);
            closeMethod.invoke(sslUtil, null, "test-file.txt");
            // Should handle null input stream gracefully
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testIsTLSEnabledTrue() throws Exception {
        Configuration mockConfig = new BaseConfiguration();
        mockConfig.setProperty(SecurityProperties.TLS_ENABLED, true);
        SSLUtil testSSLUtil = new SSLUtil() {
            @Override
            protected Configuration getConfiguration() {
                return mockConfig;
            }
        };
        // Use reflection to test private method
        try {
            Method isTLSEnabledMethod = SSLUtil.class.getDeclaredMethod("isTLSEnabled");
            isTLSEnabledMethod.setAccessible(true);
            boolean result = (boolean) isTLSEnabledMethod.invoke(testSSLUtil);
            assertEquals(result, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testIsTLSEnabledFalse() throws Exception {
        Configuration mockConfig = new BaseConfiguration();
        mockConfig.setProperty(SecurityProperties.TLS_ENABLED, false);
        SSLUtil testSSLUtil = new SSLUtil() {
            @Override
            protected Configuration getConfiguration() {
                return mockConfig;
            }
        };
        // Use reflection to test private method
        try {
            Method isTLSEnabledMethod = SSLUtil.class.getDeclaredMethod("isTLSEnabled");
            isTLSEnabledMethod.setAccessible(true);
            boolean result = (boolean) isTLSEnabledMethod.invoke(testSSLUtil);
            assertEquals(result, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetKeyManagersWithEmptyKeystore() throws Exception {
        Configuration mockConfig = new BaseConfiguration();
        // No keystore configured
        SSLUtil testSSLUtil = new SSLUtil() {
            @Override
            protected Configuration getConfiguration() {
                return mockConfig;
            }
        };
        try {
            Method getKeyManagersMethod = SSLUtil.class.getDeclaredMethod("getKeyManagers");
            getKeyManagersMethod.setAccessible(true);
            Object result = getKeyManagersMethod.invoke(testSSLUtil);
            assertNull(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetTrustManagersWithEmptyTruststore() throws Exception {
        Configuration mockConfig = new BaseConfiguration();
        // No truststore configured
        SSLUtil testSSLUtil = new SSLUtil() {
            @Override
            protected Configuration getConfiguration() {
                return mockConfig;
            }
        };
        try {
            Method getTrustManagersMethod = SSLUtil.class.getDeclaredMethod("getTrustManagers");
            getTrustManagersMethod.setAccessible(true);
            Object result = getTrustManagersMethod.invoke(testSSLUtil);
            assertNull(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSetSSLContextWithTLSEnabled() throws Exception {
        Configuration mockConfig = new BaseConfiguration();
        mockConfig.setProperty(SecurityProperties.TLS_ENABLED, true);
        // No certificate stores configured, should handle gracefully
        SSLUtil testSSLUtil = new SSLUtil() {
            @Override
            protected Configuration getConfiguration() {
                return mockConfig;
            }
        };
        // Should complete without throwing exception even with no certs configured
        testSSLUtil.setSSLContext();
    }

    // Note: Methods that depend on static SecurityUtil calls are simplified
    // as static mocking is not available in Mockito 1.8.5.
    // For full coverage of SSL functionality, integration tests with real
    // keystore files would be needed.
}
