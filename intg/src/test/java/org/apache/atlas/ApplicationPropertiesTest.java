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
package org.apache.atlas;

import org.apache.atlas.utils.AtlasConfigurationUtil;
import org.apache.commons.configuration.Configuration;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

/**
 * Unit test for {@link ApplicationProperties}
 *
 */

public class ApplicationPropertiesTest {
    @Test
    public void testGetFileAsInputStream() throws Exception {
        Configuration props = ApplicationProperties.get("test.properties");
        InputStream   inStr = null;

        // configured file as class loader resource
        try {
            inStr = ApplicationProperties.getFileAsInputStream(props, "jaas.properties.file", null);
            assertNotNull(inStr);
        } finally {
            if (inStr != null) {
                inStr.close();
            }
        }

        // configured file from file system path
        props.setProperty("jaas.properties.file", "src/test/resources/atlas-jaas.properties");
        try {
            inStr = ApplicationProperties.getFileAsInputStream(props, "jaas.properties.file", null);

            assertNotNull(inStr);
        } finally {
            if (inStr != null) {
                inStr.close();
            }
        }

        // default file as class loader resource
        try {
            inStr = ApplicationProperties.getFileAsInputStream(props, "property.not.specified.in.config", "atlas-jaas.properties");

            assertNotNull(inStr);
        } finally {
            if (inStr != null) {
                inStr.close();
            }
        }

        // default file relative to working directory
        try {
            inStr = ApplicationProperties.getFileAsInputStream(props, "property.not.specified.in.config", "src/test/resources/atlas-jaas.properties");

            assertNotNull(inStr);
        } finally {
            if (inStr != null) {
                inStr.close();
            }
        }

        // default file relative to atlas configuration directory
        String originalConfDirSetting = System.setProperty(ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY, "src/test/resources");

        try {
            inStr = ApplicationProperties.getFileAsInputStream(props, "property.not.specified.in.config", "atlas-jaas.properties");

            assertNotNull(inStr);
        } finally {
            if (inStr != null) {
                inStr.close();
            }
            if (originalConfDirSetting != null) {
                System.setProperty(ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY, originalConfDirSetting);
            } else {
                System.clearProperty(ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY);
            }
        }

        // non-existent property and no default file
        try {
            inStr = ApplicationProperties.getFileAsInputStream(props, "property.not.specified.in.config", null);

            fail("Expected " + AtlasException.class.getSimpleName() + " but none thrown");
        } catch (AtlasException e) {
            // good
        } finally {
            if (inStr != null) {
                inStr.close();
            }
        }

        // configured file not found in file system or classpath
        props.setProperty("jaas.properties.file", "does_not_exist.txt");

        try {
            inStr = ApplicationProperties.getFileAsInputStream(props, "jaas.properties.file", null);

            fail("Expected " + AtlasException.class.getSimpleName() + " but none thrown");
        } catch (AtlasException e) {
            // good
        } finally {
            if (inStr != null) {
                inStr.close();
            }
        }
    }

    @Test
    public void verifySetDefault() throws AtlasException {
        Configuration props = ApplicationProperties.get("test.properties");
        ApplicationProperties aProps = (ApplicationProperties) props;

        String defaultValue = "someValue";
        String someKey = "someKey";
        AbstractMap.SimpleEntry<String, String> defaultKV = new AbstractMap.SimpleEntry<>(someKey, defaultValue);

        aProps.setDefault(defaultKV, "newValue");

        assertNotEquals(props.getString(someKey), defaultValue);
        aProps.setDefault(defaultKV, "");
        assertEquals(props.getString(someKey), defaultValue);
    }

    @Test
    public void verifyGetLatesttString() throws AtlasException {
        String key = "atlas.metadata.namespace";
        String oldVal = "nm-sp-1";
        String newVal = "nm-sp-2";
        Configuration atlasConf = ApplicationProperties.get("test.properties");

        assertEquals(atlasConf.getString(key), oldVal);
        assertEquals(AtlasConfigurationUtil.getRecentString(atlasConf, key, oldVal), newVal);
        assertEquals(AtlasConfigurationUtil.getRecentString(atlasConf, "garbage", oldVal), oldVal);
    }

    @Test
    public void verifyCustomisedPath() throws AtlasException, IOException {
        Configuration atlasConf = ApplicationProperties.getConf("src/test/resources/test.properties");
        InputStream inStr = null;

        try {
            inStr = ApplicationProperties.getFileAsInputStream(atlasConf, "jaas.properties.file", null);
            assertNotNull(inStr);
        } finally {
            if (inStr != null) {
                inStr.close();
            }
        }
    }

    @Test
    public void verifyPropertyValues() throws AtlasException {
        Configuration props = ApplicationProperties.getConf("src/test/resources/test.properties");
        ApplicationProperties aProps = (ApplicationProperties) props;

        String defaultValue = "atlas";
        String someKey = "atlas.service";

        assertFalse(aProps.getString(someKey).equals(defaultValue));
    }

    @Test
    public void verifyCustomisedPathFailureExpected() {
        try (MockedStatic<ApplicationProperties> mocked = Mockito.mockStatic(ApplicationProperties.class)) {
            mocked.when(() -> ApplicationProperties.getConf("src/test/resources/incorrectfile.properties"))
                    .thenThrow(new AtlasException("Failed to load application properties"));

            AtlasException ex = expectThrows(
                    AtlasException.class,
                    () -> ApplicationProperties.getConf("src/test/resources/incorrectfile.properties"));

            assertTrue(
                    ex.getMessage().contains("Failed to load application properties"),
                    "Exception message mismatch!");
        }
    }

    @Test
    public void verifyClientConfiguration_withStaticMock_onlyUsingMockitoMock() throws Exception {
        Configuration mockConf = Mockito.mock(Configuration.class);

        Mockito.when(mockConf.getString("atlas.server.url")).thenReturn("http://localhost:21000");
        Mockito.when(mockConf.getString("atlas.login.method")).thenReturn("basic");

        try (MockedStatic<ApplicationProperties> mocked = Mockito.mockStatic(ApplicationProperties.class)) {
            mocked.when(() -> ApplicationProperties.getConf(Mockito.any(Configuration.class)))
                    .thenReturn(mockConf);
            Configuration result = ApplicationProperties.getConf(mockConf);

            assertSame(mockConf, result);
            assertEquals(result.getString("atlas.server.url"), "http://localhost:21000");
            assertEquals(result.getString("atlas.login.method"), "basic");
        }
    }
}
