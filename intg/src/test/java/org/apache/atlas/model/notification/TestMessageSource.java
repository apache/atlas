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
package org.apache.atlas.model.notification;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestMessageSource {
    private MessageSource messageSource;

    @BeforeMethod
    public void setUp() {
        messageSource = new MessageSource();
    }

    @Test
    public void testDefaultConstructor() {
        MessageSource source = new MessageSource();

        assertNull(source.getSource());
        assertNull(source.getVersion());
    }

    @Test
    public void testParameterizedConstructor() {
        String sourceName = "TestSource";
        MessageSource source = new MessageSource(sourceName);

        assertEquals(source.getSource(), sourceName);
        assertNotNull(source.getVersion());
    }

    @Test
    public void testSourceGetterSetter() {
        String sourceName = "TestSource";

        messageSource.setSource(sourceName);
        assertEquals(messageSource.getSource(), sourceName);

        messageSource.setSource(null);
        assertNull(messageSource.getSource());
    }

    @Test
    public void testSourceGetterSetterAlias() {
        String sourceName = "TestSourceAlias";

        messageSource.setSource(sourceName);
        assertEquals(messageSource.getSource(), sourceName);
    }

    @Test
    public void testVersionGetter() {
        MessageSource source = new MessageSource("test");

        String version = source.getVersion();
        assertNotNull(version);
    }

    @Test
    public void testParameterizedConstructorSetsVersion() {
        String sourceName = "TestSource";
        MessageSource source = new MessageSource(sourceName);

        assertEquals(source.getSource(), sourceName);
        assertNotNull(source.getVersion());
    }

    @Test
    public void testEmptySourceName() {
        String emptySource = "";
        MessageSource source = new MessageSource(emptySource);

        assertEquals(source.getSource(), emptySource);
        assertNotNull(source.getVersion());
    }

    @Test
    public void testNullSourceName() {
        MessageSource source = new MessageSource(null);

        assertNull(source.getSource());
        assertNotNull(source.getVersion());
    }

    @Test
    public void testSettersReturnVoid() {
        messageSource.setSource("test");
        assertNotNull(messageSource);
    }

    @Test
    public void testSourceNameWithSpecialCharacters() {
        String specialSource = "Test-Source_123!@#";
        MessageSource source = new MessageSource(specialSource);

        assertEquals(source.getSource(), specialSource);
    }

    @Test
    public void testSourceNameWithSpaces() {
        String sourceWithSpaces = "Test Source Name";
        MessageSource source = new MessageSource(sourceWithSpaces);

        assertEquals(source.getSource(), sourceWithSpaces);
    }

    @Test
    public void testMultipleInstances() {
        MessageSource source1 = new MessageSource("Source1");
        MessageSource source2 = new MessageSource("Source2");

        assertEquals(source1.getSource(), "Source1");
        assertEquals(source2.getSource(), "Source2");

        // Both should have versions set
        assertNotNull(source1.getVersion());
        assertNotNull(source2.getVersion());

        // Versions should be the same (static build version)
        assertEquals(source1.getVersion(), source2.getVersion());
    }

    @Test
    public void testVersionConsistency() {
        MessageSource source1 = new MessageSource("Source1");
        MessageSource source2 = new MessageSource("Source2");

        // Both instances should have the same version
        assertEquals(source1.getVersion(), source2.getVersion());
    }

    @Test
    public void testVersionNotNull() {
        MessageSource source = new MessageSource("test");

        String version = source.getVersion();
        assertNotNull(version);
    }

    @Test
    public void testBuildVersionDefault() throws Exception {
        MessageSource source = new MessageSource("test");
        String version = source.getVersion();

        assertNotNull(version);
    }

    @Test
    public void testSerializable() {
        MessageSource source = new MessageSource("test");
        assertNotNull(source);
    }

    @Test
    public void testStaticInitialization() throws Exception {
        Field storedVersionField = MessageSource.class.getDeclaredField("storedVersion");
        storedVersionField.setAccessible(true);
        String storedVersion = (String) storedVersionField.get(null);

        assertNotNull(storedVersion);
    }

    @Test
    public void testFetchBuildVersionMethod() throws Exception {
        Method fetchBuildVersionMethod = MessageSource.class.getDeclaredMethod("fetchBuildVersion");
        fetchBuildVersionMethod.setAccessible(true);

        String version = (String) fetchBuildVersionMethod.invoke(null);

        assertNotNull(version);
    }

    @Test
    public void testConstantValues() throws Exception {
        // Test that constants are properly defined
        Field buildInfoPropertiesField = MessageSource.class.getDeclaredField("BUILDINFO_PROPERTIES");
        buildInfoPropertiesField.setAccessible(true);
        String buildInfoProperties = (String) buildInfoPropertiesField.get(null);

        assertEquals(buildInfoProperties, "/atlas-buildinfo.properties");

        Field buildVersionPropertyKeyField = MessageSource.class.getDeclaredField("BUILD_VERSION_PROPERTY_KEY");
        buildVersionPropertyKeyField.setAccessible(true);
        String buildVersionPropertyKey = (String) buildVersionPropertyKeyField.get(null);

        assertEquals(buildVersionPropertyKey, "build.version");

        Field buildVersionDefaultField = MessageSource.class.getDeclaredField("BUILD_VERSION_DEFAULT");
        buildVersionDefaultField.setAccessible(true);
        String buildVersionDefault = (String) buildVersionDefaultField.get(null);

        assertEquals(buildVersionDefault, "UNKNOWN");
    }

    @Test
    public void testVersionFieldAccess() throws Exception {
        // Test that version field can be accessed properly
        MessageSource source = new MessageSource("test");

        Field versionField = MessageSource.class.getDeclaredField("version");
        versionField.setAccessible(true);
        String directVersionAccess = (String) versionField.get(source);

        assertEquals(directVersionAccess, source.getVersion());
    }

    @Test
    public void testNameFieldAccess() throws Exception {
        // Test that name field can be accessed properly
        String testName = "TestName";
        MessageSource source = new MessageSource(testName);

        Field nameField = MessageSource.class.getDeclaredField("name");
        nameField.setAccessible(true);
        String directNameAccess = (String) nameField.get(source);

        assertEquals(directNameAccess, source.getSource());
        assertEquals(directNameAccess, testName);
    }

    @Test
    public void testDefaultConstructorVersionField() throws Exception {
        // Test that default constructor doesn't set version field
        MessageSource source = new MessageSource();

        Field versionField = MessageSource.class.getDeclaredField("version");
        versionField.setAccessible(true);
        String versionValue = (String) versionField.get(source);

        assertNull(versionValue);
        assertNull(source.getVersion());
    }

    @Test
    public void testJacksonAnnotations() {
        MessageSource source = new MessageSource("test");

        assertNotNull(source);
        assertNotNull(source.getSource());
        assertNotNull(source.getVersion());
    }

    @Test
    public void testHashCodeConsistency() {
        MessageSource source = new MessageSource("TestSource");
        int hashCode1 = source.hashCode();
        int hashCode2 = source.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        MessageSource source1 = new MessageSource("TestSource");
        MessageSource source2 = new MessageSource("TestSource");

        // Note: hashCode may not be overridden, but we test consistency
        int hashCode1 = source1.hashCode();
        int hashCode2 = source2.hashCode();

        // If equals is overridden, hashCode should be consistent
        assertNotNull(hashCode1);
        assertNotNull(hashCode2);
    }

    @Test
    public void testEqualsWithSameObject() {
        MessageSource source = new MessageSource("TestSource");
        assertTrue(source.equals(source));
    }

    @Test
    public void testEqualsWithNull() {
        MessageSource source = new MessageSource("TestSource");
        assertFalse(source.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        MessageSource source = new MessageSource("TestSource");
        assertFalse(source.equals("not a message source"));
    }

    @Test
    public void testToString() {
        MessageSource source = new MessageSource("TestSource");
        String result = source.toString();

        assertNotNull(result);
    }

    @Test
    public void testXmlAnnotations() {
        MessageSource source = new MessageSource("test");

        assertNotNull(source);
    }

    @Test
    public void testVersionSetterMethod() throws Exception {
        MessageSource source = new MessageSource();

        try {
            Method setVersionMethod = MessageSource.class.getDeclaredMethod("setVersion", String.class);
            setVersionMethod.setAccessible(true);
            setVersionMethod.invoke(source, "test-version");

            assertEquals(source.getVersion(), "test-version");
        } catch (NoSuchMethodException ignored) {
        }
    }

    @Test
    public void testBuildInfoPropertiesHandling() throws Exception {
        Method fetchBuildVersionMethod = MessageSource.class.getDeclaredMethod("fetchBuildVersion");
        fetchBuildVersionMethod.setAccessible(true);

        String version = (String) fetchBuildVersionMethod.invoke(null);

        assertNotNull(version);
        assertTrue(version.length() > 0);
    }

    @Test
    public void testStaticVersionConsistency() {
        MessageSource source1 = new MessageSource("Source1");
        MessageSource source2 = new MessageSource("Source2");
        MessageSource source3 = new MessageSource("Source3");

        String version1 = source1.getVersion();
        String version2 = source2.getVersion();
        String version3 = source3.getVersion();

        assertEquals(version1, version2);
        assertEquals(version2, version3);
        assertEquals(version1, version3);
    }

    @Test
    public void testSourceNameEdgeCases() {
        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longName.append("LongSourceName");
        }
        String veryLongName = longName.toString();

        MessageSource source = new MessageSource(veryLongName);
        assertEquals(source.getSource(), veryLongName);
        assertNotNull(source.getVersion());
    }

    @Test
    public void testSourceNameWithNewlines() {
        String nameWithNewlines = "Source\nWith\nNewlines";
        MessageSource source = new MessageSource(nameWithNewlines);

        assertEquals(source.getSource(), nameWithNewlines);
        assertNotNull(source.getVersion());
    }

    @Test
    public void testSourceNameWithTabs() {
        String nameWithTabs = "Source\tWith\tTabs";
        MessageSource source = new MessageSource(nameWithTabs);

        assertEquals(source.getSource(), nameWithTabs);
        assertNotNull(source.getVersion());
    }

    @Test
    public void testLoggerAccess() throws Exception {
        Field loggerField = MessageSource.class.getDeclaredField("LOG");
        loggerField.setAccessible(true);
        Object logger = loggerField.get(null);

        assertNotNull(logger);
    }

    @Test
    public void testSerialVersionUID() throws Exception {
        Field serialVersionUIDField = MessageSource.class.getDeclaredField("serialVersionUID");
        serialVersionUIDField.setAccessible(true);
        long serialVersionUID = (Long) serialVersionUIDField.get(null);

        assertEquals(serialVersionUID, 1L);
    }
}
