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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class AtlasConfigurationUtilTest {
    @Test
    public void testGetRecentStringWithSingleValue() {
        Configuration config = new BaseConfiguration();
        config.addProperty("test.key", "value1");

        String result = AtlasConfigurationUtil.getRecentString(config, "test.key");
        assertEquals(result, "value1");
    }

    @Test
    public void testGetRecentStringWithMultipleValues() {
        Configuration config = new BaseConfiguration();
        config.addProperty("test.key", "value1");
        config.addProperty("test.key", "value2");
        config.addProperty("test.key", "value3");

        String result = AtlasConfigurationUtil.getRecentString(config, "test.key");
        assertEquals(result, "value3");
    }

    @Test
    public void testGetRecentStringWithEmptyArray() {
        Configuration config = new BaseConfiguration();

        String result = AtlasConfigurationUtil.getRecentString(config, "nonexistent.key");
        assertNull(result);
    }

    @Test
    public void testGetRecentStringWithDefaultValue() {
        Configuration config = new BaseConfiguration();

        String result = AtlasConfigurationUtil.getRecentString(config, "nonexistent.key", "defaultValue");
        assertEquals(result, "defaultValue");
    }

    @Test
    public void testGetRecentStringWithDefaultValueAndExistingKey() {
        Configuration config = new BaseConfiguration();
        config.addProperty("test.key", "value1");
        config.addProperty("test.key", "value2");

        String result = AtlasConfigurationUtil.getRecentString(config, "test.key", "defaultValue");
        assertEquals(result, "value2");
    }

    @Test
    public void testGetRecentStringWithDefaultValueAndEmptyArray() {
        Configuration config = new BaseConfiguration();

        String result = AtlasConfigurationUtil.getRecentString(config, "nonexistent.key", "defaultValue");
        assertEquals(result, "defaultValue");
    }

    @Test
    public void testGetRecentStringWithNullDefaultValue() {
        Configuration config = new BaseConfiguration();

        String result = AtlasConfigurationUtil.getRecentString(config, "nonexistent.key", null);
        assertNull(result);
    }

    @Test
    public void testPrivateConstructor() throws Exception {
        Constructor<AtlasConfigurationUtil> constructor = AtlasConfigurationUtil.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        try {
            constructor.newInstance();
        } catch (InvocationTargetException e) {
        }
    }

    @Test
    public void testGetRecentStringWithSingleElementArray() {
        Configuration config = new BaseConfiguration();
        config.setProperty("test.key", new String[] {"onlyValue"});
        String result = AtlasConfigurationUtil.getRecentString(config, "test.key");
        assertEquals(result, "onlyValue");
    }

    @Test
    public void testGetRecentStringTwoParameterOverload() {
        Configuration config = new BaseConfiguration();
        config.addProperty("test.key", "value1");

        String result = AtlasConfigurationUtil.getRecentString(config, "test.key");
        assertEquals(result, "value1");
    }
}
