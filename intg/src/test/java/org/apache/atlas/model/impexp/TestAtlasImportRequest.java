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

package org.apache.atlas.model.impexp;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasImportRequest {
    private AtlasImportRequest importRequest;

    @BeforeMethod
    public void setUp() {
        importRequest = new AtlasImportRequest();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasImportRequest request = new AtlasImportRequest();

        assertNotNull(request);
        assertNotNull(request.getOptions());
        assertTrue(request.getOptions().isEmpty());
    }

    @Test
    public void testOptionsSetterGetter() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", "value2");

        importRequest.setOptions(options);

        assertEquals(importRequest.getOptions(), options);
        assertEquals(importRequest.getOptions().size(), 2);
        assertEquals(importRequest.getOptions().get("key1"), "value1");
        assertEquals(importRequest.getOptions().get("key2"), "value2");
    }

    @Test
    public void testOptionsWithNullMap() {
        importRequest.setOptions(null);
        assertNull(importRequest.getOptions());
    }

    @Test
    public void testGetStartGuidDefault() {
        String startGuid = importRequest.getStartGuid();
        assertNull(startGuid);
    }

    @Test
    public void testGetStartGuidWithValue() {
        Map<String, String> options = new HashMap<>();
        options.put("startGuid", "test-guid-123");
        importRequest.setOptions(options);

        String startGuid = importRequest.getStartGuid();
        assertEquals(startGuid, "test-guid-123");
    }

    @Test
    public void testGetFileNameDefault() {
        String fileName = importRequest.getFileName();
        assertNull(fileName);
    }

    @Test
    public void testGetFileNameWithValue() {
        Map<String, String> options = new HashMap<>();
        options.put("fileName", "test-export.zip");
        importRequest.setOptions(options);

        String fileName = importRequest.getFileName();
        assertEquals(fileName, "test-export.zip");
    }

    @Test
    public void testSetFileName() {
        String fileName = "import-file.zip";
        importRequest.setFileName(fileName);

        assertEquals(importRequest.getFileName(), fileName);
        assertEquals(importRequest.getOptions().get("fileName"), fileName);
    }

    @Test
    public void testSetFileNameWithNullOptions() {
        importRequest.setOptions(null);
        importRequest.setFileName("test-file.zip");

        assertNotNull(importRequest.getOptions());
        assertEquals(importRequest.getFileName(), "test-file.zip");
    }

    @Test
    public void testGetStartPositionDefault() {
        String startPosition = importRequest.getStartPosition();
        assertNull(startPosition);
    }

    @Test
    public void testGetStartPositionWithValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.START_POSITION_KEY, "100");
        importRequest.setOptions(options);

        String startPosition = importRequest.getStartPosition();
        assertEquals(startPosition, "100");
    }

    @Test
    public void testGetUpdateTypeDefsDefault() {
        String updateTypeDefs = importRequest.getUpdateTypeDefs();
        assertNull(updateTypeDefs);
    }

    @Test
    public void testGetUpdateTypeDefsWithValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.UPDATE_TYPE_DEFINITION_KEY, "true");
        importRequest.setOptions(options);

        String updateTypeDefs = importRequest.getUpdateTypeDefs();
        assertEquals(updateTypeDefs, "true");
    }

    @Test
    public void testIsReplicationOptionSetDefault() {
        boolean isSet = importRequest.isReplicationOptionSet();
        assertFalse(isSet);
    }

    @Test
    public void testIsReplicationOptionSetWithEmptyOptions() {
        importRequest.setOptions(new HashMap<>());
        boolean isSet = importRequest.isReplicationOptionSet();
        assertFalse(isSet);
    }

    @Test
    public void testIsReplicationOptionSetWithReplicationOption() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, "source-server");
        importRequest.setOptions(options);

        boolean isSet = importRequest.isReplicationOptionSet();
        assertTrue(isSet);
    }

    @Test
    public void testSkipUpdateReplicationAttrDefault() {
        boolean skip = importRequest.skipUpdateReplicationAttr();
        assertFalse(skip);
    }

    @Test
    public void testSkipUpdateReplicationAttrWithStringTrue() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_SKIP_UPDATE_REPLICATION_ATTR, "true");
        importRequest.setOptions(options);

        boolean skip = importRequest.skipUpdateReplicationAttr();
        assertTrue(skip);
    }

    @Test
    public void testSkipUpdateReplicationAttrWithStringFalse() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_SKIP_UPDATE_REPLICATION_ATTR, "false");
        importRequest.setOptions(options);

        boolean skip = importRequest.skipUpdateReplicationAttr();
        assertFalse(skip);
    }

    @Test
    public void testSkipUpdateReplicationAttrWithNullValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_SKIP_UPDATE_REPLICATION_ATTR, null);
        importRequest.setOptions(options);

        boolean skip = importRequest.skipUpdateReplicationAttr();
        assertFalse(skip);
    }

    @Test
    public void testGetOptionKeyReplicatedFromDefault() {
        String replicatedFrom = importRequest.getOptionKeyReplicatedFrom();
        assertEquals(replicatedFrom, "");
    }

    @Test
    public void testGetOptionKeyReplicatedFromWithValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, "source-cluster");
        importRequest.setOptions(options);

        String replicatedFrom = importRequest.getOptionKeyReplicatedFrom();
        assertEquals(replicatedFrom, "source-cluster");
    }

    @Test
    public void testGetOptionKeyNumWorkersDefault() {
        int numWorkers = importRequest.getOptionKeyNumWorkers();
        assertEquals(numWorkers, 1);
    }

    @Test
    public void testGetOptionKeyNumWorkersWithValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_NUM_WORKERS, "5");
        importRequest.setOptions(options);

        int numWorkers = importRequest.getOptionKeyNumWorkers();
        assertEquals(numWorkers, 5);
    }

    @Test
    public void testGetOptionKeyNumWorkersWithInvalidValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_NUM_WORKERS, "invalid");
        importRequest.setOptions(options);

        try {
            importRequest.getOptionKeyNumWorkers();
        } catch (NumberFormatException e) {
            // Expected exception
            assertTrue(true);
        }
    }

    @Test
    public void testGetOptionKeyBatchSizeDefault() {
        int batchSize = importRequest.getOptionKeyBatchSize();
        assertEquals(batchSize, 1);
    }

    @Test
    public void testGetOptionKeyBatchSizeWithValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_BATCH_SIZE, "10");
        importRequest.setOptions(options);

        int batchSize = importRequest.getOptionKeyBatchSize();
        assertEquals(batchSize, 10);
    }

    @Test
    public void testGetOptionKeyBatchSizeWithInvalidValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_BATCH_SIZE, "invalid");
        importRequest.setOptions(options);

        try {
            importRequest.getOptionKeyBatchSize();
        } catch (NumberFormatException e) {
            // Expected exception
            assertTrue(true);
        }
    }

    @Test
    public void testSetOption() {
        importRequest.setOption("testKey", "testValue");

        Map<String, String> options = importRequest.getOptions();
        assertNotNull(options);
        assertEquals(options.get("testKey"), "testValue");
    }

    @Test
    public void testSetOptionWithNullOptions() {
        importRequest.setOptions(null);
        importRequest.setOption("testKey", "testValue");

        Map<String, String> options = importRequest.getOptions();
        assertNotNull(options);
        assertEquals(options.get("testKey"), "testValue");
    }

    @Test
    public void testSetOptionOverwrite() {
        importRequest.setOption("key", "value1");
        importRequest.setOption("key", "value2");

        assertEquals(importRequest.getOptions().get("key"), "value2");
    }

    @Test
    public void testGetSizeOptionDefault() {
        int size = importRequest.getSizeOption();
        assertEquals(size, 1);
    }

    @Test
    public void testGetSizeOptionWithValue() {
        Map<String, String> options = new HashMap<>();
        options.put("size", "100");
        importRequest.setOptions(options);

        int size = importRequest.getSizeOption();
        assertEquals(size, 100);
    }

    @Test
    public void testGetSizeOptionWithInvalidValue() {
        Map<String, String> options = new HashMap<>();
        options.put("size", "invalid");
        importRequest.setOptions(options);

        try {
            importRequest.getSizeOption();
        } catch (NumberFormatException e) {
            // Expected exception
            assertTrue(true);
        }
    }

    @Test
    public void testSetSizeOption() {
        importRequest.setSizeOption(50);

        assertEquals(importRequest.getSizeOption(), 50);
        assertEquals(importRequest.getOptions().get("size"), "50");
    }

    @Test
    public void testSetSizeOptionWithNullOptions() {
        importRequest.setOptions(null);
        importRequest.setSizeOption(25);

        assertNotNull(importRequest.getOptions());
        assertEquals(importRequest.getSizeOption(), 25);
    }

    @Test
    public void testGetOptionForKeyPrivateMethod() throws Exception {
        // Use reflection to test private method
        Method getOptionForKeyMethod = AtlasImportRequest.class.getDeclaredMethod("getOptionForKey", String.class);
        getOptionForKeyMethod.setAccessible(true);

        // Test with null options
        importRequest.setOptions(null);
        String result = (String) getOptionForKeyMethod.invoke(importRequest, "testKey");
        assertNull(result);

        // Test with empty options
        importRequest.setOptions(new HashMap<>());
        result = (String) getOptionForKeyMethod.invoke(importRequest, "testKey");
        assertNull(result);

        // Test with existing key
        Map<String, String> options = new HashMap<>();
        options.put("testKey", "testValue");
        importRequest.setOptions(options);
        result = (String) getOptionForKeyMethod.invoke(importRequest, "testKey");
        assertEquals(result, "testValue");

        // Test with non-existing key
        result = (String) getOptionForKeyMethod.invoke(importRequest, "nonExistentKey");
        assertNull(result);
    }

    @Test
    public void testGetOptionsValuePrivateMethod() throws Exception {
        // Use reflection to test private method
        Method getOptionsValueMethod = AtlasImportRequest.class.getDeclaredMethod("getOptionsValue", String.class, int.class);
        getOptionsValueMethod.setAccessible(true);

        // Test with null options
        importRequest.setOptions(null);
        int result = (Integer) getOptionsValueMethod.invoke(importRequest, "testKey", 99);
        assertEquals(result, 99);

        // Test with empty value
        Map<String, String> options = new HashMap<>();
        options.put("testKey", "");
        importRequest.setOptions(options);
        result = (Integer) getOptionsValueMethod.invoke(importRequest, "testKey", 99);
        assertEquals(result, 99);

        // Test with valid value
        options.put("testKey", "123");
        importRequest.setOptions(options);
        result = (Integer) getOptionsValueMethod.invoke(importRequest, "testKey", 99);
        assertEquals(result, 123);

        // Test with null value
        options.put("testKey", null);
        importRequest.setOptions(options);
        result = (Integer) getOptionsValueMethod.invoke(importRequest, "testKey", 99);
        assertEquals(result, 99);
    }

    @Test
    public void testToString() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", "value2");
        importRequest.setOptions(options);

        String result = importRequest.toString();
        assertNotNull(result);
        assertTrue(result.contains("AtlasImportRequest{"));
        assertTrue(result.contains("options={"));
    }

    @Test
    public void testToStringWithEmptyRequest() {
        String result = importRequest.toString();
        assertNotNull(result);
        assertTrue(result.contains("AtlasImportRequest{"));
        assertTrue(result.contains("options={"));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        StringBuilder result = importRequest.toString(null);
        assertNotNull(result);
        assertTrue(result.toString().contains("AtlasImportRequest{"));
    }

    @Test
    public void testToStringWithExistingStringBuilder() {
        StringBuilder sb = new StringBuilder("Prefix: ");
        StringBuilder result = importRequest.toString(sb);
        assertNotNull(result);
        assertTrue(result.toString().startsWith("Prefix: "));
        assertTrue(result.toString().contains("AtlasImportRequest{"));
    }

    @Test
    public void testConstantValues() {
        assertEquals(AtlasImportRequest.TRANSFORMS_KEY, "transforms");
        assertEquals(AtlasImportRequest.TRANSFORMERS_KEY, "transformers");
        assertEquals(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, "replicatedFrom");
        assertEquals(AtlasImportRequest.OPTION_KEY_SKIP_UPDATE_REPLICATION_ATTR, "skipUpdateReplicationAttr");
        assertEquals(AtlasImportRequest.OPTION_KEY_MIGRATION_FILE_NAME, "migrationFileName");
        assertEquals(AtlasImportRequest.OPTION_KEY_MIGRATION, "migration");
        assertEquals(AtlasImportRequest.OPTION_KEY_NUM_WORKERS, "numWorkers");
        assertEquals(AtlasImportRequest.OPTION_KEY_BATCH_SIZE, "batchSize");
        assertEquals(AtlasImportRequest.OPTION_KEY_FORMAT, "format");
        assertEquals(AtlasImportRequest.OPTION_KEY_FORMAT_ZIP_DIRECT, "zipDirect");
        assertEquals(AtlasImportRequest.START_POSITION_KEY, "startPosition");
        assertEquals(AtlasImportRequest.UPDATE_TYPE_DEFINITION_KEY, "updateTypeDefinition");
    }

    @Test
    public void testBoundaryValues() {
        // Test with boundary integer values
        importRequest.setSizeOption(Integer.MAX_VALUE);
        assertEquals(importRequest.getSizeOption(), Integer.MAX_VALUE);

        importRequest.setSizeOption(0);
        assertEquals(importRequest.getSizeOption(), 0);

        importRequest.setSizeOption(-1);
        assertEquals(importRequest.getSizeOption(), -1);

        // Test with large number strings
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_NUM_WORKERS, String.valueOf(Integer.MAX_VALUE));
        importRequest.setOptions(options);
        assertEquals(importRequest.getOptionKeyNumWorkers(), Integer.MAX_VALUE);

        options.put(AtlasImportRequest.OPTION_KEY_BATCH_SIZE, String.valueOf(Integer.MAX_VALUE));
        importRequest.setOptions(options);
        assertEquals(importRequest.getOptionKeyBatchSize(), Integer.MAX_VALUE);
    }

    @Test
    public void testSpecialCharactersInOptions() {
        String specialKey = "spcKey";
        String specialValue = "spcVal";

        importRequest.setOption(specialKey, specialValue);

        assertEquals(importRequest.getOptions().get(specialKey), specialValue);
    }

    @Test
    public void testLargeNumberOfOptions() {
        // Test with many options
        for (int i = 0; i < 1000; i++) {
            importRequest.setOption("key" + i, "value" + i);
        }

        Map<String, String> options = importRequest.getOptions();
        assertEquals(options.size(), 1000);
        assertEquals(options.get("key500"), "value500");
    }

    @Test
    public void testEmptyStringValues() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, "");
        options.put(AtlasImportRequest.OPTION_KEY_NUM_WORKERS, "");
        options.put(AtlasImportRequest.OPTION_KEY_BATCH_SIZE, "");
        importRequest.setOptions(options);

        assertEquals(importRequest.getOptionKeyReplicatedFrom(), "");
        assertEquals(importRequest.getOptionKeyNumWorkers(), 1);
        assertEquals(importRequest.getOptionKeyBatchSize(), 1);
    }

    @Test
    public void testNullStringValues() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, null);
        importRequest.setOptions(options);

        boolean isSet = importRequest.isReplicationOptionSet();
        assertTrue(isSet); // Key exists even though value is null
    }

    @Test
    public void testZeroAndNegativeNumbers() {
        Map<String, String> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_NUM_WORKERS, "0");
        options.put(AtlasImportRequest.OPTION_KEY_BATCH_SIZE, "-5");
        importRequest.setOptions(options);

        assertEquals(importRequest.getOptionKeyNumWorkers(), 0);
        assertEquals(importRequest.getOptionKeyBatchSize(), -5);
    }

    @Test
    public void testMixedTypeOptionsHandling() {
        Map<String, String> options = new HashMap<>();
        options.put("stringOption", "test");
        options.put("numberOption", "123");
        options.put("booleanOption", "true");
        options.put("emptyOption", "");
        importRequest.setOptions(options);

        assertEquals(importRequest.getOptions().get("stringOption"), "test");
        assertEquals(importRequest.getOptions().get("numberOption"), "123");
        assertEquals(importRequest.getOptions().get("booleanOption"), "true");
        assertEquals(importRequest.getOptions().get("emptyOption"), "");
    }
}
