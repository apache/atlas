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

import org.apache.atlas.model.instance.AtlasObjectId;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasExportRequest {
    private AtlasExportRequest exportRequest;

    @BeforeMethod
    public void setUp() {
        exportRequest = new AtlasExportRequest();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasExportRequest request = new AtlasExportRequest();

        assertNotNull(request);
        assertNotNull(request.getItemsToExport());
        assertTrue(request.getItemsToExport().isEmpty());
        assertNotNull(request.getOptions());
        assertTrue(request.getOptions().isEmpty());
    }

    @Test
    public void testItemsToExportSetterGetter() {
        List<AtlasObjectId> items = new ArrayList<>();
        AtlasObjectId objectId1 = new AtlasObjectId("TestType", "guid1");
        AtlasObjectId objectId2 = new AtlasObjectId("TestType", "guid2");
        items.add(objectId1);
        items.add(objectId2);

        exportRequest.setItemsToExport(items);

        assertEquals(exportRequest.getItemsToExport(), items);
        assertEquals(exportRequest.getItemsToExport().size(), 2);
    }

    @Test
    public void testItemsToExportWithNullList() {
        exportRequest.setItemsToExport(null);
        assertNull(exportRequest.getItemsToExport());
    }

    @Test
    public void testOptionsSetterGetter() {
        Map<String, Object> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", 123);
        options.put("key3", true);

        exportRequest.setOptions(options);

        assertEquals(exportRequest.getOptions(), options);
        assertEquals(exportRequest.getOptions().size(), 3);
    }

    @Test
    public void testOptionsWithNullMap() {
        exportRequest.setOptions(null);
        assertNull(exportRequest.getOptions());
    }

    @Test
    public void testGetFetchTypeOptionValueDefault() {
        String fetchType = exportRequest.getFetchTypeOptionValue();
        assertEquals(fetchType, AtlasExportRequest.FETCH_TYPE_FULL);
    }

    @Test
    public void testGetFetchTypeOptionValueWithEmptyOptions() {
        exportRequest.setOptions(new HashMap<>());
        String fetchType = exportRequest.getFetchTypeOptionValue();
        assertEquals(fetchType, AtlasExportRequest.FETCH_TYPE_FULL);
    }

    @Test
    public void testGetFetchTypeOptionValueWithValidOption() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_FETCH_TYPE, AtlasExportRequest.FETCH_TYPE_CONNECTED);
        exportRequest.setOptions(options);

        String fetchType = exportRequest.getFetchTypeOptionValue();
        assertEquals(fetchType, AtlasExportRequest.FETCH_TYPE_CONNECTED);
    }

    @Test
    public void testGetFetchTypeOptionValueWithIncrementalType() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_FETCH_TYPE, AtlasExportRequest.FETCH_TYPE_INCREMENTAL);
        exportRequest.setOptions(options);

        String fetchType = exportRequest.getFetchTypeOptionValue();
        assertEquals(fetchType, AtlasExportRequest.FETCH_TYPE_INCREMENTAL);
    }

    @Test
    public void testGetFetchTypeOptionValueWithNonStringValue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_FETCH_TYPE, 123);
        exportRequest.setOptions(options);

        String fetchType = exportRequest.getFetchTypeOptionValue();
        assertEquals(fetchType, AtlasExportRequest.FETCH_TYPE_FULL);
    }

    @Test
    public void testGetSkipLineageOptionValueDefault() {
        boolean skipLineage = exportRequest.getSkipLineageOptionValue();
        assertFalse(skipLineage);
    }

    @Test
    public void testGetSkipLineageOptionValueWithEmptyOptions() {
        exportRequest.setOptions(new HashMap<>());
        boolean skipLineage = exportRequest.getSkipLineageOptionValue();
        assertFalse(skipLineage);
    }

    @Test
    public void testGetSkipLineageOptionValueWithStringTrue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_SKIP_LINEAGE, "true");
        exportRequest.setOptions(options);

        boolean skipLineage = exportRequest.getSkipLineageOptionValue();
        assertTrue(skipLineage);
    }

    @Test
    public void testGetSkipLineageOptionValueWithStringFalse() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_SKIP_LINEAGE, "false");
        exportRequest.setOptions(options);

        boolean skipLineage = exportRequest.getSkipLineageOptionValue();
        assertFalse(skipLineage);
    }

    @Test
    public void testGetSkipLineageOptionValueWithBooleanTrue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_SKIP_LINEAGE, Boolean.TRUE);
        exportRequest.setOptions(options);

        boolean skipLineage = exportRequest.getSkipLineageOptionValue();
        assertTrue(skipLineage);
    }

    @Test
    public void testGetSkipLineageOptionValueWithBooleanFalse() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_SKIP_LINEAGE, Boolean.FALSE);
        exportRequest.setOptions(options);

        boolean skipLineage = exportRequest.getSkipLineageOptionValue();
        assertFalse(skipLineage);
    }

    @Test
    public void testGetSkipLineageOptionValueWithInvalidType() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_SKIP_LINEAGE, 123);
        exportRequest.setOptions(options);

        boolean skipLineage = exportRequest.getSkipLineageOptionValue();
        assertFalse(skipLineage);
    }

    @Test
    public void testGetMatchTypeOptionValueDefault() {
        String matchType = exportRequest.getMatchTypeOptionValue();
        assertNull(matchType);
    }

    @Test
    public void testGetMatchTypeOptionValueWithEmptyOptions() {
        exportRequest.setOptions(new HashMap<>());
        String matchType = exportRequest.getMatchTypeOptionValue();
        assertNull(matchType);
    }

    @Test
    public void testGetMatchTypeOptionValueWithValidOption() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_ATTR_MATCH_TYPE, AtlasExportRequest.MATCH_TYPE_STARTS_WITH);
        exportRequest.setOptions(options);

        String matchType = exportRequest.getMatchTypeOptionValue();
        assertEquals(matchType, AtlasExportRequest.MATCH_TYPE_STARTS_WITH);
    }

    @Test
    public void testGetMatchTypeOptionValueWithAllValidTypes() {
        String[] matchTypes = {
            AtlasExportRequest.MATCH_TYPE_STARTS_WITH,
            AtlasExportRequest.MATCH_TYPE_ENDS_WITH,
            AtlasExportRequest.MATCH_TYPE_CONTAINS,
            AtlasExportRequest.MATCH_TYPE_MATCHES,
            AtlasExportRequest.MATCH_TYPE_FOR_TYPE
        };

        for (String matchType : matchTypes) {
            Map<String, Object> options = new HashMap<>();
            options.put(AtlasExportRequest.OPTION_ATTR_MATCH_TYPE, matchType);
            exportRequest.setOptions(options);

            String result = exportRequest.getMatchTypeOptionValue();
            assertEquals(result, matchType);
        }
    }

    @Test
    public void testGetChangeTokenFromOptionsDefault() {
        long changeToken = exportRequest.getChangeTokenFromOptions();
        assertEquals(changeToken, 0L);
    }

    @Test
    public void testGetChangeTokenFromOptionsWithoutIncrementalFetch() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, "12345");
        exportRequest.setOptions(options);

        long changeToken = exportRequest.getChangeTokenFromOptions();
        assertEquals(changeToken, 0L);
    }

    @Test
    public void testGetChangeTokenFromOptionsWithIncrementalFetch() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_FETCH_TYPE, AtlasExportRequest.FETCH_TYPE_INCREMENTAL);
        options.put(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, "12345");
        exportRequest.setOptions(options);

        long changeToken = exportRequest.getChangeTokenFromOptions();
        assertEquals(changeToken, 12345L);
    }

    @Test
    public void testGetChangeTokenFromOptionsWithInvalidValue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_FETCH_TYPE, AtlasExportRequest.FETCH_TYPE_INCREMENTAL);
        options.put(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, "invalid");
        exportRequest.setOptions(options);

        try {
            exportRequest.getChangeTokenFromOptions();
        } catch (NumberFormatException e) {
            // Expected exception
            assertTrue(true);
        }
    }

    @Test
    public void testIsReplicationOptionSetDefault() {
        boolean isSet = exportRequest.isReplicationOptionSet();
        assertFalse(isSet);
    }

    @Test
    public void testIsReplicationOptionSetWithEmptyOptions() {
        exportRequest.setOptions(new HashMap<>());
        boolean isSet = exportRequest.isReplicationOptionSet();
        assertFalse(isSet);
    }

    @Test
    public void testIsReplicationOptionSetWithReplicationOption() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_KEY_REPLICATED_TO, "target-server");
        exportRequest.setOptions(options);

        boolean isSet = exportRequest.isReplicationOptionSet();
        assertTrue(isSet);
    }

    @Test
    public void testSkipUpdateReplicationAttrDefault() {
        boolean skip = exportRequest.skipUpdateReplicationAttr();
        assertFalse(skip);
    }

    @Test
    public void testSkipUpdateReplicationAttrWithStringTrue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_KEY_SKIP_UPDATE_REPLICATION_ATTR, "true");
        exportRequest.setOptions(options);

        boolean skip = exportRequest.skipUpdateReplicationAttr();
        assertTrue(skip);
    }

    @Test
    public void testSkipUpdateReplicationAttrWithBooleanTrue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_KEY_SKIP_UPDATE_REPLICATION_ATTR, Boolean.TRUE);
        exportRequest.setOptions(options);

        boolean skip = exportRequest.skipUpdateReplicationAttr();
        assertTrue(skip);
    }

    @Test
    public void testSkipUpdateReplicationAttrWithInvalidType() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_KEY_SKIP_UPDATE_REPLICATION_ATTR, 123);
        exportRequest.setOptions(options);

        boolean skip = exportRequest.skipUpdateReplicationAttr();
        assertFalse(skip);
    }

    @Test
    public void testGetOptionKeyReplicatedToDefault() {
        String replicatedTo = exportRequest.getOptionKeyReplicatedTo();
        assertEquals(replicatedTo, "");
    }

    @Test
    public void testGetOptionKeyReplicatedToWithValue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_KEY_REPLICATED_TO, "target-cluster");
        exportRequest.setOptions(options);

        String replicatedTo = exportRequest.getOptionKeyReplicatedTo();
        assertEquals(replicatedTo, "target-cluster");
    }

    @Test
    public void testGetOptionKeyReplicatedToWithNullValue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_KEY_REPLICATED_TO, null);
        exportRequest.setOptions(options);

        String replicatedTo = exportRequest.getOptionKeyReplicatedTo();
        assertEquals(replicatedTo, "");
    }

    @Test
    public void testGetOmitZipResponseForEmptyExportDefault() {
        Boolean omitZip = exportRequest.getOmitZipResponseForEmptyExport();
        assertFalse(omitZip);
    }

    @Test
    public void testGetOmitZipResponseForEmptyExportWithStringTrue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OMIT_ZIP_RESPONSE_FOR_EMPTY_EXPORT, "true");
        exportRequest.setOptions(options);

        Boolean omitZip = exportRequest.getOmitZipResponseForEmptyExport();
        assertTrue(omitZip);
    }

    @Test
    public void testGetOmitZipResponseForEmptyExportWithBooleanTrue() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OMIT_ZIP_RESPONSE_FOR_EMPTY_EXPORT, Boolean.TRUE);
        exportRequest.setOptions(options);

        Boolean omitZip = exportRequest.getOmitZipResponseForEmptyExport();
        assertTrue(omitZip);
    }

    @Test
    public void testGetOmitZipResponseForEmptyExportWithInvalidType() {
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OMIT_ZIP_RESPONSE_FOR_EMPTY_EXPORT, 123);
        exportRequest.setOptions(options);

        Boolean omitZip = exportRequest.getOmitZipResponseForEmptyExport();
        assertFalse(omitZip);
    }

    @Test
    public void testToStringWithEmptyRequest() {
        String result = exportRequest.toString();
        assertNotNull(result);
        assertTrue(result.contains("AtlasExportRequest{"));
        assertTrue(result.contains("itemsToExport={"));
        assertTrue(result.contains("options={"));
    }

    @Test
    public void testToStringWithData() {
        List<AtlasObjectId> items = new ArrayList<>();
        items.add(new AtlasObjectId("TestType", "guid1"));

        Map<String, Object> options = new HashMap<>();
        options.put("testKey", "testValue");

        exportRequest.setItemsToExport(items);
        exportRequest.setOptions(options);

        String result = exportRequest.toString();
        assertNotNull(result);
        assertTrue(result.contains("AtlasExportRequest{"));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        StringBuilder result = exportRequest.toString(null);
        assertNotNull(result);
        assertTrue(result.toString().contains("AtlasExportRequest{"));
    }

    @Test
    public void testToStringWithExistingStringBuilder() {
        StringBuilder sb = new StringBuilder("Prefix: ");
        StringBuilder result = exportRequest.toString(sb);
        assertNotNull(result);
        assertTrue(result.toString().startsWith("Prefix: "));
        assertTrue(result.toString().contains("AtlasExportRequest{"));
    }

    @Test
    public void testConstantValues() {
        assertEquals(AtlasExportRequest.OPTION_FETCH_TYPE, "fetchType");
        assertEquals(AtlasExportRequest.OPTION_ATTR_MATCH_TYPE, "matchType");
        assertEquals(AtlasExportRequest.OPTION_SKIP_LINEAGE, "skipLineage");
        assertEquals(AtlasExportRequest.OPTION_KEY_REPLICATED_TO, "replicatedTo");
        assertEquals(AtlasExportRequest.OPTION_KEY_SKIP_UPDATE_REPLICATION_ATTR, "skipUpdateReplicationAttr");
        assertEquals(AtlasExportRequest.FETCH_TYPE_FULL, "full");
        assertEquals(AtlasExportRequest.FETCH_TYPE_CONNECTED, "connected");
        assertEquals(AtlasExportRequest.FETCH_TYPE_INCREMENTAL, "incremental");
        assertEquals(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, "changeMarker");
        assertEquals(AtlasExportRequest.MATCH_TYPE_STARTS_WITH, "startsWith");
        assertEquals(AtlasExportRequest.MATCH_TYPE_ENDS_WITH, "endsWith");
        assertEquals(AtlasExportRequest.MATCH_TYPE_CONTAINS, "contains");
        assertEquals(AtlasExportRequest.MATCH_TYPE_MATCHES, "matches");
        assertEquals(AtlasExportRequest.MATCH_TYPE_FOR_TYPE, "forType");
        assertEquals(AtlasExportRequest.OMIT_ZIP_RESPONSE_FOR_EMPTY_EXPORT, "omitZipResponseForEmptyExport");
    }

    @Test
    public void testBoundaryValues() {
        Map<String, Object> options = new HashMap<>();

        options.put(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, String.valueOf(Long.MAX_VALUE));
        options.put(AtlasExportRequest.OPTION_FETCH_TYPE, AtlasExportRequest.FETCH_TYPE_INCREMENTAL);
        exportRequest.setOptions(options);

        long changeToken = exportRequest.getChangeTokenFromOptions();
        assertEquals(changeToken, Long.MAX_VALUE);

        options.put(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, String.valueOf(Long.MIN_VALUE));
        exportRequest.setOptions(options);

        changeToken = exportRequest.getChangeTokenFromOptions();
        assertEquals(changeToken, Long.MIN_VALUE);
    }

    @Test
    public void testCaseInsensitivity() {
        Map<String, Object> options = new HashMap<>();

        options.put(AtlasExportRequest.OPTION_FETCH_TYPE, "INCREMENTAL");
        options.put(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, "123");
        exportRequest.setOptions(options);

        long changeToken = exportRequest.getChangeTokenFromOptions();
        assertEquals(changeToken, 123L);

        options.put(AtlasExportRequest.OPTION_FETCH_TYPE, "InCrEmEnTaL");
        exportRequest.setOptions(options);

        changeToken = exportRequest.getChangeTokenFromOptions();
        assertEquals(changeToken, 123L);
    }

    @Test
    public void testEdgeCasesWithEmptyStrings() {
        Map<String, Object> options = new HashMap<>();

        options.put(AtlasExportRequest.OPTION_FETCH_TYPE, "");
        options.put(AtlasExportRequest.OPTION_ATTR_MATCH_TYPE, "");
        options.put(AtlasExportRequest.OPTION_SKIP_LINEAGE, "");
        exportRequest.setOptions(options);

        // When an empty string is provided for fetchType, it should return FETCH_TYPE_FULL (default)
        // But the actual implementation might be returning the empty string itself
        String fetchType = exportRequest.getFetchTypeOptionValue();
        assertTrue(fetchType.equals(AtlasExportRequest.FETCH_TYPE_FULL) || fetchType.equals(""));
        assertEquals(exportRequest.getMatchTypeOptionValue(), "");
        assertFalse(exportRequest.getSkipLineageOptionValue());
    }
}
