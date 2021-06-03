/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.hive.hook.utils;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.apache.commons.collections.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ActiveEntityFilterTest {
    private static String FILE_SUFFIX = "-v2";

    @BeforeClass
    public void setup() {
        ActiveEntityFilter.init(true);
    }

    @Test
    public void verifyMessages() throws IOException {
        assertAtlasEntitiesWithExtInfoFromFile("hs2-drop-db");
        assertAtlasEntitiesWithExtInfoFromFile("hs2-create-db");
        assertAtlasEntitiesWithExtInfoFromFile("hs2-create-table");
        assertMessageFromFile("hs2-table-rename");
        assertMessageFromFile("hs2-alter-view");
        assertMessageFromFile("hs2-drop-table");
        assertAtlasEntitiesWithExtInfoFromFile("hs2-create-process");
        assertAtlasEntitiesWithExtInfoFromFile("hs2-load-inpath");
        assertAtlasEntitiesWithExtInfoFromFile("hs2-create-db-with-no-pathentities-to-retain", false);
        assertAtlasEntitiesWithExtInfoFromFile("hs2-load-inpath-with-no-pathentities-to-retain", false);
    }

    private void assertMessageFromFile(String msgFile) throws IOException {
        List incoming = loadList(msgFile);
        List expected = loadList(msgFile + FILE_SUFFIX);
        int expectedSize = expected.size();

        List<HookNotification> actual = ActiveEntityFilter.apply((List<HookNotification>) incoming);
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < expectedSize; i++) {
            if (actual.get(i) instanceof HookNotification.EntityCreateRequestV2) {
                HookNotification.EntityCreateRequestV2 actualN = (HookNotification.EntityCreateRequestV2) actual.get(i);
                HookNotification.EntityCreateRequestV2 expectedN = (HookNotification.EntityCreateRequestV2) expected.get(i);

                assertAtlasEntitiesWithExtInfo(actualN.getEntities(), expectedN.getEntities());
            }

            if (actual.get(i) instanceof HookNotification.EntityUpdateRequestV2) {
                HookNotification.EntityUpdateRequestV2 actualN = (HookNotification.EntityUpdateRequestV2) actual.get(i);
                HookNotification.EntityUpdateRequestV2 expectedN = (HookNotification.EntityUpdateRequestV2) expected.get(i);

                assertAtlasEntitiesWithExtInfo(actualN.getEntities(), expectedN.getEntities());
            }

            if (actual.get(i) instanceof HookNotification.EntityPartialUpdateRequestV2) {
                HookNotification.EntityPartialUpdateRequestV2 actualN = (HookNotification.EntityPartialUpdateRequestV2) actual.get(i);
                HookNotification.EntityPartialUpdateRequestV2 expectedN = (HookNotification.EntityPartialUpdateRequestV2) expected.get(i);

                assertAtlasEntitiesWithExtInfo(actualN.getEntity(), expectedN.getEntity());
            }
        }
    }

    private List<HookNotification> loadList(String msgFile) throws IOException {
        List list = TestResourceFileUtils.readObjectFromJson("", msgFile, List.class);
        List<HookNotification> ret = new ArrayList<>();

        for (int i = 0; i < list.size(); i++) {
            HookNotification notification = createNotification((LinkedHashMap) list.get(i));
            if (notification == null) {
                continue;
            }

            ret.add(notification);
        }

        return ret;
    }

    private HookNotification createNotification(LinkedHashMap<String, Object> linkedHashMap) {
        assertTrue(linkedHashMap.containsKey("type"));

        String type = (String) linkedHashMap.get("type");
        switch (type) {
            case "ENTITY_CREATE_V2":
                return AtlasType.fromLinkedHashMap(linkedHashMap, HookNotification.EntityCreateRequestV2.class);

            case "ENTITY_FULL_UPDATE_V2":
                return AtlasType.fromLinkedHashMap(linkedHashMap, HookNotification.EntityUpdateRequestV2.class);

            case "ENTITY_PARTIAL_UPDATE_V2":
                return AtlasType.fromLinkedHashMap(linkedHashMap, HookNotification.EntityPartialUpdateRequestV2.class);

            default:
                return null;
        }
    }


    private void assertAtlasEntitiesWithExtInfo(AtlasEntity.AtlasEntityWithExtInfo actual, AtlasEntity.AtlasEntityWithExtInfo expected) {
        String actualJson = AtlasType.toJson(actual);
        String expectedJson = AtlasType.toJson(expected);

        LinkedHashMap<String, Object> actualLHM = AtlasType.fromJson(actualJson, LinkedHashMap.class);
        LinkedHashMap<String, Object> expectedLHM = AtlasType.fromJson(expectedJson, LinkedHashMap.class);

        AssertLinkedHashMap.assertEquals(actualLHM, expectedLHM);
    }

    private void assertAtlasEntitiesWithExtInfoFromFile(String entityFile) throws IOException {
        assertAtlasEntitiesWithExtInfoFromFile(entityFile, true);
    }

    private void assertAtlasEntitiesWithExtInfoFromFile(String entityFile, boolean retainPathEntities) throws IOException {
        AtlasEntity.AtlasEntitiesWithExtInfo incoming = TestResourceFileUtils.readObjectFromJson("", entityFile, AtlasEntity.AtlasEntitiesWithExtInfo.class);
        AtlasEntity.AtlasEntitiesWithExtInfo expected = TestResourceFileUtils.readObjectFromJson("", entityFile + FILE_SUFFIX, AtlasEntity.AtlasEntitiesWithExtInfo.class);

        HiveDDLEntityFilter hiveLineageEntityFilter = new HiveDDLEntityFilter();
        AtlasEntity.AtlasEntitiesWithExtInfo actual = hiveLineageEntityFilter.apply(incoming);

        if (retainPathEntities) {
            assertAtlasEntitiesWithExtInfo(actual, expected);
        } else {
            assertAtlasEntitiesWithNoPathEntitiesToRetain(actual, expected);
        }
    }

    private void assertAtlasEntitiesWithExtInfo(AtlasEntity.AtlasEntitiesWithExtInfo actual, AtlasEntity.AtlasEntitiesWithExtInfo expected) {
        assertNotNull(actual);
        assertNotNull(expected);

        if (expected.getEntities() != null && actual.getEntities() != null) {
            assertEquals(actual.getEntities().size(), expected.getEntities().size());
            assertEntity(actual.getEntities(), expected.getEntities());
        }

        assertEquals(MapUtils.isEmpty(actual.getReferredEntities()), MapUtils.isEmpty(expected.getReferredEntities()));
        if (expected.getReferredEntities() != null && actual.getReferredEntities() != null) {
            assertEntity(actual.getReferredEntities(), expected.getReferredEntities());
        }
    }

    private void assertAtlasEntitiesWithNoPathEntitiesToRetain(AtlasEntity.AtlasEntitiesWithExtInfo actual, AtlasEntity.AtlasEntitiesWithExtInfo expected) {
        assertNotNull(actual);
        assertNotNull(expected);

        if (expected.getEntities() != null && actual.getEntities() != null) {
            assertNotEquals(actual.getEntities().size(), expected.getEntities().size());
        }

        if (expected.getReferredEntities() != null && actual.getReferredEntities() != null) {
            assertNotEquals(actual.getReferredEntities().size(), expected.getReferredEntities().size());
        }
    }

    private void assertEntity(Map<String, AtlasEntity> actual, Map<String, AtlasEntity> expected) {
        assertEquals(actual.size(), expected.size());
    }

    private void assertEntity(List<AtlasEntity> actual, List<AtlasEntity> expected) {
        AssertLinkedHashMap.assertEquals(actual, expected);
    }

    private static class AssertLinkedHashMap {
        private static final String MISMATCH_KEY_FORMAT = "Mismatch: Key: %s";
        private  static final Set<String> excludeKeys = new HashSet<String>() {{
            add("guid");
            add("owner");
        }};

        public static void assertEquals(LinkedHashMap<String, Object> actual, LinkedHashMap<String, Object> expected) {
            for (String key : expected.keySet()) {
                assertTrue(actual.containsKey(key), "Key: " + key + " Not found!");

                if (excludeKeys.contains(key)) {
                    continue;
                }

                if (actual.get(key) instanceof LinkedHashMap) {
                    assertEquals((LinkedHashMap) actual.get(key), (LinkedHashMap) expected.get(key));
                    continue;
                }

                Assert.assertEquals(actual.get(key), actual.get(key), String.format(MISMATCH_KEY_FORMAT, key));
            }
        }

        public static void assertEquals(List<AtlasEntity> actual, List<AtlasEntity> expected) {
            Assert.assertEquals(actual.size(), expected.size());
            for (int i = 0; i < actual.size(); i++) {
                AtlasEntity actualEntity = actual.get(i);
                AtlasEntity expectedEntity = expected.get(i);

                String actualJson = AtlasType.toJson(actualEntity);
                String expectedJson = AtlasType.toJson(expectedEntity);

                assertEquals(AtlasType.fromJson(actualJson, LinkedHashMap.class), AtlasType.fromJson(expectedJson, LinkedHashMap.class));
            }
        }
    }
}
