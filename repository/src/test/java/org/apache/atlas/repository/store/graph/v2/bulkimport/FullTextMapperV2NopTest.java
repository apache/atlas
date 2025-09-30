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
package org.apache.atlas.repository.store.graph.v2.bulkimport;

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;

public class FullTextMapperV2NopTest {
    private FullTextMapperV2Nop fullTextMapperV2Nop;

    @BeforeMethod
    public void setUp() {
        fullTextMapperV2Nop = new FullTextMapperV2Nop();
    }

    @Test
    public void testGetIndexTextForClassifications() {
        String guid = "test-guid-123";
        List<AtlasClassification> classifications = Arrays.asList(createMockClassification("classification1"), createMockClassification("classification2"));
        // Test with valid parameters
        String result = fullTextMapperV2Nop.getIndexTextForClassifications(guid, classifications);
        assertNull(result);
        // Test with null guid
        result = fullTextMapperV2Nop.getIndexTextForClassifications(null, classifications);
        assertNull(result);
        // Test with empty guid
        result = fullTextMapperV2Nop.getIndexTextForClassifications("", classifications);
        assertNull(result);
        // Test with null classifications
        result = fullTextMapperV2Nop.getIndexTextForClassifications(guid, null);
        assertNull(result);
        // Test with empty classifications
        result = fullTextMapperV2Nop.getIndexTextForClassifications(guid, Arrays.asList());
        assertNull(result);
    }

    @Test
    public void testGetIndexTextForEntity() {
        String guid = "entity-guid-456";
        // Test with valid guid
        String result = fullTextMapperV2Nop.getIndexTextForEntity(guid);
        assertNull(result);
        // Test with null guid
        result = fullTextMapperV2Nop.getIndexTextForEntity(null);
        assertNull(result);
        // Test with empty guid
        result = fullTextMapperV2Nop.getIndexTextForEntity("");
        assertNull(result);
        // Test with special characters in guid
        result = fullTextMapperV2Nop.getIndexTextForEntity("guid-with-special-chars-!@#$%");
        assertNull(result);
    }

    @Test
    public void testGetClassificationTextForEntity() {
        AtlasEntity mockEntity = createMockEntity("TestEntity", "test-entity-guid");
        // Test with valid entity
        String result = fullTextMapperV2Nop.getClassificationTextForEntity(mockEntity);
        assertNull(result);
        // Test with null entity
        result = fullTextMapperV2Nop.getClassificationTextForEntity(null);
        assertNull(result);
        // Test with entity having classifications
        AtlasEntity entityWithClassifications = createMockEntity("EntityWithClassifications", "guid-with-classifications");
        result = fullTextMapperV2Nop.getClassificationTextForEntity(entityWithClassifications);
        assertNull(result);
    }

    @Test
    public void testGetAndCacheEntity() {
        String guid = "cache-test-guid-789";
        // Test with valid guid
        AtlasEntity result = fullTextMapperV2Nop.getAndCacheEntity(guid);
        assertNull(result);
        // Test with null guid
        result = fullTextMapperV2Nop.getAndCacheEntity(null);
        assertNull(result);
        // Test with empty guid
        result = fullTextMapperV2Nop.getAndCacheEntity("");
        assertNull(result);
        // Test multiple calls with same guid to verify no caching side effects
        result = fullTextMapperV2Nop.getAndCacheEntity(guid);
        assertNull(result);
        result = fullTextMapperV2Nop.getAndCacheEntity(guid);
        assertNull(result);
    }

    @Test
    public void testGetAndCacheEntityWithIncludeReferences() {
        String guid = "references-test-guid-999";
        // Test with includeReferences true
        AtlasEntity result = fullTextMapperV2Nop.getAndCacheEntity(guid, true);
        assertNull(result);
        // Test with includeReferences false
        result = fullTextMapperV2Nop.getAndCacheEntity(guid, false);
        assertNull(result);
        // Test with null guid and includeReferences true
        result = fullTextMapperV2Nop.getAndCacheEntity(null, true);
        assertNull(result);
        // Test with null guid and includeReferences false
        result = fullTextMapperV2Nop.getAndCacheEntity(null, false);
        assertNull(result);
        // Test with empty guid
        result = fullTextMapperV2Nop.getAndCacheEntity("", true);
        assertNull(result);
        result = fullTextMapperV2Nop.getAndCacheEntity("", false);
        assertNull(result);
    }

    @Test
    public void testGetAndCacheEntityWithExtInfo() {
        String guid = "ext-info-test-guid-111";
        // Test with valid guid
        AtlasEntity.AtlasEntityWithExtInfo result = fullTextMapperV2Nop.getAndCacheEntityWithExtInfo(guid);
        assertNull(result);
        // Test with null guid
        result = fullTextMapperV2Nop.getAndCacheEntityWithExtInfo(null);
        assertNull(result);
        // Test with empty guid
        result = fullTextMapperV2Nop.getAndCacheEntityWithExtInfo("");
        assertNull(result);
        // Test multiple calls to verify no state issues
        result = fullTextMapperV2Nop.getAndCacheEntityWithExtInfo(guid);
        assertNull(result);
        result = fullTextMapperV2Nop.getAndCacheEntityWithExtInfo(guid);
        assertNull(result);
    }

    @Test
    public void testAllMethodsReturnNull() {
        String testGuid = "comprehensive-test-guid";
        AtlasEntity testEntity = createMockEntity("TestType", testGuid);
        List<AtlasClassification> testClassifications = Arrays.asList(createMockClassification("TestClassification"));
        // Verify all methods return null
        assertNull(fullTextMapperV2Nop.getIndexTextForClassifications(testGuid, testClassifications));
        assertNull(fullTextMapperV2Nop.getIndexTextForEntity(testGuid));
        assertNull(fullTextMapperV2Nop.getClassificationTextForEntity(testEntity));
        assertNull(fullTextMapperV2Nop.getAndCacheEntity(testGuid));
        assertNull(fullTextMapperV2Nop.getAndCacheEntity(testGuid, true));
        assertNull(fullTextMapperV2Nop.getAndCacheEntity(testGuid, false));
        assertNull(fullTextMapperV2Nop.getAndCacheEntityWithExtInfo(testGuid));
    }

    private AtlasClassification createMockClassification(String typeName) {
        AtlasClassification classification = mock(AtlasClassification.class);
        when(classification.getTypeName()).thenReturn(typeName);
        return classification;
    }

    private AtlasEntity createMockEntity(String typeName, String guid) {
        AtlasEntity entity = mock(AtlasEntity.class);
        when(entity.getTypeName()).thenReturn(typeName);
        when(entity.getGuid()).thenReturn(guid);
        return entity;
    }
}
