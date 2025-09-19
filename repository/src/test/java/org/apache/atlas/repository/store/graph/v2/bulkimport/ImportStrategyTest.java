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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.v2.EntityImportStream;
import org.apache.atlas.v1.typesystem.types.utils.TypesUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ImportStrategyTest {
    private TestImportStrategy importStrategy;

    @BeforeMethod
    public void setUp() {
        importStrategy = new TestImportStrategy();
    }

    @Test
    public void testRunMethodsExist() {
        // Test that the abstract methods are properly defined
        EntityImportStream mockStream = mock(EntityImportStream.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        Set<String> processedGuids = new HashSet<>();
        List<String> residualList = Arrays.asList();
        try {
            // Test first run method
            EntityMutationResponse result1 = importStrategy.run(mockStream, mockResult);
            assertNotNull(result1);

            // Test second run method
            TypesUtil.Pair<EntityMutationResponse, Float> result2 = importStrategy.run(mockEntityWithExtInfo, mockResponse, mockResult, processedGuids, 1, 100, 0.5f, residualList);
            assertNotNull(result2);
            assertNotNull(result2.left);
            assertNotNull(result2.right);
        } catch (AtlasBaseException e) {
            // Expected for abstract methods in test implementation
        }
    }

    @Test
    public void testAbstractClassCanBeExtended() {
        // Test that we can extend the abstract class
        assertNotNull(importStrategy);
        assertEquals(TestImportStrategy.class, importStrategy.getClass());
    }

    @Test
    public void testMethodSignatures() throws AtlasBaseException {
        // Test method signatures by calling with various parameter combinations
        EntityImportStream mockStream = mock(EntityImportStream.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);

        // Test with null stream - should handle gracefully in implementation
        EntityMutationResponse result = importStrategy.run(null, mockResult);
        assertNotNull(result);

        // Test with null result - should handle gracefully in implementation
        result = importStrategy.run(mockStream, null);
        assertNotNull(result);

        // Test with both null - should handle gracefully in implementation
        result = importStrategy.run(null, null);
        assertNotNull(result);
    }

    @Test
    public void testSecondRunMethodWithVariousParameters() throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getGuid()).thenReturn("test-guid-123");
        when(mockEntity.getTypeName()).thenReturn("TestType");

        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        Set<String> processedGuids = new HashSet<>();
        processedGuids.add("processed-guid-1");
        processedGuids.add("processed-guid-2");
        List<String> residualList = Arrays.asList("residual-1", "residual-2");

        // Test with various parameter combinations
        TypesUtil.Pair<EntityMutationResponse, Float> result;

        // Test with normal parameters
        result = importStrategy.run(mockEntityWithExtInfo, mockResponse, mockResult,
                                   processedGuids, 10, 100, 0.25f, residualList);
        assertNotNull(result);

        // Test with edge case parameters
        result = importStrategy.run(mockEntityWithExtInfo, mockResponse, mockResult,
                                   processedGuids, 0, 1, 0.0f, residualList);
        assertNotNull(result);

        // Test with maximum values
        result = importStrategy.run(mockEntityWithExtInfo, mockResponse, mockResult,
                                   processedGuids, 999, 1000, 99.9f, residualList);
        assertNotNull(result);

        // Test with null entity
        result = importStrategy.run(null, mockResponse, mockResult,
                                   processedGuids, 1, 10, 0.1f, residualList);
        assertNotNull(result);

        // Test with null response
        result = importStrategy.run(mockEntityWithExtInfo, null, mockResult,
                                   processedGuids, 1, 10, 0.1f, residualList);
        assertNotNull(result);

        // Test with null result
        result = importStrategy.run(mockEntityWithExtInfo, mockResponse, null,
                                   processedGuids, 1, 10, 0.1f, residualList);
        assertNotNull(result);

        // Test with null processedGuids
        result = importStrategy.run(mockEntityWithExtInfo, mockResponse, mockResult,
                                   null, 1, 10, 0.1f, residualList);
        assertNotNull(result);

        // Test with null residualList
        result = importStrategy.run(mockEntityWithExtInfo, mockResponse, mockResult,
                                   processedGuids, 1, 10, 0.1f, null);
        assertNotNull(result);
    }

    @Test
    public void testPercentageCalculations() throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getGuid()).thenReturn("percentage-test-guid");

        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        Set<String> processedGuids = new HashSet<>();
        List<String> residualList = Arrays.asList();

        // Test percentage calculations with various values
        float[] testPercentages = {0.0f, 0.25f, 0.5f, 0.75f, 1.0f, 50.0f, 99.9f, 100.0f};

        for (float testPercentage : testPercentages) {
            TypesUtil.Pair<EntityMutationResponse, Float> result = importStrategy.run(mockEntityWithExtInfo, mockResponse, mockResult, processedGuids, 1, 100, testPercentage, residualList);
            assertNotNull(result);
            assertNotNull(result.right);
        }
    }

    // Test implementation of the abstract class
    private static class TestImportStrategy extends ImportStrategy {
        @Override
        public EntityMutationResponse run(EntityImportStream entityStream, AtlasImportResult importResult) throws AtlasBaseException {
            return mock(EntityMutationResponse.class);
        }

        @Override
        public TypesUtil.Pair<EntityMutationResponse, Float> run(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo,
                                                                 EntityMutationResponse ret,
                                                                 AtlasImportResult importResult,
                                                                 Set<String> processedGuids,
                                                                 int entityStreamPosition,
                                                                 int streamSize,
                                                                 float currentPercent,
                                                                 List<String> residualList) throws AtlasBaseException {
            EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
            float newPercent = currentPercent + ((float) entityStreamPosition / streamSize) * 10;
            return TypesUtil.Pair.of(mockResponse, newPercent);
        }
    }
}
