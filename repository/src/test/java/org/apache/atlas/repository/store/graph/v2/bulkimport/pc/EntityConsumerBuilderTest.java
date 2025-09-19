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
package org.apache.atlas.repository.store.graph.v2.bulkimport.pc;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;

public class EntityConsumerBuilderTest {
    private EntityConsumerBuilder entityConsumerBuilder;
    private AtlasTypeRegistry mockTypeRegistry;
    private AtlasGraph mockAtlasGraph;
    private AtlasEntityStoreV2 mockEntityStore;
    private EntityGraphRetriever mockEntityRetriever;
    private AtlasGraph mockAtlasGraphBulk;
    private AtlasEntityStoreV2 mockEntityStoreBulk;
    private EntityGraphRetriever mockEntityRetrieverBulk;

    @BeforeMethod
    public void setUp() {
        mockTypeRegistry = mock(AtlasTypeRegistry.class);
        mockAtlasGraph = mock(AtlasGraph.class);
        mockEntityStore = mock(AtlasEntityStoreV2.class);
        mockEntityRetriever = mock(EntityGraphRetriever.class);
        mockAtlasGraphBulk = mock(AtlasGraph.class);
        mockEntityStoreBulk = mock(AtlasEntityStoreV2.class);
        mockEntityRetrieverBulk = mock(EntityGraphRetriever.class);

        entityConsumerBuilder = new EntityConsumerBuilder(
                mockTypeRegistry,
                mockAtlasGraph,
                mockEntityStore,
                mockEntityRetriever,
                mockAtlasGraphBulk,
                mockEntityStoreBulk,
                mockEntityRetrieverBulk, 100, true);
    }

    @Test
    public void testConstructor() {
        assertNotNull(entityConsumerBuilder);
    }

    @Test
    public void testConstructorWithDifferentBatchSizes() {
        // Test with different batch sizes
        EntityConsumerBuilder builder1 = new EntityConsumerBuilder(
                mockTypeRegistry, mockAtlasGraph, mockEntityStore, mockEntityRetriever,
                mockAtlasGraphBulk, mockEntityStoreBulk, mockEntityRetrieverBulk, 1, false);
        assertNotNull(builder1);

        EntityConsumerBuilder builder2 = new EntityConsumerBuilder(
                mockTypeRegistry, mockAtlasGraph, mockEntityStore, mockEntityRetriever,
                mockAtlasGraphBulk, mockEntityStoreBulk, mockEntityRetrieverBulk, 1000, true);
        assertNotNull(builder2);

        EntityConsumerBuilder builder3 = new EntityConsumerBuilder(
                mockTypeRegistry, mockAtlasGraph, mockEntityStore, mockEntityRetriever,
                mockAtlasGraphBulk, mockEntityStoreBulk, mockEntityRetrieverBulk, 0, false);
        assertNotNull(builder3);
    }

    @Test
    public void testConstructorWithNullParameters() {
        // Test constructor with null parameters
        EntityConsumerBuilder builderWithNulls = new EntityConsumerBuilder(null, null, null, null, null, null, null, 50, false);
        assertNotNull(builderWithNulls);
    }

    @Test
    public void testBuild() {
        BlockingQueue<AtlasEntity.AtlasEntityWithExtInfo> queue = new ArrayBlockingQueue<>(10);

        EntityConsumer consumer = entityConsumerBuilder.build(queue);
        assertNotNull(consumer);
    }

    @Test
    public void testBuildWithDifferentQueueSizes() {
        // Test with different queue sizes
        BlockingQueue<AtlasEntity.AtlasEntityWithExtInfo> smallQueue = new ArrayBlockingQueue<>(1);
        EntityConsumer consumer1 = entityConsumerBuilder.build(smallQueue);
        assertNotNull(consumer1);

        BlockingQueue<AtlasEntity.AtlasEntityWithExtInfo> largeQueue = new ArrayBlockingQueue<>(1000);
        EntityConsumer consumer2 = entityConsumerBuilder.build(largeQueue);
        assertNotNull(consumer2);
    }

    @Test
    public void testBuildWithNullQueue() {
        // Test with null queue - should still create consumer
        EntityConsumer consumer = entityConsumerBuilder.build(null);
        assertNotNull(consumer);
    }

    @Test
    public void testBuildMultipleTimes() {
        BlockingQueue<AtlasEntity.AtlasEntityWithExtInfo> queue = new ArrayBlockingQueue<>(10);

        // Build multiple consumers with same builder
        EntityConsumer consumer1 = entityConsumerBuilder.build(queue);
        EntityConsumer consumer2 = entityConsumerBuilder.build(queue);
        EntityConsumer consumer3 = entityConsumerBuilder.build(queue);

        assertNotNull(consumer1);
        assertNotNull(consumer2);
        assertNotNull(consumer3);

        // Each consumer should be a different instance
        assertNotSame(consumer1, consumer2);
        assertNotSame(consumer2, consumer3);
        assertNotSame(consumer1, consumer3);
    }

    @Test
    public void testFieldAccessUsingReflection() throws Exception {
        // Test access to private fields using reflection
        Field typeRegistryField = EntityConsumerBuilder.class.getDeclaredField("typeRegistry");
        typeRegistryField.setAccessible(true);
        AtlasTypeRegistry retrievedTypeRegistry = (AtlasTypeRegistry) typeRegistryField.get(entityConsumerBuilder);
        assertEquals(mockTypeRegistry, retrievedTypeRegistry);

        Field atlasGraphField = EntityConsumerBuilder.class.getDeclaredField("atlasGraph");
        atlasGraphField.setAccessible(true);
        AtlasGraph retrievedAtlasGraph = (AtlasGraph) atlasGraphField.get(entityConsumerBuilder);
        assertEquals(mockAtlasGraph, retrievedAtlasGraph);

        Field entityStoreField = EntityConsumerBuilder.class.getDeclaredField("entityStore");
        entityStoreField.setAccessible(true);
        AtlasEntityStoreV2 retrievedEntityStore = (AtlasEntityStoreV2) entityStoreField.get(entityConsumerBuilder);
        assertEquals(mockEntityStore, retrievedEntityStore);

        Field entityRetrieverField = EntityConsumerBuilder.class.getDeclaredField("entityRetriever");
        entityRetrieverField.setAccessible(true);
        EntityGraphRetriever retrievedEntityRetriever = (EntityGraphRetriever) entityRetrieverField.get(entityConsumerBuilder);
        assertEquals(mockEntityRetriever, retrievedEntityRetriever);

        Field atlasGraphBulkField = EntityConsumerBuilder.class.getDeclaredField("atlasGraphBulk");
        atlasGraphBulkField.setAccessible(true);
        AtlasGraph retrievedAtlasGraphBulk = (AtlasGraph) atlasGraphBulkField.get(entityConsumerBuilder);
        assertEquals(mockAtlasGraphBulk, retrievedAtlasGraphBulk);

        Field entityStoreBulkField = EntityConsumerBuilder.class.getDeclaredField("entityStoreBulk");
        entityStoreBulkField.setAccessible(true);
        AtlasEntityStoreV2 retrievedEntityStoreBulk = (AtlasEntityStoreV2) entityStoreBulkField.get(entityConsumerBuilder);
        assertEquals(mockEntityStoreBulk, retrievedEntityStoreBulk);

        Field entityRetrieverBulkField = EntityConsumerBuilder.class.getDeclaredField("entityRetrieverBulk");
        entityRetrieverBulkField.setAccessible(true);
        EntityGraphRetriever retrievedEntityRetrieverBulk = (EntityGraphRetriever) entityRetrieverBulkField.get(entityConsumerBuilder);
        assertEquals(mockEntityRetrieverBulk, retrievedEntityRetrieverBulk);

        Field batchSizeField = EntityConsumerBuilder.class.getDeclaredField("batchSize");
        batchSizeField.setAccessible(true);
        int retrievedBatchSize = (int) batchSizeField.get(entityConsumerBuilder);
        assertEquals(100, retrievedBatchSize);

        Field isMigrationImportField = EntityConsumerBuilder.class.getDeclaredField("isMigrationImport");
        isMigrationImportField.setAccessible(true);
        boolean retrievedIsMigrationImport = (boolean) isMigrationImportField.get(entityConsumerBuilder);
        assertEquals(true, retrievedIsMigrationImport);
    }

    @Test
    public void testBuilderWithVariousMigrationSettings() {
        // Test with migration import true
        EntityConsumerBuilder migrationBuilder = new EntityConsumerBuilder(
                mockTypeRegistry, mockAtlasGraph, mockEntityStore, mockEntityRetriever,
                mockAtlasGraphBulk, mockEntityStoreBulk, mockEntityRetrieverBulk, 50, true);

        BlockingQueue<AtlasEntity.AtlasEntityWithExtInfo> queue = new ArrayBlockingQueue<>(10);
        EntityConsumer migrationConsumer = migrationBuilder.build(queue);
        assertNotNull(migrationConsumer);

        // Test with migration import false
        EntityConsumerBuilder regularBuilder = new EntityConsumerBuilder(
                mockTypeRegistry, mockAtlasGraph, mockEntityStore, mockEntityRetriever,
                mockAtlasGraphBulk, mockEntityStoreBulk, mockEntityRetrieverBulk, 50, false);

        EntityConsumer regularConsumer = regularBuilder.build(queue);
        assertNotNull(regularConsumer);
    }

    @Test
    public void testBuilderWithEdgeCaseBatchSizes() throws Exception {
        // Test with negative batch size
        EntityConsumerBuilder negativeBuilder = new EntityConsumerBuilder(
                mockTypeRegistry, mockAtlasGraph, mockEntityStore, mockEntityRetriever,
                mockAtlasGraphBulk, mockEntityStoreBulk, mockEntityRetrieverBulk, -1, false);

        Field batchSizeField = EntityConsumerBuilder.class.getDeclaredField("batchSize");
        batchSizeField.setAccessible(true);
        int retrievedBatchSize = (int) batchSizeField.get(negativeBuilder);
        assertEquals(-1, retrievedBatchSize);

        BlockingQueue<AtlasEntity.AtlasEntityWithExtInfo> queue = new ArrayBlockingQueue<>(10);
        EntityConsumer consumer = negativeBuilder.build(queue);
        assertNotNull(consumer);

        // Test with very large batch size
        EntityConsumerBuilder largeBuilder = new EntityConsumerBuilder(
                mockTypeRegistry, mockAtlasGraph, mockEntityStore, mockEntityRetriever,
                mockAtlasGraphBulk, mockEntityStoreBulk, mockEntityRetrieverBulk, Integer.MAX_VALUE, true);

        batchSizeField = EntityConsumerBuilder.class.getDeclaredField("batchSize");
        batchSizeField.setAccessible(true);
        retrievedBatchSize = (int) batchSizeField.get(largeBuilder);
        assertEquals(Integer.MAX_VALUE, retrievedBatchSize);

        EntityConsumer largeConsumer = largeBuilder.build(queue);
        assertNotNull(largeConsumer);
    }
}
