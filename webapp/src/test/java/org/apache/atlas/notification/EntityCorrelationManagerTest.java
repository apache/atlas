/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.atlas.notification;

import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.store.graph.EntityCorrelationStore;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

public class EntityCorrelationManagerTest {
    @Mock
    private EntityCorrelationStore entityCorrelationStore;

    @Mock
    private AtlasEntityHeader entityHeader1;

    @Mock
    private AtlasEntityHeader entityHeader2;

    private EntityCorrelationManager correlationManager;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        correlationManager = new EntityCorrelationManager(entityCorrelationStore);
    }

    @Test
    public void testConstructor() {
        EntityCorrelationManager manager = new EntityCorrelationManager(entityCorrelationStore);
        assertNotNull(manager);
        assertEquals(entityCorrelationStore, getEntityCorrelationStoreField(manager));
    }

    @Test
    public void testAddWithNullEntityCorrelationStore() {
        correlationManager = new EntityCorrelationManager(null);
        List<AtlasEntityHeader> entityHeaders = Collections.singletonList(entityHeader1);
        when(entityHeader1.getGuid()).thenReturn("guid1");

        correlationManager.add(true, 12345L, entityHeaders);

        verifyNoInteractions(entityHeader1, entityCorrelationStore);
    }

    @Test
    public void testAddWithSpooledFalse() {
        List<AtlasEntityHeader> entityHeaders = Collections.singletonList(entityHeader1);
        when(entityHeader1.getGuid()).thenReturn("guid1");

        correlationManager.add(false, 12345L, entityHeaders);

        verifyNoInteractions(entityCorrelationStore);
    }

    @Test
    public void testAddWithEmptyEntityHeaders() {
        correlationManager.add(true, 12345L, Collections.emptyList());
        verifyNoInteractions(entityCorrelationStore);
    }

    @Test
    public void testAddWithNullEntityHeaders() {
        correlationManager.add(true, 12345L, null);
        verifyNoInteractions(entityCorrelationStore);
    }

    @Test
    public void testAddWithValidEntityHeaders() {
        List<AtlasEntityHeader> entityHeaders = Arrays.asList(entityHeader1, entityHeader2);
        when(entityHeader1.getGuid()).thenReturn("guid1");
        when(entityHeader2.getGuid()).thenReturn("guid2");

        correlationManager.add(true, 12345L, entityHeaders);

        verify(entityCorrelationStore).add("guid1", 12345L);
        verify(entityCorrelationStore).add("guid2", 12345L);
    }

    @Test
    public void testAddWithEmptyOrNullGuids() {
        List<AtlasEntityHeader> entityHeaders = Arrays.asList(entityHeader1, entityHeader2);
        when(entityHeader1.getGuid()).thenReturn("");
        when(entityHeader2.getGuid()).thenReturn(null);

        correlationManager.add(true, 12345L, entityHeaders);

        verifyNoInteractions(entityCorrelationStore);
        verify(entityHeader1).getGuid();
        verify(entityHeader2).getGuid();
    }

    @Test
    public void testGetGuidForDeletedEntityWithNullEntityCorrelationStore() {
        correlationManager = new EntityCorrelationManager(null);

        String result = correlationManager.getGuidForDeletedEntityToBeCorrelated("test.qualified.name", 12345L);

        assertNull(result);
        verifyNoInteractions(entityCorrelationStore);
    }

    @Test
    public void testGetGuidForDeletedEntityWithInvalidTimestamp() {
        String result = correlationManager.getGuidForDeletedEntityToBeCorrelated("test.qualified.name", 0L);
        assertNull(result);

        result = correlationManager.getGuidForDeletedEntityToBeCorrelated("test.qualified.name", -1L);
        assertNull(result);

        verifyNoInteractions(entityCorrelationStore);
    }

    @Test
    public void testGetGuidForDeletedEntityWithValidInput() {
        when(entityCorrelationStore.findCorrelatedGuid("test.qualified.name", 12345L)).thenReturn("guid1");

        String result = correlationManager.getGuidForDeletedEntityToBeCorrelated("test.qualified.name", 12345L);

        assertEquals("guid1", result);
        verify(entityCorrelationStore).findCorrelatedGuid("test.qualified.name", 12345L);
    }

    @Test
    public void testGetGuidForDeletedEntityWithNoCorrelatedGuid() {
        when(entityCorrelationStore.findCorrelatedGuid("test.qualified.name", 12345L)).thenReturn(null);

        String result = correlationManager.getGuidForDeletedEntityToBeCorrelated("test.qualified.name", 12345L);

        assertNull(result);
        verify(entityCorrelationStore).findCorrelatedGuid("test.qualified.name", 12345L);
    }

    /**
     * Helper method to access private field entityCorrelationStore via reflection
     */
    private EntityCorrelationStore getEntityCorrelationStoreField(EntityCorrelationManager manager) {
        try {
            java.lang.reflect.Field field = EntityCorrelationManager.class.getDeclaredField("entityCorrelationStore");
            field.setAccessible(true);
            return (EntityCorrelationStore) field.get(manager);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access entityCorrelationStore field", e);
        }
    }
}
