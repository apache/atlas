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
package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class TypeRegistryCatchUpTest {
    private static final String TYPE_NAME = "PII";

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private AtlasTypeDefStore typeDefStore;

    private AtlasClassificationType classificationType;
    private TypeRegistryCatchUp     catchUp;
    private int                     storeLookups;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        classificationType = mock(AtlasClassificationType.class);
        storeLookups       = 0;

        // Counted so a test can assert the store is never resolved during construction: asking for it
        // eagerly reintroduces the startup dependency cycle this indirection exists to avoid.
        catchUp = new TypeRegistryCatchUp(typeRegistry, () -> {
            storeLookups++;

            return typeDefStore;
        });
    }

    @Test
    public void constructionDoesNotResolveTheStore() {
        assertEquals(storeLookups, 0);
    }

    @Test
    public void registryAlreadyHasTypeSoStoreIsNotConsulted() {
        when(typeRegistry.getClassificationTypeByName(TYPE_NAME)).thenReturn(classificationType);

        assertSame(catchUp.classificationType(TYPE_NAME), classificationType);

        verifyNoInteractions(typeDefStore);
    }

    @Test
    public void unknownTypeDoesNotTriggerReload() throws AtlasBaseException {
        when(typeRegistry.getClassificationTypeByName(TYPE_NAME)).thenReturn(null);
        when(typeDefStore.getClassificationDefByName(TYPE_NAME)).thenThrow(new AtlasBaseException("no such type"));

        assertNull(catchUp.classificationType(TYPE_NAME));

        verify(typeDefStore, never()).init();
    }

    @Test
    public void typeInStoreButNotRegistryTriggersReload() throws AtlasBaseException {
        when(typeRegistry.getClassificationTypeByName(TYPE_NAME)).thenReturn(null, null, classificationType);
        when(typeDefStore.getClassificationDefByName(TYPE_NAME)).thenReturn(new AtlasClassificationDef(TYPE_NAME));

        assertSame(catchUp.classificationType(TYPE_NAME), classificationType);

        verify(typeDefStore, times(1)).init();
    }

    @Test
    public void secondMissWithinTheIntervalReusesTheFirstReload() throws AtlasBaseException {
        when(typeRegistry.getClassificationTypeByName(TYPE_NAME)).thenReturn(null);
        when(typeDefStore.getClassificationDefByName(TYPE_NAME)).thenReturn(new AtlasClassificationDef(TYPE_NAME));

        assertNull(catchUp.classificationType(TYPE_NAME));
        assertNull(catchUp.classificationType(TYPE_NAME));

        verify(typeDefStore, times(1)).init();
    }

    @Test
    public void failedReloadIsReportedAsTypeNotFound() throws AtlasBaseException {
        when(typeRegistry.getClassificationTypeByName(anyString())).thenReturn(null);
        when(typeDefStore.getClassificationDefByName(TYPE_NAME)).thenReturn(new AtlasClassificationDef(TYPE_NAME));

        doThrow(new AtlasBaseException("reload failed")).when(typeDefStore).init();

        assertNull(catchUp.classificationType(TYPE_NAME));
    }
}
