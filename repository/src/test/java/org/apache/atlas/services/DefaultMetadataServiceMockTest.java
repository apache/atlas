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

package org.apache.atlas.services;

import com.google.inject.Provider;

import org.apache.atlas.AtlasException;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.TypesChangeListener;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.commons.configuration.Configuration;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class DefaultMetadataServiceMockTest {

    @Mock
    private IBootstrapTypesRegistrar typesRegistrar;

    @Mock
    private TypeSystem typeSystem;

    @Mock
    private MetadataRepository metadataRepository;

    @Mock
    private ITypeStore typeStore;

    @Mock
    private Configuration configuration;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testShouldInvokeTypesRegistrarOnCreation() throws AtlasException {
        when(typeSystem.isRegistered(any(String.class))).thenReturn(true);
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(false);
        DefaultMetadataService defaultMetadataService = new DefaultMetadataService(mock(MetadataRepository.class),
                mock(ITypeStore.class),
                typesRegistrar, new ArrayList<Provider<TypesChangeListener>>(),
                new ArrayList<Provider<EntityChangeListener>>(), typeSystem, configuration, null);

        verify(typesRegistrar).registerTypes(ReservedTypesRegistrar.getTypesDir(),
                typeSystem, defaultMetadataService);
    }

    @Test
    public void testShouldNotRestoreTypesIfHAIsEnabled() throws AtlasException {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        new DefaultMetadataService(metadataRepository, typeStore,
                typesRegistrar, new ArrayList<Provider<TypesChangeListener>>(),
                new ArrayList<Provider<EntityChangeListener>>(), typeSystem, configuration, null);

        verifyZeroInteractions(typeStore);
        verify(typeSystem, never()).defineTypes(Matchers.<TypesDef>any());
        verifyZeroInteractions(typesRegistrar);
    }

    @Test
    public void testShouldRestoreTypeSystemOnServerActive() throws AtlasException {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        TypesDef typesDef = mock(TypesDef.class);
        when(typeStore.restore()).thenReturn(typesDef);
        when(typeSystem.isRegistered(any(String.class))).thenReturn(true);

        DefaultMetadataService defaultMetadataService = new DefaultMetadataService(metadataRepository,
                typeStore,
                typesRegistrar, new ArrayList<Provider<TypesChangeListener>>(),
                new ArrayList<Provider<EntityChangeListener>>(), typeSystem, configuration, null);
        defaultMetadataService.instanceIsActive();

        verify(typeStore).restore();
        verify(typeSystem).defineTypes(typesDef);
        verify(typesRegistrar).registerTypes(ReservedTypesRegistrar.getTypesDir(),
                typeSystem, defaultMetadataService);
    }

    @Test
    public void testShouldOnlyRestoreCacheOnServerActiveIfAlreadyDoneOnce() throws AtlasException {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        TypesDef typesDef = mock(TypesDef.class);
        when(typeStore.restore()).thenReturn(typesDef);
        when(typeSystem.isRegistered(any(String.class))).thenReturn(true);

        TypeSystem.TransientTypeSystem transientTypeSystem = mock(TypeSystem.TransientTypeSystem.class);
        HashMap<String, IDataType> typesAdded = new HashMap<>();
        when(transientTypeSystem.getTypesAdded()).thenReturn(typesAdded);
        when(typeSystem.createTransientTypeSystem(typesDef, true)).
                thenReturn(transientTypeSystem);
        DefaultMetadataService defaultMetadataService = new DefaultMetadataService(metadataRepository,
                typeStore,
                typesRegistrar, new ArrayList<Provider<TypesChangeListener>>(),
                new ArrayList<Provider<EntityChangeListener>>(), typeSystem, configuration, null);

        defaultMetadataService.instanceIsActive();
        defaultMetadataService.instanceIsPassive();
        defaultMetadataService.instanceIsActive();

        verify(typeStore, times(2)).restore();
        verify(typeSystem, times(1)).defineTypes(typesDef);
        verify(typesRegistrar, times(1)).
                registerTypes(ReservedTypesRegistrar.getTypesDir(), typeSystem, defaultMetadataService);
        verify(typeSystem, times(1)).createTransientTypeSystem(typesDef, true);
        verify(typeSystem, times(1)).commitTypes(typesAdded);
    }
}
