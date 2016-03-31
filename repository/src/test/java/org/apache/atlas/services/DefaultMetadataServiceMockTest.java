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
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultMetadataServiceMockTest {

    @Test
    public void testShouldInvokeTypesRegistrarOnCreation() throws AtlasException {
        IBootstrapTypesRegistrar typesRegistrar = mock(IBootstrapTypesRegistrar.class);
        TypeSystem typeSystem = mock(TypeSystem.class);
        when(typeSystem.isRegistered(any(String.class))).thenReturn(true);
        DefaultMetadataService defaultMetadataService = new DefaultMetadataService(mock(MetadataRepository.class),
                mock(ITypeStore.class),
                typesRegistrar, new ArrayList<Provider<TypesChangeListener>>(),
                new ArrayList<Provider<EntityChangeListener>>(), typeSystem);

        verify(typesRegistrar).registerTypes(ReservedTypesRegistrar.getTypesDir(),
                typeSystem, defaultMetadataService);
    }
}
