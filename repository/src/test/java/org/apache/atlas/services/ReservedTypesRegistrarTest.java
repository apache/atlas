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

import org.apache.atlas.AtlasException;
import org.apache.atlas.TestUtils;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ReservedTypesRegistrarTest {

    @Mock
    private TypeSystem typeSystem;

    @Mock
    private MetadataService metadataService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRegistrationWithNoFiles() throws AtlasException {
        IBootstrapTypesRegistrar bootstrapTypesRegistrar = new ReservedTypesRegistrar();
        bootstrapTypesRegistrar.registerTypes("/some/dir/", typeSystem, metadataService);
        verifyZeroInteractions(typeSystem);
    }

    @Test
    public void testRegisterFirstChecksClassTypeIsRegistered() throws AtlasException {
        ReservedTypesRegistrar reservedTypesRegistrar = new ReservedTypesRegistrar();
        TypesDef typesDef = TestUtils.defineHiveTypes();
        String typesJson = TypesSerialization.toJson(typesDef);
        reservedTypesRegistrar.registerType(typeSystem, metadataService, "/some/file/model.json", typesJson);
        InOrder inOrder = inOrder(typeSystem, metadataService);
        inOrder.verify(typeSystem).isRegistered(typesDef.classTypesAsJavaList().get(0).typeName);
        inOrder.verify(metadataService).createType(typesJson);
    }

    @Test
    public void testRegisterCreatesTypesUsingMetadataService() throws AtlasException {
        ReservedTypesRegistrar reservedTypesRegistrar = new ReservedTypesRegistrar();
        TypesDef typesDef = TestUtils.defineHiveTypes();
        String typesJson = TypesSerialization.toJson(typesDef);
        reservedTypesRegistrar.registerType(typeSystem, metadataService, "/some/file/model.json", typesJson);
        verify(metadataService).createType(typesJson);
    }

    @Test(expectedExceptions = ReservedTypesRegistrationException.class)
    public void testRegisterFailsIfErrorInJson() throws AtlasException {
        ReservedTypesRegistrar reservedTypesRegistrar = new ReservedTypesRegistrar();
        reservedTypesRegistrar.registerType(typeSystem, metadataService, "/some/file/model.json", "invalid json");
    }

    @Test(expectedExceptions = AtlasException.class)
    public void testRegisterFailsOnTypeCreationException() throws AtlasException {
        ReservedTypesRegistrar reservedTypesRegistrar = new ReservedTypesRegistrar();
        TypesDef typesDef = TestUtils.defineHiveTypes();
        String typesJson = TypesSerialization.toJson(typesDef);
        when(metadataService.createType(typesJson)).thenThrow(new AtlasException("some exception"));
        reservedTypesRegistrar.registerType(typeSystem, metadataService, "/some/file/model.json", typesJson);
    }

    @Test
    public void testShouldNotRegisterIfTypeIsAlreadyRegistered() throws AtlasException {
        ReservedTypesRegistrar reservedTypesRegistrar = new ReservedTypesRegistrar();
        TypesDef typesDef = TestUtils.defineHiveTypes();
        String typesJson = TypesSerialization.toJson(typesDef);
        when(typeSystem.isRegistered(typesDef.classTypesAsJavaList().get(0).typeName)).thenReturn(true);
        reservedTypesRegistrar.registerType(typeSystem, metadataService, "/some/file/model.json", typesJson);
        verifyZeroInteractions(metadataService);
    }
}
