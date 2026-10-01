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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.IFullTextMapper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.TypeRegistryVersionGate;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link EntityGraphMapper}. */
public class EntityGraphMapperTest {
    @Test
    public void validateAndNormalizeForUpdate_refreshesRegistryEvenWhenTypeExists() throws Exception {
        AtlasTypeRegistry registry = mock(AtlasTypeRegistry.class);
        TypeRegistryVersionGate gate = mock(TypeRegistryVersionGate.class);
        AtlasClassificationType cType = mock(AtlasClassificationType.class);

        when(registry.getClassificationTypeByName("c1")).thenReturn(cType);
        when(gate.ensureUpToDate()).thenReturn(true);
        doAnswer(invocation -> null).when(cType).validateValueForUpdate(any(), anyString(), any());
        doAnswer(invocation -> null).when(cType).getNormalizedValueForUpdate(any());

        EntityGraphMapper mapper = newMapper(registry);
        mapper.setTypeRegistryVersionGate(gate);
        mapper.validateAndNormalizeForUpdate(new AtlasClassification("c1"));

        verify(gate).ensureUpToDate();
    }

    private static EntityGraphMapper newMapper(AtlasTypeRegistry registry) {
        return new EntityGraphMapper(
                mock(DeleteHandlerDelegate.class),
                registry,
                mock(AtlasGraph.class),
                mock(AtlasRelationshipStore.class),
                mock(IAtlasEntityChangeNotifier.class),
                mock(AtlasInstanceConverter.class),
                mock(IFullTextMapper.class),
                mock(TaskManagement.class));
    }
}
