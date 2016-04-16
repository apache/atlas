/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class GraphBackedRepositorySoftDeleteTest extends GraphBackedMetadataRepositoryDeleteTestBase {
    @Override
    DeleteHandler getDeleteHandler(TypeSystem typeSystem) {
        return new SoftDeleteHandler(typeSystem);
    }

    @Override
    protected void assertTestDeleteReference(ITypedReferenceableInstance expected) throws Exception {
        ITypedReferenceableInstance process = repositoryService.getEntityDefinition(expected.getId()._getId());
        List<ITypedReferenceableInstance> outputs =
                (List<ITypedReferenceableInstance>) process.get(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS);
        List<ITypedReferenceableInstance> expectedOutputs =
                (List<ITypedReferenceableInstance>) process.get(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS);
        assertEquals(outputs.size(), expectedOutputs.size());
    }

    @Override
    protected void assertEntityDeleted(String id) throws Exception {
        ITypedReferenceableInstance entity = repositoryService.getEntityDefinition(id);
        assertEquals(entity.getId().getState(), Id.EntityState.DELETED);
    }

    @Override
    protected void assertTestDeleteEntities(ITypedReferenceableInstance expected) throws Exception {
        //Assert that the deleted table can be fully constructed back
        ITypedReferenceableInstance table = repositoryService.getEntityDefinition(expected.getId()._getId());
        List<ITypedReferenceableInstance> columns =
                (List<ITypedReferenceableInstance>) table.get(TestUtils.COLUMNS_ATTR_NAME);
        List<ITypedReferenceableInstance> expectedColumns =
                (List<ITypedReferenceableInstance>) table.get(TestUtils.COLUMNS_ATTR_NAME);
        assertEquals(columns.size(), expectedColumns.size());
    }

    @Override
    protected void assertVerticesDeleted(List<Vertex> vertices) {
        for (Vertex vertex : vertices) {
            assertEquals(vertex.getProperty(Constants.STATE_PROPERTY_KEY), Id.EntityState.DELETED.name());
        }
    }

    @Override
    protected void assertTestUpdateEntity_MultiplicityOneNonCompositeReference() throws Exception {
        // Verify that max is no longer a subordinate of jane.
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition("Manager", "name", "Jane");
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        Assert.assertEquals(subordinates.size(), 2);
    }

    @Override
    protected void assertTestDisconnectBidirectionalReferences() throws Exception {
        // Verify that the Manager.subordinates still references deleted employee
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition("Manager", "name", "Jane");
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        assertEquals(subordinates.size(), 2);
    }

    @Override
    protected void assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(String structContainerGuid)
            throws Exception {
        // Verify that the unidirectional references from the struct and trait instances
        // to the deleted entities were disconnected.
        ITypedReferenceableInstance structContainerConvertedEntity =
                repositoryService.getEntityDefinition(structContainerGuid);
        ITypedStruct struct = (ITypedStruct) structContainerConvertedEntity.get("struct");
        assertNotNull(struct.get("target"));
        IStruct trait = structContainerConvertedEntity.getTrait("TestTrait");
        assertNotNull(trait);
        assertNotNull(trait.get("target"));

    }

    @Override
    protected void assertTestDisconnectMapReferenceFromClassType(String mapOwnerGuid) throws Exception {
        ITypedReferenceableInstance mapOwnerInstance = repositoryService.getEntityDefinition(mapOwnerGuid);
        Map<String, ITypedReferenceableInstance> map =
                (Map<String, ITypedReferenceableInstance>) mapOwnerInstance.get("map");
        assertNotNull(map);
        assertEquals(map.size(), 1);
        Map<String, ITypedReferenceableInstance> biMap =
                (Map<String, ITypedReferenceableInstance>) mapOwnerInstance.get("biMap");
        assertNotNull(biMap);
        assertEquals(biMap.size(), 1);
    }
}
