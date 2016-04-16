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
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNotNull;

public class GraphBackedRepositoryHardDeleteTest extends GraphBackedMetadataRepositoryDeleteTestBase {
    @Override
    DeleteHandler getDeleteHandler(TypeSystem typeSystem) {
        return new HardDeleteHandler(typeSystem);
    }

    @Override
    protected void assertTestDeleteReference(ITypedReferenceableInstance processInstance) throws Exception {
        //assert that outputs is empty
        ITypedReferenceableInstance newProcess =
                repositoryService.getEntityDefinition(processInstance.getId()._getId());
        assertNull(newProcess.get(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS));
    }

    @Override
    protected void assertEntityDeleted(String id) throws Exception {
        try {
            repositoryService.getEntityDefinition(id);
            fail("Expected EntityNotFoundException");
        } catch(EntityNotFoundException e) {
            //expected
        }
    }

    @Override
    protected void assertTestDeleteEntities(ITypedReferenceableInstance tableInstance) {
        int vertexCount = getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, TestUtils.TABLE_TYPE).size();
        assertEquals(vertexCount, 0);

        vertexCount = getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, TestUtils.COLUMN_TYPE).size();
        assertEquals(vertexCount, 0);
    }

    @Override
    protected void assertVerticesDeleted(List<Vertex> vertices) {
        assertEquals(vertices.size(), 0);
    }

    @Override
    protected void assertTestUpdateEntity_MultiplicityOneNonCompositeReference() throws Exception {
        // Verify that max is no longer a subordinate of jane.
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition("Manager", "name", "Jane");
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        Assert.assertEquals(subordinates.size(), 1);
    }

    @Override
    protected void assertTestDisconnectBidirectionalReferences() throws Exception {
        // Verify that the Manager.subordinates reference to the deleted employee
        // Max was disconnected.
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition("Manager", "name", "Jane");
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        assertEquals(subordinates.size(), 1);
    }

    @Override
    protected void assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(String structContainerGuid)
            throws Exception {
        // Verify that the unidirectional references from the struct and trait instances
        // to the deleted entities were disconnected.
        ITypedReferenceableInstance structContainerConvertedEntity =
                repositoryService.getEntityDefinition(structContainerGuid);
        ITypedStruct struct = (ITypedStruct) structContainerConvertedEntity.get("struct");
        assertNull(struct.get("target"));
        IStruct trait = structContainerConvertedEntity.getTrait("TestTrait");
        assertNotNull(trait);
        assertNull(trait.get("target"));
    }

    @Override
    protected void assertTestDisconnectMapReferenceFromClassType(String mapOwnerGuid) throws Exception {
        // Verify map references from mapOwner were disconnected.
        ITypedReferenceableInstance mapOwnerInstance = repositoryService.getEntityDefinition(mapOwnerGuid);
        assertNull(mapOwnerInstance.get("map"));
        assertNull(mapOwnerInstance.get("biMap"));

        Vertex mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        Object object = mapOwnerVertex.getProperty("MapOwner.map.value1");
        assertNull(object);
        object = mapOwnerVertex.getProperty("MapOwner.biMap.value1");
        assertNull(object);
    }
}
