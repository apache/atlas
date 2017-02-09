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

import org.apache.atlas.repository.graphdb.AtlasVertex;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.NullRequiredAttributeException;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;

import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestUtils.COLUMNS_ATTR_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNotNull;

public class GraphBackedRepositoryHardDeleteTest extends GraphBackedMetadataRepositoryDeleteTestBase {
    @Override
    DeleteHandler getDeleteHandler(TypeSystem typeSystem) {
        return new HardDeleteHandler(typeSystem);
    }

    @Override
    protected void assertTestDeleteEntityWithTraits(String guid) {
        //entity is deleted. So, no assertions
    }

    @Override
    protected void assertTableForTestDeleteReference(String tableId) {
        //entity is deleted. So, no assertions
    }

    @Override
    protected void assertColumnForTestDeleteReference(ITypedReferenceableInstance tableInstance) throws AtlasException {
        List<ITypedReferenceableInstance> columns =
                (List<ITypedReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME);
        assertNull(columns);
    }

    @Override
    protected void assertProcessForTestDeleteReference(ITypedReferenceableInstance processInstance) throws Exception {
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
            // expected
        }
    }

    @Override
    protected void assertDeletedColumn(ITypedReferenceableInstance tableInstance) throws AtlasException {
        assertEquals(((List<IReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME)).size(), 2);
    }

    @Override
    protected void assertTestDeleteEntities(ITypedReferenceableInstance tableInstance) {
        int vertexCount = getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, TestUtils.TABLE_TYPE).size();
        assertEquals(vertexCount, 0);

        vertexCount = getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, TestUtils.COLUMN_TYPE).size();
        assertEquals(vertexCount, 0);
    }

    @Override
    protected void assertVerticesDeleted(List<AtlasVertex> vertices) {
        assertEquals(vertices.size(), 0);
    }

    @Override
    protected void assertTestUpdateEntity_MultiplicityOneNonCompositeReference(String janeGuid) throws Exception {
        // Verify that max is no longer a subordinate of jane.
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition(janeGuid);
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        Assert.assertEquals(subordinates.size(), 1);
    }

    @Override
    protected void assertJohnForTestDisconnectBidirectionalReferences(ITypedReferenceableInstance john,
                                                                      String janeGuid) throws Exception {
        assertNull(john.get("manager"));
    }

    @Override
    protected void assertMaxForTestDisconnectBidirectionalReferences(Map<String, String> nameGuidMap)
            throws Exception {
        // Verify that the Department.employees reference to the deleted employee
        // was disconnected.
        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(nameGuidMap.get("hr"));
        List<ITypedReferenceableInstance> employees = (List<ITypedReferenceableInstance>) hrDept.get("employees");
        Assert.assertEquals(employees.size(), 3);
        String maxGuid = nameGuidMap.get("Max");
        for (ITypedReferenceableInstance employee : employees) {
            Assert.assertNotEquals(employee.getId()._getId(), maxGuid);
        }

        // Verify that the Manager.subordinates reference to the deleted employee
        // Max was disconnected.
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition(nameGuidMap.get("Jane"));
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        assertEquals(subordinates.size(), 1);

        // Verify that max's Person.mentor unidirectional reference to john was disconnected.
        ITypedReferenceableInstance john = repositoryService.getEntityDefinition(nameGuidMap.get("John"));
        assertNull(john.get("mentor"));
    }

    @Override
    protected void assertTestDisconnectUnidirectionalArrayReferenceFromClassType(
            List<ITypedReferenceableInstance> columns, String columnGuid) {
        assertEquals(columns.size(), 4);
        for (ITypedReferenceableInstance column : columns) {
            assertFalse(column.getId()._getId().equals(columnGuid));
        }
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

        AtlasVertex mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        Object object = mapOwnerVertex.getProperty("MapOwner.map.value1", String.class);
        assertNull(object);
        object = mapOwnerVertex.getProperty("MapOwner.biMap.value1", String.class);
        assertNull(object);
    }

    @Override
    protected void assertTestDeleteTargetOfMultiplicityRequiredReference() throws Exception {

        Assert.fail("Lower bound on attribute Manager.subordinates was not enforced - " +
            NullRequiredAttributeException.class.getSimpleName() + " was expected but none thrown");
    }

    @Override
    protected void assertTestLowerBoundsIgnoredOnDeletedEntities(List<ITypedReferenceableInstance> employees) {

        Assert.assertEquals(employees.size(), 1, "References to deleted employees were not disconnected");
    }

    @Override
    protected void assertTestLowerBoundsIgnoredOnCompositeDeletedEntities(String hrDeptGuid) throws Exception {

        try {
            repositoryService.getEntityDefinition(hrDeptGuid);
            Assert.fail(EntityNotFoundException.class.getSimpleName() + " was expected but none thrown");
        }
        catch (EntityNotFoundException e) {
            // good
        }
    }

    @Override
    protected void verifyTestDeleteEntityWithDuplicateReferenceListElements(List columnsPropertyValue) {

        // With hard deletes enabled, verify that duplicate edge IDs for deleted edges
        // were removed from the array property list.
        Assert.assertEquals(columnsPropertyValue.size(), 2);
    }
}
