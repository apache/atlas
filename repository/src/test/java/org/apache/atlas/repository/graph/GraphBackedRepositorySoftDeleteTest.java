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

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.Id.EntityState;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;

import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestUtils.COLUMNS_ATTR_NAME;
import static org.apache.atlas.TestUtils.NAME;
import static org.apache.atlas.TestUtils.PII;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class GraphBackedRepositorySoftDeleteTest extends GraphBackedMetadataRepositoryDeleteTestBase {
    @Override
    DeleteHandler getDeleteHandler(TypeSystem typeSystem) {
        return new SoftDeleteHandler(typeSystem);
    }

    @Override
    protected void assertTestDeleteEntityWithTraits(String guid) throws Exception {
        ITypedReferenceableInstance instance = repositoryService.getEntityDefinition(guid);
        assertTrue(instance.getTraits().contains(PII));
    }

    @Override
    protected void assertTableForTestDeleteReference(String tableId) throws Exception  {
        ITypedReferenceableInstance table = repositoryService.getEntityDefinition(tableId);
        assertNotNull(table.get(NAME));
        assertNotNull(table.get("description"));
        assertNotNull(table.get("type"));
        assertNotNull(table.get("tableType"));
        assertNotNull(table.get("created"));

        Id dbId = (Id) table.get("database");
        assertNotNull(dbId);

        ITypedReferenceableInstance db = repositoryService.getEntityDefinition(dbId.getId()._getId());
        assertNotNull(db);
        assertEquals(db.getId().getState(), Id.EntityState.ACTIVE);
    }

    @Override
    protected void assertColumnForTestDeleteReference(ITypedReferenceableInstance tableInstance) throws AtlasException {
        List<ITypedReferenceableInstance> columns =
                (List<ITypedReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME);
        assertEquals(columns.size(), 1);
        assertEquals(columns.get(0).getId().getState(), Id.EntityState.DELETED);
    }

    @Override
    protected void assertProcessForTestDeleteReference(ITypedReferenceableInstance expected) throws Exception {
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
    protected void assertDeletedColumn(ITypedReferenceableInstance tableInstance) throws AtlasException {
        List<IReferenceableInstance> columns = (List<IReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME);
        assertEquals(columns.size(), 3);
        assertEquals(columns.get(0).getId().getState(), Id.EntityState.DELETED);
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
        assertNotNull(table.get("database"));
    }

    @Override
    protected void assertVerticesDeleted(List<AtlasVertex> vertices) {
        for (AtlasVertex vertex : vertices) {
            assertEquals(AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.STATE_PROPERTY_KEY, String.class), Id.EntityState.DELETED.name());
        }
    }

    @Override
    protected void assertTestUpdateEntity_MultiplicityOneNonCompositeReference(String janeGuid) throws Exception {
        // Verify Jane's subordinates reference cardinality is still 2.
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition(janeGuid);
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        Assert.assertEquals(subordinates.size(), 2);
    }

    @Override
    protected void assertJohnForTestDisconnectBidirectionalReferences(ITypedReferenceableInstance john, String janeGuid)
            throws Exception {
        Id mgr = (Id) john.get("manager");
        assertNotNull(mgr);
        assertEquals(mgr._getId(), janeGuid);
        assertEquals(mgr.getState(), Id.EntityState.DELETED);
    }

    @Override
    protected void assertMaxForTestDisconnectBidirectionalReferences(Map<String, String> nameGuidMap) throws Exception {
        // Verify that the Department.employees reference to the deleted employee
        // was disconnected.
        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(nameGuidMap.get("hr"));
        List<ITypedReferenceableInstance> employees = (List<ITypedReferenceableInstance>) hrDept.get("employees");
        Assert.assertEquals(employees.size(), 4);
        String maxGuid = nameGuidMap.get("Max");
        for (ITypedReferenceableInstance employee : employees) {
            if (employee.getId()._getId().equals(maxGuid)) {
                assertEquals(employee.getId().getState(), Id.EntityState.DELETED);
            }
        }

        // Verify that the Manager.subordinates still references deleted employee
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition(nameGuidMap.get("Jane"));
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        assertEquals(subordinates.size(), 2);
        for (ITypedReferenceableInstance subordinate : subordinates) {
            if (subordinate.getId()._getId().equals(maxGuid)) {
                assertEquals(subordinate.getId().getState(), Id.EntityState.DELETED);
            }
        }

        // Verify that max's Person.mentor unidirectional reference to john was disconnected.
        ITypedReferenceableInstance john = repositoryService.getEntityDefinition(nameGuidMap.get("John"));
        Id mentor = (Id) john.get("mentor");
        assertEquals(mentor._getId(), maxGuid);
        assertEquals(mentor.getState(), Id.EntityState.DELETED);
    }

    @Override
    protected void assertTestDisconnectUnidirectionalArrayReferenceFromClassType(
            List<ITypedReferenceableInstance> columns, String columnGuid) {
        Assert.assertEquals(columns.size(), 5);
        for (ITypedReferenceableInstance column : columns) {
            if (column.getId()._getId().equals(columnGuid)) {
                assertEquals(column.getId().getState(), Id.EntityState.DELETED);
            } else {
                assertEquals(column.getId().getState(), Id.EntityState.ACTIVE);
            }
        }

    }

    @Override
    protected void assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(String structContainerGuid)
            throws Exception {
        // Verify that the unidirectional references from the struct and trait instances
        // to the deleted entities were not disconnected.
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

    @Override
    protected void assertTestDeleteTargetOfMultiplicityRequiredReference() throws Exception {
        // No-op - it's ok that no exception was thrown if soft deletes are enabled.
    }

    @Override
    protected void assertTestLowerBoundsIgnoredOnDeletedEntities(List<ITypedReferenceableInstance> employees) {

        Assert.assertEquals(employees.size(), 4, "References to deleted employees should not have been disconnected with soft deletes enabled");
    }

    @Override
    protected void assertTestLowerBoundsIgnoredOnCompositeDeletedEntities(String hrDeptGuid) throws Exception {

        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Assert.assertEquals(hrDept.getId().getState(), EntityState.DELETED);
    }

    @Override
    protected void verifyTestDeleteEntityWithDuplicateReferenceListElements(List columnsPropertyValue) {

        // With soft deletes enabled, verify that edge IDs for deleted edges
        // were not removed from the array property list.
        Assert.assertEquals(columnsPropertyValue.size(), 4);
    }
}
