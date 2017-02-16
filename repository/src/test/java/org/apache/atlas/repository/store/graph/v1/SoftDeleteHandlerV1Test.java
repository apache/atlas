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
package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasClient;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.persistence.Id;
import org.testng.Assert;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestUtils.COLUMNS_ATTR_NAME;
import static org.apache.atlas.TestUtils.NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class SoftDeleteHandlerV1Test extends AtlasDeleteHandlerV1Test {

    @Inject
    MetadataService metadataService;

    @Override
    DeleteHandlerV1 getDeleteHandler(final AtlasTypeRegistry typeRegistry) {
        return new SoftDeleteHandlerV1(typeRegistry);
    }

    @Override
    protected void assertDeletedColumn(final AtlasEntity.AtlasEntityWithExtInfo tableInstance) throws AtlasBaseException {
        final List<AtlasObjectId> columns = (List<AtlasObjectId>) tableInstance.getEntity().getAttribute(COLUMNS_ATTR_NAME);
        Assert.assertEquals(columns.size(), 3);

        final AtlasEntity.AtlasEntityWithExtInfo colDeleted = entityStore.getById(columns.get(0).getGuid());
        assertEquals(colDeleted.getEntity().getStatus(), AtlasEntity.Status.DELETED);
    }

    @Override
    protected void assertTestDeleteEntities(final AtlasEntity.AtlasEntityWithExtInfo tableInstance) throws Exception {
        //Assert that the deleted table can be fully constructed back
        List<IReferenceableInstance> columns = (List<IReferenceableInstance>) tableInstance.getEntity().getAttribute(COLUMNS_ATTR_NAME);
        assertEquals(columns.size(), 3);
        assertNotNull(tableInstance.getEntity().getAttribute("database"));
    }

    @Override
    protected void assertTableForTestDeleteReference(final String tableId) throws Exception {

        ITypedReferenceableInstance table = metadataService.getEntityDefinition(tableId);
        assertNotNull(table.get(NAME));
        assertNotNull(table.get("description"));
        assertNotNull(table.get("type"));
        assertNotNull(table.get("tableType"));
        assertNotNull(table.get("created"));

        Id dbId = (Id) table.get("database");
        assertNotNull(dbId);

        ITypedReferenceableInstance db = metadataService.getEntityDefinition(dbId.getId()._getId());
        assertNotNull(db);
        assertEquals(db.getId().getState(), Id.EntityState.ACTIVE);

    }

    @Override
    protected void assertColumnForTestDeleteReference(final AtlasEntity.AtlasEntityWithExtInfo tableInstance) throws AtlasBaseException {
        List<AtlasObjectId> columns = (List<AtlasObjectId>) tableInstance.getEntity().getAttribute(COLUMNS_ATTR_NAME);
        assertEquals(columns.size(), 1);

        final AtlasEntity.AtlasEntityWithExtInfo byId = entityStore.getById(columns.get(0).getGuid());
        assertEquals(byId.getEntity().getStatus(), AtlasEntity.Status.DELETED);
    }

    @Override
    protected void assertProcessForTestDeleteReference(final AtlasEntityHeader processInstance) throws Exception {
        //
        ITypedReferenceableInstance process = metadataService.getEntityDefinition(processInstance.getGuid());
        List<ITypedReferenceableInstance> outputs =
            (List<ITypedReferenceableInstance>) process.get(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS);
        List<ITypedReferenceableInstance> expectedOutputs =
            (List<ITypedReferenceableInstance>) process.get(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS);
        assertEquals(outputs.size(), expectedOutputs.size());

    }

    @Override
    protected void assertEntityDeleted(final String id) throws Exception {
        final AtlasEntity.AtlasEntityWithExtInfo byId = entityStore.getById(id);
        assertEquals(byId.getEntity().getStatus(), AtlasEntity.Status.DELETED);
    }

    @Override
    protected void assertTestUpdateEntity_MultiplicityOneNonCompositeReference(final String janeGuid) throws Exception {
        // Verify Jane's subordinates reference cardinality is still 2.
        ITypedReferenceableInstance jane = metadataService.getEntityDefinition(janeGuid);
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        Assert.assertEquals(subordinates.size(), 2);
    }

    @Override
    protected void assertJohnForTestDisconnectBidirectionalReferences(final AtlasEntity.AtlasEntityWithExtInfo john, final String janeGuid) throws Exception {
        AtlasObjectId mgr = (AtlasObjectId) john.getEntity().getAttribute("manager");
        assertNotNull(mgr);
        assertEquals(mgr.getGuid(), janeGuid);


        final AtlasEntity.AtlasEntityWithExtInfo mgrEntity = entityStore.getById(mgr.getGuid());
        assertEquals(mgrEntity.getEntity().getStatus(), AtlasEntity.Status.DELETED);
    }

    @Override
    protected void assertMaxForTestDisconnectBidirectionalReferences(final Map<String, String> nameGuidMap) throws Exception {

        // Verify that the Department.employees reference to the deleted employee
        // was disconnected.
        ITypedReferenceableInstance hrDept = metadataService.getEntityDefinition(nameGuidMap.get("hr"));
        List<ITypedReferenceableInstance> employees = (List<ITypedReferenceableInstance>) hrDept.get("employees");
        Assert.assertEquals(employees.size(), 4);
        String maxGuid = nameGuidMap.get("Max");
        for (ITypedReferenceableInstance employee : employees) {
            if (employee.getId()._getId().equals(maxGuid)) {
                assertEquals(employee.getId().getState(), Id.EntityState.DELETED);
            }
        }

        // Verify that the Manager.subordinates still references deleted employee
        ITypedReferenceableInstance jane = metadataService.getEntityDefinition(nameGuidMap.get("Jane"));
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        assertEquals(subordinates.size(), 2);
        for (ITypedReferenceableInstance subordinate : subordinates) {
            if (subordinate.getId()._getId().equals(maxGuid)) {
                assertEquals(subordinate.getId().getState(), Id.EntityState.DELETED);
            }
        }

        // Verify that max's Person.mentor unidirectional reference to john was disconnected.
        ITypedReferenceableInstance john = metadataService.getEntityDefinition(nameGuidMap.get("John"));
        Id mentor = (Id) john.get("mentor");
        assertEquals(mentor._getId(), maxGuid);
        assertEquals(mentor.getState(), Id.EntityState.DELETED);

    }

    @Override
    protected void assertTestDisconnectUnidirectionalArrayReferenceFromClassType(final List<AtlasObjectId> columns, final String columnGuid) throws AtlasBaseException {
        Assert.assertEquals(columns.size(), 3);
        for (AtlasObjectId column : columns) {
            AtlasEntity.AtlasEntityWithExtInfo columnEntity = entityStore.getById(column.getGuid());
            if (column.getGuid().equals(columnGuid)) {
                assertEquals(columnEntity.getEntity().getStatus(), AtlasEntity.Status.DELETED);
            } else {
                assertEquals(columnEntity.getEntity().getStatus(), AtlasEntity.Status.ACTIVE);
            }
        }
    }

    @Override
    protected void assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(final String structContainerGuid) throws Exception {
        // Verify that the unidirectional references from the struct and trait instances
        // to the deleted entities were not disconnected.
        ITypedReferenceableInstance structContainerConvertedEntity =
            metadataService.getEntityDefinition(structContainerGuid);
        ITypedStruct struct = (ITypedStruct) structContainerConvertedEntity.get("struct");
        assertNotNull(struct.get("target"));
        IStruct trait = structContainerConvertedEntity.getTrait("TestTrait");
        assertNotNull(trait);
        assertNotNull(trait.get("target"));

    }

    @Override
    protected void assertVerticesDeleted(List<AtlasVertex> vertices) {
        for (AtlasVertex vertex : vertices) {
            assertEquals(GraphHelper.getSingleValuedProperty(vertex, Constants.STATE_PROPERTY_KEY, String.class), Id.EntityState.DELETED.name());
        }
    }
}
