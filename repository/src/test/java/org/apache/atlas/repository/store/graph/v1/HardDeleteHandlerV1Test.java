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
import org.apache.atlas.AtlasException;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.store.DeleteType;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.testng.Assert;
import org.testng.annotations.Guice;

import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestUtils.COLUMNS_ATTR_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNotNull;

@Guice(modules = TestModules.TestOnlyModule.class)
public class HardDeleteHandlerV1Test extends AtlasDeleteHandlerV1Test {
    public HardDeleteHandlerV1Test() {
        super(DeleteType.HARD);
    }

    @Override
    protected void assertTableForTestDeleteReference(String tableId) {
        //entity is deleted. So, no assertions
    }

    @Override
    protected void assertColumnForTestDeleteReference(final AtlasEntity.AtlasEntityWithExtInfo tableInstance) throws AtlasBaseException {
        List<AtlasObjectId> columns = (List<AtlasObjectId>) tableInstance.getEntity().getAttribute(COLUMNS_ATTR_NAME);
        assertNull(columns);
    }

    @Override
    protected void assertProcessForTestDeleteReference(final AtlasEntityHeader processInstance) throws Exception {
        //assert that outputs is empty
        ITypedReferenceableInstance newProcess =
            metadataService.getEntityDefinition(processInstance.getGuid());
        assertNull(newProcess.get(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS));
    }

    @Override
    protected void assertEntityDeleted(String id) throws Exception {
        try {
            entityStore.getById(id);
            fail("Expected EntityNotFoundException");
        } catch (AtlasBaseException e) {
            // expected
        }
    }

    @Override
    protected void assertDeletedColumn(final AtlasEntity.AtlasEntityWithExtInfo tableInstance) throws AtlasException {
        final List<AtlasObjectId> columns = (List<AtlasObjectId>) tableInstance.getEntity().getAttribute(COLUMNS_ATTR_NAME);
        Assert.assertEquals(columns.size(), 2);
    }

    @Override
    protected void assertTestDeleteEntities(AtlasEntity.AtlasEntityWithExtInfo tableInstance) {
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
        ITypedReferenceableInstance jane = metadataService.getEntityDefinition(janeGuid);
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        Assert.assertEquals(subordinates.size(), 1);
    }

    @Override
    protected void assertJohnForTestDisconnectBidirectionalReferences(final AtlasEntity.AtlasEntityWithExtInfo john, final String janeGuid) throws Exception {
        assertNull(john.getEntity().getAttribute("manager"));
    }

    @Override
    protected void assertMaxForTestDisconnectBidirectionalReferences(Map<String, String> nameGuidMap)
        throws Exception {
        // Verify that the Department.employees reference to the deleted employee
        // was disconnected.
        ITypedReferenceableInstance hrDept = metadataService.getEntityDefinition(nameGuidMap.get("hr"));
        List<ITypedReferenceableInstance> employees = (List<ITypedReferenceableInstance>) hrDept.get("employees");
        Assert.assertEquals(employees.size(), 3);
        String maxGuid = nameGuidMap.get("Max");
        for (ITypedReferenceableInstance employee : employees) {
            Assert.assertNotEquals(employee.getId()._getId(), maxGuid);
        }

        // Verify that the Manager.subordinates reference to the deleted employee
        // Max was disconnected.
        ITypedReferenceableInstance jane = metadataService.getEntityDefinition(nameGuidMap.get("Jane"));
        List<ITypedReferenceableInstance> subordinates = (List<ITypedReferenceableInstance>) jane.get("subordinates");
        assertEquals(subordinates.size(), 1);

        // Verify that max's Person.mentor unidirectional reference to john was disconnected.
        ITypedReferenceableInstance john = metadataService.getEntityDefinition(nameGuidMap.get("John"));
        assertNull(john.get("mentor"));
    }

    @Override
    protected void assertTestDisconnectUnidirectionalArrayReferenceFromClassType(
        List<AtlasObjectId> columns, String columnGuid) {
        assertEquals(columns.size(), 2);
        for (AtlasObjectId column : columns) {
            assertFalse(column.getGuid().equals(columnGuid));
        }
    }

    protected void assertTestDisconnectMapReferenceFromClassType(final String mapOwnerGuid) throws Exception {
        // Verify map references from mapOwner were disconnected.
        AtlasEntity.AtlasEntityWithExtInfo mapOwnerInstance = entityStore.getById(mapOwnerGuid);
        Map<String, AtlasObjectId> map =
            (Map<String, AtlasObjectId>) mapOwnerInstance.getEntity().getAttribute("map");
        Assert.assertNull(map);
        Map<String, AtlasObjectId> biMap =
            (Map<String, AtlasObjectId>) mapOwnerInstance.getEntity().getAttribute("biMap");
        Assert.assertNull(biMap);

        AtlasVertex mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        Object object = mapOwnerVertex.getProperty("MapOwner.map.value1", String.class);
        assertNull(object);
        object = mapOwnerVertex.getProperty("MapOwner.biMap.value1", String.class);
        assertNull(object);
    }

    @Override
    protected void assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(String structContainerGuid)
        throws Exception {
        // Verify that the unidirectional references from the struct and trait instances
        // to the deleted entities were disconnected.
        ITypedReferenceableInstance structContainerConvertedEntity =
            metadataService.getEntityDefinition(structContainerGuid);
        ITypedStruct struct = (ITypedStruct) structContainerConvertedEntity.get("struct");
        assertNull(struct.get("target"));
        IStruct trait = structContainerConvertedEntity.getTrait("TestTrait");
        assertNotNull(trait);
        assertNull(trait.get("target"));
    }
}
