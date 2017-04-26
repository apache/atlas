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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.TestOnlyModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasEdgeLabel;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestUtils.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules = TestOnlyModule.class)
public abstract class AtlasDeleteHandlerV1Test {

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    AtlasTypeDefStore typeDefStore;

    AtlasEntityStore entityStore;

    @Inject
    MetadataService metadataService;

    private AtlasEntityType compositeMapOwnerType;

    private AtlasEntityType compositeMapValueType;

    private TypeSystem typeSystem = TypeSystem.getInstance();

    AtlasEntityChangeNotifier mockChangeNotifier = mock(AtlasEntityChangeNotifier.class);


    @BeforeClass
    public void setUp() throws Exception {
        metadataService = TestUtils.addSessionCleanupWrapper(metadataService);
        new GraphBackedSearchIndexer(typeRegistry);
        final AtlasTypesDef deptTypesDef = TestUtilsV2.defineDeptEmployeeTypes();
        typeDefStore.createTypesDef(deptTypesDef);

        final AtlasTypesDef hiveTypesDef = TestUtilsV2.defineHiveTypes();
        typeDefStore.createTypesDef(hiveTypesDef);

        // Define type for map value.
        AtlasEntityDef mapValueDef = AtlasTypeUtil.createClassTypeDef("CompositeMapValue", "CompositeMapValue" + "_description", "1.0",
            ImmutableSet.<String>of(),
            AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string")
        );

        // Define type with map where the value is a composite class reference to MapValue.
        AtlasEntityDef mapOwnerDef = AtlasTypeUtil.createClassTypeDef("CompositeMapOwner", "CompositeMapOwner_description",
            ImmutableSet.<String>of(),
            AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
            new AtlasStructDef.AtlasAttributeDef("map", "map<string,CompositeMapValue>", true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1, false, false,
                new ArrayList<AtlasStructDef.AtlasConstraintDef>() {{
                    add(new AtlasStructDef.AtlasConstraintDef(AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_OWNED_REF));
                }})
        );

        final AtlasTypesDef typesDef = AtlasTypeUtil.getTypesDef(ImmutableList.<AtlasEnumDef>of(),
            ImmutableList.<AtlasStructDef>of(),
            ImmutableList.<AtlasClassificationDef>of(),
            ImmutableList.of(mapValueDef, mapOwnerDef));

        typeDefStore.createTypesDef(typesDef);

        compositeMapOwnerType = typeRegistry.getEntityTypeByName("CompositeMapOwner");
        compositeMapValueType = typeRegistry.getEntityTypeByName("CompositeMapValue");
    }

    @BeforeTest
    public void init() throws Exception {
        DeleteHandlerV1 deleteHandler = getDeleteHandler(typeRegistry);
        entityStore = new AtlasEntityStoreV1(deleteHandler, typeRegistry, mockChangeNotifier);
        RequestContextV1.clear();

    }

    @AfterClass
    public void clear() {
        AtlasGraphProvider.cleanup();
    }

    abstract DeleteHandlerV1 getDeleteHandler(AtlasTypeRegistry typeRegistry);

    @Test
    public void testDeleteAndCreate() throws Exception {
        init();
        final AtlasEntity dbEntity = TestUtilsV2.createDBEntity();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(dbEntity), false);

        init();
        //delete entity should mark it as deleted
        EntityMutationResponse deleteResponse = entityStore.deleteById(response.getFirstEntityCreated().getGuid());
        AtlasEntityHeader dbEntityCreated = response.getFirstEntityCreated();
        assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).get(0).getGuid(), dbEntityCreated.getGuid());

        //get entity by unique attribute should throw EntityNotFoundException
        try {
            metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", (String) response.getFirstEntityCreated().getAttribute("name"));
            fail("Expected EntityNotFoundException");
        } catch(EntityNotFoundException e) {
            //expected
        }

        init();
        //Create the same entity again, should create new entity
        AtlasEntity newDBEntity = TestUtilsV2.createDBEntity((String) dbEntity.getAttribute(NAME));
        EntityMutationResponse newCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(newDBEntity), false);
        assertNotEquals(newCreationResponse.getFirstEntityCreated().getGuid(), response.getFirstEntityCreated().getGuid());

        //get by unique attribute should return the new entity
        ITypedReferenceableInstance instance = metadataService.getEntityDefinitionReference(TestUtils.DATABASE_TYPE, "name", (String) dbEntity.getAttribute("name"));
        assertEquals(instance.getId()._getId(), newCreationResponse.getFirstEntityCreated().getGuid());
    }

    @Test
    public void testDeleteReference() throws Exception {
        //Deleting column should update table
        final AtlasEntity dbEntity = TestUtilsV2.createDBEntity();

        init();
        EntityMutationResponse dbCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(dbEntity), false);

        final AtlasEntity tableEntity = TestUtilsV2.createTableEntity(dbEntity);
        final AtlasEntity columnEntity = TestUtilsV2.createColumnEntity(tableEntity);
        tableEntity.setAttribute(COLUMNS_ATTR_NAME, Arrays.asList(AtlasTypeUtil.getAtlasObjectId(columnEntity)));

        AtlasEntity.AtlasEntityWithExtInfo input = new AtlasEntity.AtlasEntityWithExtInfo(tableEntity);
        input.addReferredEntity(columnEntity);

        init();
        EntityMutationResponse tblCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(input), false);
        final AtlasEntityHeader columnCreated = tblCreationResponse.getFirstCreatedEntityByTypeName(COLUMN_TYPE);
        final AtlasEntityHeader tableCreated = tblCreationResponse.getFirstCreatedEntityByTypeName(TABLE_TYPE);

        init();
        EntityMutationResponse deletionResponse = entityStore.deleteById(columnCreated.getGuid());
        assertEquals(deletionResponse.getDeletedEntities().size(), 1);
        assertEquals(deletionResponse.getDeletedEntities().get(0).getGuid(), columnCreated.getGuid());
        assertEquals(deletionResponse.getUpdatedEntities().size(), 1);
        assertEquals(deletionResponse.getUpdatedEntities().get(0).getGuid(), tableCreated.getGuid());

        assertEntityDeleted(columnCreated.getGuid());

        assertColumnForTestDeleteReference(entityStore.getById(tableCreated.getGuid()));

        //Deleting table should update process
        AtlasEntity process = TestUtilsV2.createProcessEntity(null, Arrays.asList(AtlasTypeUtil.getAtlasObjectId(tableCreated)));
        init();
        final EntityMutationResponse processCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(process), false);

        init();
        entityStore.deleteById(tableCreated.getGuid());
        assertEntityDeleted(tableCreated.getGuid());

        assertTableForTestDeleteReference(tableCreated.getGuid());
        assertProcessForTestDeleteReference(processCreationResponse.getFirstEntityCreated());
    }

    @Test
    public void testDeleteEntities() throws Exception {
        // Create a table entity, with 3 composite column entities
        init();
        final AtlasEntity dbEntity = TestUtilsV2.createDBEntity();
        EntityMutationResponse dbCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(dbEntity), false);

        final AtlasEntity tableEntity = TestUtilsV2.createTableEntity(dbEntity);
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesInfo = new AtlasEntity.AtlasEntitiesWithExtInfo(tableEntity);

        final AtlasEntity columnEntity1 = TestUtilsV2.createColumnEntity(tableEntity);
        entitiesInfo.addReferredEntity(columnEntity1);
        final AtlasEntity columnEntity2 = TestUtilsV2.createColumnEntity(tableEntity);
        entitiesInfo.addReferredEntity(columnEntity2);
        final AtlasEntity columnEntity3 = TestUtilsV2.createColumnEntity(tableEntity);
        entitiesInfo.addReferredEntity(columnEntity3);

        tableEntity.setAttribute(COLUMNS_ATTR_NAME, Arrays.asList(AtlasTypeUtil.getAtlasObjectId(columnEntity1),
                                                                  AtlasTypeUtil.getAtlasObjectId(columnEntity2),
                                                                  AtlasTypeUtil.getAtlasObjectId(columnEntity3)));

        init();

        final EntityMutationResponse tblCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo), false);

        final AtlasEntityHeader column1Created = tblCreationResponse.getCreatedEntityByTypeNameAndAttribute(COLUMN_TYPE, NAME, (String) columnEntity1.getAttribute(NAME));
        final AtlasEntityHeader column2Created = tblCreationResponse.getCreatedEntityByTypeNameAndAttribute(COLUMN_TYPE, NAME, (String) columnEntity2.getAttribute(NAME));
        final AtlasEntityHeader column3Created = tblCreationResponse.getCreatedEntityByTypeNameAndAttribute(COLUMN_TYPE, NAME, (String) columnEntity3.getAttribute(NAME));

        // Retrieve the table entities from the Repository, to get their guids and the composite column guids.
        ITypedReferenceableInstance tableInstance = metadataService.getEntityDefinitionReference(TestUtils.TABLE_TYPE, NAME, (String) tableEntity.getAttribute(NAME));
        List<IReferenceableInstance> columns = (List<IReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME);

        //Delete column
        String colId = columns.get(0).getId()._getId();
        String tableId = tableInstance.getId()._getId();

        init();

        EntityMutationResponse deletionResponse = entityStore.deleteById(colId);
        assertEquals(deletionResponse.getDeletedEntities().size(), 1);
        assertEquals(deletionResponse.getDeletedEntities().get(0).getGuid(), colId);
        assertEquals(deletionResponse.getUpdatedEntities().size(), 1);
        assertEquals(deletionResponse.getUpdatedEntities().get(0).getGuid(), tableId);
        assertEntityDeleted(colId);

        final AtlasEntity.AtlasEntityWithExtInfo tableEntityCreated = entityStore.getById(tableId);
        assertDeletedColumn(tableEntityCreated);

        assertTestDisconnectUnidirectionalArrayReferenceFromClassType(
            (List<AtlasObjectId>) tableEntityCreated.getEntity().getAttribute(COLUMNS_ATTR_NAME), colId);

        //update by removing a column - col1
        final AtlasEntity tableEntity1 = TestUtilsV2.createTableEntity(dbEntity, (String) tableEntity.getAttribute(NAME));

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesInfo1 = new AtlasEntity.AtlasEntitiesWithExtInfo(tableEntity1);
        final AtlasEntity columnEntity3New = TestUtilsV2.createColumnEntity(tableEntity1, (String) column3Created.getAttribute(NAME));
        tableEntity1.setAttribute(COLUMNS_ATTR_NAME, Arrays.asList(AtlasTypeUtil.getAtlasObjectId(columnEntity3New)));
        entitiesInfo1.addReferredEntity(columnEntity3New);

        init();
        deletionResponse = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo1), false);

        assertEquals(deletionResponse.getDeletedEntities().size(), 1);
        assertEquals(deletionResponse.getDeletedEntities().get(0).getGuid(), column2Created.getGuid());
        assertEntityDeleted(colId);

        // Delete the table entities.  The deletion should cascade to their composite columns.
        tableInstance = metadataService.getEntityDefinitionReference(TestUtils.TABLE_TYPE, NAME, (String) tableEntity.getAttribute(NAME));

        init();
        EntityMutationResponse tblDeletionResponse = entityStore.deleteById(tableInstance.getId()._getId());
        assertEquals(tblDeletionResponse.getDeletedEntities().size(), 2);

        final AtlasEntityHeader tableDeleted = tblDeletionResponse.getFirstDeletedEntityByTypeName(TABLE_TYPE);
        final AtlasEntityHeader colDeleted = tblDeletionResponse.getFirstDeletedEntityByTypeName(COLUMN_TYPE);

        // Verify that deleteEntities() response has guids for tables and their composite columns.
        Assert.assertTrue(tableDeleted.getGuid().equals(tableInstance.getId()._getId()));
        Assert.assertTrue(colDeleted.getGuid().equals(column3Created.getGuid()));

        // Verify that tables and their composite columns have been deleted from the graph Repository.
        assertEntityDeleted(tableDeleted.getGuid());
        assertEntityDeleted(colDeleted.getGuid());

    }

    protected abstract void assertDeletedColumn(AtlasEntity.AtlasEntityWithExtInfo tableInstance) throws AtlasException, AtlasBaseException;

    protected abstract void assertTestDeleteEntities(AtlasEntity.AtlasEntityWithExtInfo tableInstance) throws Exception;

    protected abstract void assertTableForTestDeleteReference(String tableId) throws Exception;

    protected abstract void assertColumnForTestDeleteReference(AtlasEntity.AtlasEntityWithExtInfo tableInstance)
        throws AtlasBaseException;

    protected abstract void assertProcessForTestDeleteReference(AtlasEntityHeader processInstance) throws Exception;

    protected abstract void assertEntityDeleted(String id) throws Exception;

    String getFirstGuid(Map<String, AtlasEntity> entityMap) {
        return entityMap.keySet().iterator().next();
    }

    @Test
    public void testUpdateEntity_MultiplicityOneNonCompositeReference() throws Exception {
        AtlasEntity.AtlasEntitiesWithExtInfo hrDept = TestUtilsV2.createDeptEg2();
        init();

        RequestContextV1.clear();
        final EntityMutationResponse hrDeptCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(hrDept), false);
        final AtlasEntityHeader deptCreated = hrDeptCreationResponse.getFirstUpdatedEntityByTypeName(DEPARTMENT_TYPE);
        final AtlasEntityHeader maxEmployeeCreated = hrDeptCreationResponse.getCreatedEntityByTypeNameAndAttribute(TestUtilsV2.EMPLOYEE_TYPE, NAME, "Max");
        final AtlasEntityHeader johnEmployeeCreated = hrDeptCreationResponse.getUpdatedEntityByTypeNameAndAttribute(TestUtilsV2.EMPLOYEE_TYPE, NAME, "John");
        final AtlasEntityHeader janeEmployeeCreated = hrDeptCreationResponse.getCreatedEntityByTypeNameAndAttribute(TestUtilsV2.MANAGER_TYPE, NAME, "Jane");
        final AtlasEntityHeader juliusEmployeeCreated = hrDeptCreationResponse.getUpdatedEntityByTypeNameAndAttribute(TestUtilsV2.MANAGER_TYPE, NAME, "Julius");

        ITypedReferenceableInstance max = metadataService.getEntityDefinition(maxEmployeeCreated.getGuid());
        String maxGuid = max.getId()._getId();
        AtlasVertex vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long creationTimestamp = GraphHelper.getSingleValuedProperty(vertex, Constants.TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(creationTimestamp);

        Long modificationTimestampPreUpdate = GraphHelper.getSingleValuedProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(modificationTimestampPreUpdate);

        AtlasEntity maxEmployee = getEmployeeByName(hrDept, "Max");
        maxEmployee.setAttribute("mentor", AtlasTypeUtil.getAtlasObjectId(johnEmployeeCreated));
        maxEmployee.setAttribute("department", AtlasTypeUtil.getAtlasObjectId(deptCreated));
        maxEmployee.setAttribute("manager", AtlasTypeUtil.getAtlasObjectId(janeEmployeeCreated));

        init();
        EntityMutationResponse entityResult = entityStore.createOrUpdate(new AtlasEntityStream(maxEmployee), false);

        assertEquals(entityResult.getUpdatedEntities().size(), 1);
        assertTrue(extractGuids(entityResult.getUpdatedEntities()).contains(maxGuid));

        // Verify the update was applied correctly - john should now be max's mentor.
        max = metadataService.getEntityDefinition(maxGuid);
        ITypedReferenceableInstance refTarget = (ITypedReferenceableInstance) max.get("mentor");
        Assert.assertEquals(refTarget.getId()._getId(), johnEmployeeCreated.getGuid());

        // Verify modification timestamp was updated.
        vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long modificationTimestampPostUpdate = GraphHelper.getSingleValuedProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(modificationTimestampPostUpdate);
        Assert.assertTrue(creationTimestamp < modificationTimestampPostUpdate);

        // Update max's mentor reference to jane.
        maxEmployee.setAttribute("mentor", AtlasTypeUtil.getAtlasObjectId(janeEmployeeCreated));
        init();
        entityResult = entityStore.createOrUpdate(new AtlasEntityStream(maxEmployee), false);
        assertEquals(entityResult.getUpdatedEntities().size(), 1);
        assertTrue(extractGuids(entityResult.getUpdatedEntities()).contains(maxGuid));

        // Verify the update was applied correctly - jane should now be max's mentor.
        max = metadataService.getEntityDefinition(maxGuid);
        refTarget = (ITypedReferenceableInstance) max.get("mentor");
        Assert.assertEquals(refTarget.getId()._getId(), janeEmployeeCreated.getGuid());

        // Verify modification timestamp was updated.
        vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long modificationTimestampPost2ndUpdate = GraphHelper.getSingleValuedProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(modificationTimestampPost2ndUpdate);
        Assert.assertTrue(modificationTimestampPostUpdate < modificationTimestampPost2ndUpdate);

        ITypedReferenceableInstance julius = metadataService.getEntityDefinition(juliusEmployeeCreated.getGuid());
        Id juliusId = julius.getId();

        init();
        maxEmployee.setAttribute("manager", AtlasTypeUtil.getAtlasObjectId(juliusEmployeeCreated));
        entityResult = entityStore.createOrUpdate(new AtlasEntityStream(maxEmployee), false);
        assertEquals(entityResult.getUpdatedEntities().size(), 3);
        List<String> updatedGuids = extractGuids(entityResult.getUpdatedEntities());
        assertTrue(updatedGuids.contains(maxGuid));
        assertTrue(updatedGuids.contains(janeEmployeeCreated.getGuid()));
        // Should have updated julius to add max in subordinates list.
        assertTrue(updatedGuids.contains(juliusEmployeeCreated.getGuid()));

        // Verify the update was applied correctly - julius should now be max's manager and max should be julius' subordinate.
        max = metadataService.getEntityDefinition(maxGuid);
        refTarget = (ITypedReferenceableInstance) max.get("manager");
        Assert.assertEquals(refTarget.getId()._getId(), juliusId._getId());
        julius = metadataService.getEntityDefinition(juliusId._getId());
        Object value = julius.get("subordinates");
        Assert.assertTrue(value instanceof List);
        List<ITypedReferenceableInstance> refList = (List<ITypedReferenceableInstance>) value;
        Assert.assertEquals(refList.size(), 1);
        Assert.assertEquals(refList.get(0).getId()._getId(), maxGuid);

        assertTestUpdateEntity_MultiplicityOneNonCompositeReference(janeEmployeeCreated.getGuid());
    }

    private Map<String, String> getEmployeeNameGuidMap(final ITypedReferenceableInstance hrDept) throws AtlasException {
        Object refValue = hrDept.get("employees");
        Assert.assertTrue(refValue instanceof List);
        List<Object> employees = (List<Object>)refValue;
        Assert.assertEquals(employees.size(), 4);
        Map<String, String> nameGuidMap = new HashMap<String, String>() {{
            put("hr", hrDept.getId()._getId());
        }};

        for (Object listValue : employees) {
            Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
            ITypedReferenceableInstance employee = (ITypedReferenceableInstance) listValue;
            nameGuidMap.put((String)employee.get("name"), employee.getId()._getId());
        }
        return nameGuidMap;
    }


    private AtlasEntity getEmployeeByName(AtlasEntity.AtlasEntitiesWithExtInfo hrDept, String name) {
        for (AtlasEntity entity : hrDept.getEntities()) {
            if ( name.equals(entity.getAttribute(NAME))) {
                return entity;
            }
        }
        return null;
    }

    protected abstract void assertTestUpdateEntity_MultiplicityOneNonCompositeReference(String janeGuid) throws Exception;

    /**
     * Verify deleting an entity which is contained by another
     * entity through a bi-directional composite reference.
     *
     * @throws Exception
     */
    @Test
    public void testDisconnectBidirectionalReferences() throws Exception {
        AtlasEntity.AtlasEntitiesWithExtInfo hrDept = TestUtilsV2.createDeptEg2();
        init();
        final EntityMutationResponse hrDeptCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(hrDept), false);

        final AtlasEntityHeader deptCreated = hrDeptCreationResponse.getFirstCreatedEntityByTypeName(DEPARTMENT_TYPE);
        final AtlasEntityHeader maxEmployee = hrDeptCreationResponse.getCreatedEntityByTypeNameAndAttribute(TestUtilsV2.EMPLOYEE_TYPE, NAME, "Max");
        final AtlasEntityHeader johnEmployee = hrDeptCreationResponse.getCreatedEntityByTypeNameAndAttribute(TestUtilsV2.EMPLOYEE_TYPE, NAME, "John");
        final AtlasEntityHeader janeEmployee = hrDeptCreationResponse.getCreatedEntityByTypeNameAndAttribute(TestUtilsV2.MANAGER_TYPE, NAME, "Jane");
        final AtlasEntityHeader juliusEmployee = hrDeptCreationResponse.getCreatedEntityByTypeNameAndAttribute(TestUtilsV2.MANAGER_TYPE, NAME, "Julius");

        ITypedReferenceableInstance hrDeptInstance = metadataService.getEntityDefinition(deptCreated.getGuid());
        Map<String, String> nameGuidMap = getEmployeeNameGuidMap(hrDeptInstance);

        // Verify that Max is one of Jane's subordinates.
        ITypedReferenceableInstance jane = metadataService.getEntityDefinition(janeEmployee.getGuid());
        Object refValue = jane.get("subordinates");
        Assert.assertTrue(refValue instanceof List);
        List<Object> subordinates = (List<Object>)refValue;
        Assert.assertEquals(subordinates.size(), 2);
        List<String> subordinateIds = new ArrayList<>(2);
        for (Object listValue : subordinates) {
            Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
            ITypedReferenceableInstance employee = (ITypedReferenceableInstance) listValue;
            subordinateIds.add(employee.getId()._getId());
        }
        Assert.assertTrue(subordinateIds.contains(maxEmployee.getGuid()));

        init();
        EntityMutationResponse entityResult = entityStore.deleteById(maxEmployee.getGuid());
        ITypedReferenceableInstance john = metadataService.getEntityDefinitionReference(TestUtilsV2.EMPLOYEE_TYPE, NAME, "John");

        assertEquals(entityResult.getDeletedEntities().size(), 1);
        assertEquals(entityResult.getDeletedEntities().get(0).getGuid(), maxEmployee.getGuid());
        assertEquals(entityResult.getUpdatedEntities().size(), 3);

        assertEquals(extractGuids(entityResult.getUpdatedEntities()), Arrays.asList(janeEmployee.getGuid(), deptCreated.getGuid(), johnEmployee.getGuid()));
        assertEntityDeleted(maxEmployee.getGuid());

        assertMaxForTestDisconnectBidirectionalReferences(nameGuidMap);

        // Now delete jane - this should disconnect the manager reference from her
        // subordinate.
        init();
        entityResult = entityStore.deleteById(janeEmployee.getGuid());
        assertEquals(entityResult.getDeletedEntities().size(), 1);
        assertEquals(entityResult.getDeletedEntities().get(0).getGuid(), janeEmployee.getGuid());
        assertEquals(entityResult.getUpdatedEntities().size(), 2);
        assertEquals(extractGuids(entityResult.getUpdatedEntities()), Arrays.asList(deptCreated.getGuid(), johnEmployee.getGuid()));

        assertEntityDeleted(janeEmployee.getGuid());

        final AtlasEntity.AtlasEntityWithExtInfo johnUpdated = entityStore.getById(johnEmployee.getGuid());
        assertJohnForTestDisconnectBidirectionalReferences(johnUpdated, janeEmployee.getGuid());
    }

    protected List<String> extractGuids(final List<AtlasEntityHeader> updatedEntities) {
        List<String> guids = new ArrayList<>();
        for (AtlasEntityHeader header : updatedEntities ) {
            guids.add(header.getGuid());
        }
        return guids;
    }

    protected abstract void assertJohnForTestDisconnectBidirectionalReferences(AtlasEntity.AtlasEntityWithExtInfo john,
        String janeGuid) throws Exception;

    protected abstract void assertMaxForTestDisconnectBidirectionalReferences(Map<String, String> nameGuidMap)
        throws Exception;

    protected abstract void assertTestDisconnectUnidirectionalArrayReferenceFromClassType(
        List<AtlasObjectId> columns, String columnGuid) throws AtlasBaseException;

    /**
     * Verify deleting entities that are the target of a unidirectional class array reference
     * from a struct or trait instance.
     */
    @Test
    public void testDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes() throws Exception {
        // Define class types.
        AtlasStructDef.AtlasAttributeDef[] structTargetAttributes = new AtlasStructDef.AtlasAttributeDef[]{
            new AtlasStructDef.AtlasAttributeDef("attr1", "string",
                true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, false,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList())};

        AtlasEntityDef structTargetDef =
            new AtlasEntityDef("StructTarget", "StructTarget_description", "1.0",
                Arrays.asList(structTargetAttributes), Collections.<String>emptySet());


        AtlasStructDef.AtlasAttributeDef[] traitTargetAttributes = new AtlasStructDef.AtlasAttributeDef[]{
            new AtlasStructDef.AtlasAttributeDef("attr1", "string",
                true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, false,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList())};

        AtlasEntityDef traitTargetDef =
            new AtlasEntityDef("TraitTarget", "TraitTarget_description", "1.0",
                Arrays.asList(traitTargetAttributes), Collections.<String>emptySet());

        AtlasStructDef.AtlasAttributeDef[] structContainerAttributes = new AtlasStructDef.AtlasAttributeDef[]{
            new AtlasStructDef.AtlasAttributeDef("struct", "TestStruct",
                true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, false,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList())};

        AtlasEntityDef structContainerDef =
            new AtlasEntityDef("StructContainer", "StructContainer_description", "1.0",
                Arrays.asList(structContainerAttributes), Collections.<String>emptySet());

        // Define struct and trait types which have a unidirectional array reference
        // to a class type.
        AtlasStructDef.AtlasAttributeDef[] structDefAttributes = new AtlasStructDef.AtlasAttributeDef[] {
            new AtlasStructDef.AtlasAttributeDef("target", "array<StructTarget>",
            true,
            AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
            false, false,
            Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),

            new AtlasStructDef.AtlasAttributeDef("nestedStructs", "array<NestedStruct>",
            true,
            AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
            false, false,
            Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()) };

        AtlasStructDef structDef = new AtlasStructDef("TestStruct", "TestStruct_desc", "1.0", Arrays.asList(structDefAttributes));


        // Define struct and trait types which have a unidirectional array reference
        // to a class type.
        AtlasStructDef.AtlasAttributeDef[] nestedStructDefAttributes = new AtlasStructDef.AtlasAttributeDef[] {
            new AtlasStructDef.AtlasAttributeDef("attr1", "string",
                true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, false,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),

            new AtlasStructDef.AtlasAttributeDef("target", "array<TraitTarget>",
                true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, false,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()) };

        AtlasStructDef nestedStructDef = new AtlasStructDef("NestedStruct", "NestedStruct_desc", "1.0", Arrays.asList(nestedStructDefAttributes));

        AtlasStructDef.AtlasAttributeDef[] traitDefAttributes = new AtlasStructDef.AtlasAttributeDef[] {
            new AtlasStructDef.AtlasAttributeDef("target", "array<TraitTarget>",
                true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, false,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList())
        };

        AtlasClassificationDef traitDef = new AtlasClassificationDef("TestTrait", "TestTrait_desc", "1.0", Arrays.asList(traitDefAttributes));

        AtlasTypesDef typesDef = AtlasTypeUtil.getTypesDef(ImmutableList.<AtlasEnumDef>of(),
            ImmutableList.<AtlasStructDef>of(structDef, nestedStructDef),
            ImmutableList.<AtlasClassificationDef>of(traitDef),
            ImmutableList.<AtlasEntityDef>of(structTargetDef, traitTargetDef, structContainerDef));

        typeDefStore.createTypesDef(typesDef);

        // Create instances of class, struct, and trait types.
        final AtlasEntity structTargetEntity = new AtlasEntity("StructTarget");
        final AtlasEntity traitTargetEntity = new AtlasEntity("TraitTarget");
        final AtlasEntity structContainerEntity = new AtlasEntity("StructContainer");
        AtlasStruct structInstance = new AtlasStruct("TestStruct");
        AtlasStruct nestedStructInstance = new AtlasStruct("NestedStruct");
        Struct traitInstance = new Struct("TestTrait");
        structContainerEntity.setAttribute("struct", structInstance);
        structInstance.setAttribute("target", ImmutableList.of(AtlasTypeUtil.getAtlasObjectId(structTargetEntity)));
        structInstance.setAttribute("nestedStructs", ImmutableList.of(nestedStructInstance));

        AtlasEntity.AtlasEntitiesWithExtInfo structCreationObj = new AtlasEntity.AtlasEntitiesWithExtInfo();
        structCreationObj.addEntity(structContainerEntity);
        structCreationObj.addEntity(traitTargetEntity);
        structCreationObj.addReferredEntity(structTargetEntity);

        init();

        AtlasEntityStream entityStream = new AtlasEntityStream(structCreationObj);

        EntityMutationResponse response = entityStore.createOrUpdate(entityStream, false);
        Assert.assertEquals(response.getCreatedEntities().size(), 3);

        final List<String> structTarget = metadataService.getEntityList("StructTarget");
        Assert.assertEquals(structTarget.size(), 1);
        final String structTargetGuid = structTarget.get(0);

        final List<String> traitTarget = metadataService.getEntityList("TraitTarget");
        Assert.assertEquals(traitTarget.size(), 1);
        final String traitTargetGuid = traitTarget.get(0);

        final List<String> structContainerTarget = metadataService.getEntityList("StructContainer");
        Assert.assertEquals(structContainerTarget.size(), 1);
        String structContainerGuid = structContainerTarget.get(0);

        // Add TestTrait to StructContainer instance
        traitInstance.set("target", ImmutableList.of(new Id(traitTargetGuid, 0, "TraitTarget")));
        TraitType traitType = typeSystem.getDataType(TraitType.class, "TestTrait");
        ITypedStruct convertedTrait = traitType.convert(traitInstance, Multiplicity.REQUIRED);
        metadataService.addTrait(structContainerGuid, convertedTrait);

        // Verify that the unidirectional references from the struct and trait instances
        // are pointing at the target entities.
        final ITypedReferenceableInstance structContainerConvertedEntity = metadataService.getEntityDefinition(structContainerGuid);
        Object object = structContainerConvertedEntity.get("struct");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof ITypedStruct);
        ITypedStruct struct = (ITypedStruct) object;
        object = struct.get("target");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof List);
        List<ITypedReferenceableInstance> refList = (List<ITypedReferenceableInstance>)object;
        Assert.assertEquals(refList.size(), 1);
        Assert.assertEquals(refList.get(0).getId()._getId(), structTargetGuid);

        IStruct trait = structContainerConvertedEntity.getTrait("TestTrait");
        Assert.assertNotNull(trait);
        object = trait.get("target");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof List);
        refList = (List<ITypedReferenceableInstance>)object;
        Assert.assertEquals(refList.size(), 1);
        Assert.assertEquals(refList.get(0).getId()._getId(), traitTargetGuid);

        init();
        // Delete the entities that are targets of the struct and trait instances.
        EntityMutationResponse entityResult = entityStore.deleteByIds(new ArrayList<String>() {{
            add(structTargetGuid);
            add(traitTargetGuid);
        }});
        Assert.assertEquals(entityResult.getDeletedEntities().size(), 2);
        Assert.assertTrue(extractGuids(entityResult.getDeletedEntities()).containsAll(Arrays.asList(structTargetGuid, traitTargetGuid)));
        assertEntityDeleted(structTargetGuid);
        assertEntityDeleted(traitTargetGuid);

        assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(structContainerGuid);

        init();
        // Delete the entity which contains nested structs and has the TestTrait trait.
        entityResult = entityStore.deleteById(structContainerGuid);
        Assert.assertEquals(entityResult.getDeletedEntities().size(), 1);
        Assert.assertTrue(extractGuids(entityResult.getDeletedEntities()).contains(structContainerGuid));
        assertEntityDeleted(structContainerGuid);

        // Verify all TestStruct struct vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "TestStruct"));

        // Verify all NestedStruct struct vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "NestedStruct"));

        // Verify all TestTrait trait vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "TestTrait"));
    }


    /**
     * Verify deleting entities that are the target of class map references.
     */
    @Test
    public void testDisconnectMapReferenceFromClassType() throws Exception {
        // Define type for map value.
        AtlasStructDef.AtlasAttributeDef[] mapValueAttributes = new AtlasStructDef.AtlasAttributeDef[]{
            new AtlasStructDef.AtlasAttributeDef("biMapOwner", "MapOwner",
                true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, false,
                new ArrayList<AtlasStructDef.AtlasConstraintDef>() {{
                    add(new AtlasStructDef.AtlasConstraintDef(
                        AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_INVERSE_REF, new HashMap<String, Object>() {{
                        put(AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ATTRIBUTE, "biMap");
                    }}));
                }})};

        AtlasEntityDef mapValueContainerDef =
            new AtlasEntityDef("MapValue", "MapValue_desc", "1.0",
                Arrays.asList(mapValueAttributes), Collections.<String>emptySet());

        // Define type with unidirectional and bidirectional map references,
        // where the map value is a class reference to MapValue.

        AtlasStructDef.AtlasAttributeDef[] mapOwnerAttributes = new AtlasStructDef.AtlasAttributeDef[]{
            new AtlasStructDef.AtlasAttributeDef("map", "map<string,MapValue>",
                true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, false,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
            new AtlasStructDef.AtlasAttributeDef("biMap", "map<string,MapValue>",
                true,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, false,
                new ArrayList<AtlasStructDef.AtlasConstraintDef>() {{
                    add(new AtlasStructDef.AtlasConstraintDef(
                        AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_INVERSE_REF, new HashMap<String, Object>() {{
                        put(AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ATTRIBUTE, "biMapOwner");
                    }}));
                }})};

        AtlasEntityDef mapOwnerContainerDef =
            new AtlasEntityDef("MapOwner", "MapOwner_desc", "1.0",
                Arrays.asList(mapOwnerAttributes), Collections.<String>emptySet());

        AtlasTypesDef typesDef = AtlasTypeUtil.getTypesDef(ImmutableList.<AtlasEnumDef>of(),
            ImmutableList.<AtlasStructDef>of(),
            ImmutableList.<AtlasClassificationDef>of(),
            ImmutableList.<AtlasEntityDef>of(mapValueContainerDef, mapOwnerContainerDef));

        typeDefStore.createTypesDef(typesDef);

        // Create instances of MapOwner and MapValue.
        // Set MapOwner.map and MapOwner.biMap with one entry that references MapValue instance.
        AtlasEntity mapOwnerInstance = new AtlasEntity("MapOwner");
        AtlasEntity mapValueInstance = new AtlasEntity("MapValue");

        mapOwnerInstance.setAttribute("map", Collections.singletonMap("value1", AtlasTypeUtil.getAtlasObjectId(mapValueInstance)));
        mapOwnerInstance.setAttribute("biMap", Collections.singletonMap("value1", AtlasTypeUtil.getAtlasObjectId(mapValueInstance)));
        // Set biMapOwner reverse reference on MapValue.
        mapValueInstance.setAttribute("biMapOwner", AtlasTypeUtil.getAtlasObjectId(mapOwnerInstance));

        AtlasEntity.AtlasEntitiesWithExtInfo entities = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entities.addReferredEntity(mapValueInstance);
        entities.addEntity(mapOwnerInstance);

        final EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entities), false);
        Assert.assertEquals(response.getCreatedEntities().size(), 2);
        final List<AtlasEntityHeader> mapOwnerCreated = response.getCreatedEntitiesByTypeName("MapOwner");
        AtlasEntity.AtlasEntityWithExtInfo mapOwnerEntity = entityStore.getById(mapOwnerCreated.get(0).getGuid());

        String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(typeRegistry.getEntityTypeByName("MapOwner"), "map");
        String mapEntryLabel = edgeLabel + "." + "value1";
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(mapEntryLabel);

        // Verify MapOwner.map attribute has expected value.
        String mapValueGuid = null;
        AtlasVertex mapOwnerVertex = null;
        for (String mapAttrName : Arrays.asList("map", "biMap")) {
            Object object = mapOwnerEntity.getEntity().getAttribute(mapAttrName);
            Assert.assertNotNull(object);
            Assert.assertTrue(object instanceof Map);
            Map<String, AtlasObjectId> map = (Map<String, AtlasObjectId>)object;
            Assert.assertEquals(map.size(), 1);
            AtlasObjectId value1Id = map.get("value1");
            Assert.assertNotNull(value1Id);
            mapValueGuid = value1Id.getGuid();
            mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerEntity.getEntity().getGuid());
            object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey(), Object.class);
            Assert.assertNotNull(object);
        }

        // Delete the map value instance.
        // This should disconnect the references from the map owner instance.
        entityStore.deleteById(mapValueGuid);
        assertEntityDeleted(mapValueGuid);
        assertTestDisconnectMapReferenceFromClassType(mapOwnerEntity.getEntity().getGuid());
    }

    protected abstract void assertTestDisconnectMapReferenceFromClassType(String mapOwnerGuid) throws Exception;

    @Test
    public void testDeleteByUniqueAttribute() throws Exception {
        // Create a table entity, with 3 composite column entities
        init();
        final AtlasEntity dbEntity = TestUtilsV2.createDBEntity();
        EntityMutationResponse dbCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(dbEntity), false);

        final AtlasEntity tableEntity = TestUtilsV2.createTableEntity(dbEntity);
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesInfo = new AtlasEntity.AtlasEntitiesWithExtInfo(tableEntity);

        final AtlasEntity columnEntity1 = TestUtilsV2.createColumnEntity(tableEntity);
        entitiesInfo.addReferredEntity(columnEntity1);
        final AtlasEntity columnEntity2 = TestUtilsV2.createColumnEntity(tableEntity);
        entitiesInfo.addReferredEntity(columnEntity2);
        final AtlasEntity columnEntity3 = TestUtilsV2.createColumnEntity(tableEntity);
        entitiesInfo.addReferredEntity(columnEntity3);

        tableEntity.setAttribute(COLUMNS_ATTR_NAME, Arrays.asList(AtlasTypeUtil.getAtlasObjectId(columnEntity1),
                                                                  AtlasTypeUtil.getAtlasObjectId(columnEntity2),
                                                                  AtlasTypeUtil.getAtlasObjectId(columnEntity3)));

        init();

        final EntityMutationResponse tblCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo), false);

        final AtlasEntityHeader column1Created = tblCreationResponse.getCreatedEntityByTypeNameAndAttribute(COLUMN_TYPE, NAME, (String) columnEntity1.getAttribute(NAME));
        final AtlasEntityHeader column2Created = tblCreationResponse.getCreatedEntityByTypeNameAndAttribute(COLUMN_TYPE, NAME, (String) columnEntity2.getAttribute(NAME));
        final AtlasEntityHeader column3Created = tblCreationResponse.getCreatedEntityByTypeNameAndAttribute(COLUMN_TYPE, NAME, (String) columnEntity3.getAttribute(NAME));

        // Retrieve the table entities from the Repository, to get their guids and the composite column guids.
        ITypedReferenceableInstance tableInstance = metadataService.getEntityDefinitionReference(TestUtils.TABLE_TYPE, NAME, (String) tableEntity.getAttribute(NAME));
        List<IReferenceableInstance> columns = (List<IReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME);

        //Delete column
        String colId = columns.get(0).getId()._getId();
        String tableId = tableInstance.getId()._getId();

        init();

        Map<String, Object> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put(NAME, column1Created.getAttribute(NAME));

        AtlasEntityType columnType = typeRegistry.getEntityTypeByName(COLUMN_TYPE);
        EntityMutationResponse deletionResponse = entityStore.deleteByUniqueAttributes(columnType, uniqueAttrs);
        assertEquals(deletionResponse.getDeletedEntities().size(), 1);
        assertEquals(deletionResponse.getDeletedEntities().get(0).getGuid(), colId);
        assertEquals(deletionResponse.getUpdatedEntities().size(), 1);
        assertEquals(deletionResponse.getUpdatedEntities().get(0).getGuid(), tableId);
        assertEntityDeleted(colId);
    }

    @Test
    public void testDeleteEntitiesWithCompositeMapReference() throws Exception {
        // Create instances of MapOwner and MapValue.
        // Set MapOwner.map with one entry that references MapValue instance.
        AtlasEntity.AtlasEntityWithExtInfo entityDefinition = createMapOwnerAndValueEntities();
        String mapOwnerGuid = entityDefinition.getEntity().getGuid();

        // Verify MapOwner.map attribute has expected value.
        AtlasEntity.AtlasEntityWithExtInfo mapOwnerInstance = entityStore.getById(mapOwnerGuid);
        Object object = mapOwnerInstance.getEntity().getAttribute("map");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof Map);
        Map<String, AtlasObjectId> map = (Map<String, AtlasObjectId>)object;
        Assert.assertEquals(map.size(), 1);
        AtlasObjectId mapValueInstance = map.get("value1");
        Assert.assertNotNull(mapValueInstance);
        String mapValueGuid = mapValueInstance.getGuid();
        String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(compositeMapOwnerType, "map");
        String mapEntryLabel = edgeLabel + "." + "value1";
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(mapEntryLabel);
        AtlasVertex mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey(), Object.class);
        Assert.assertNotNull(object);

        RequestContextV1.clear();
        List<AtlasEntityHeader> deletedEntities = entityStore.deleteById(mapOwnerGuid).getDeletedEntities();
        Assert.assertEquals(deletedEntities.size(), 2);
        Assert.assertTrue(extractGuids(deletedEntities).contains(mapOwnerGuid));
        Assert.assertTrue(extractGuids(deletedEntities).contains(mapValueGuid));

        assertEntityDeleted(mapOwnerGuid);
        assertEntityDeleted(mapValueGuid);
    }

    @Test
    public void testDeleteTargetOfRequiredMapReference() throws Exception {
        // Define type for map value.
        AtlasEntityDef mapValueDef =
            new AtlasEntityDef("RequiredMapValue", "RequiredMapValue_description", "1.0",
                Collections.<AtlasStructDef.AtlasAttributeDef>emptyList(), Collections.<String>emptySet());

        AtlasStructDef.AtlasAttributeDef[] mapOwnerAttributes = new AtlasStructDef.AtlasAttributeDef[]{
            new AtlasStructDef.AtlasAttributeDef("map", "map<string,RequiredMapValue>",
                false,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                false, false,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList())
                };

        AtlasEntityDef mapOwnerDef =
            new AtlasEntityDef("RequiredMapOwner", "RequiredMapOwner_description", "1.0",
                Arrays.asList(mapOwnerAttributes), Collections.<String>emptySet());

        AtlasTypesDef typesDef = AtlasTypeUtil.getTypesDef(ImmutableList.<AtlasEnumDef>of(),
            ImmutableList.<AtlasStructDef>of(),
            ImmutableList.<AtlasClassificationDef>of(),
            ImmutableList.<AtlasEntityDef>of(mapValueDef, mapOwnerDef));

        typeDefStore.createTypesDef(typesDef);

        AtlasEntityType mapOwnerType = typeRegistry.getEntityTypeByName("RequiredMapOwner");
        AtlasEntityType mapValueType = typeRegistry.getEntityTypeByName("RequiredMapValue");

        // Create instances of RequiredMapOwner and RequiredMapValue.
        // Set RequiredMapOwner.map with one entry that references RequiredMapValue instance.
        AtlasEntity mapOwnerInstance = new AtlasEntity(mapOwnerType.getTypeName());
        AtlasEntity mapValueInstance = new AtlasEntity(mapValueType.getTypeName());
        mapOwnerInstance.setAttribute("map", Collections.singletonMap("value1", AtlasTypeUtil.getAtlasObjectId(mapValueInstance)));

        AtlasEntity.AtlasEntitiesWithExtInfo entities = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entities.addReferredEntity(mapValueInstance);
        entities.addEntity(mapOwnerInstance);

        List<AtlasEntityHeader> createEntitiesResult = entityStore.createOrUpdate(new AtlasEntityStream(entities), false).getCreatedEntities();
        Assert.assertEquals(createEntitiesResult.size(), 2);
        List<String> guids = metadataService.getEntityList("RequiredMapOwner");
        Assert.assertEquals(guids.size(), 1);
        String mapOwnerGuid = guids.get(0);
        guids = metadataService.getEntityList("RequiredMapValue");
        Assert.assertEquals(guids.size(), 1);
        String mapValueGuid = guids.get(0);

        // Verify MapOwner.map attribute has expected value.
        final AtlasEntity.AtlasEntityWithExtInfo mapOwnerInstance1 = entityStore.getById(mapOwnerGuid);
        Object object = mapOwnerInstance1.getEntity().getAttribute("map");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof Map);
        Map<String, AtlasObjectId> map = (Map<String, AtlasObjectId>)object;
        Assert.assertEquals(map.size(), 1);
        AtlasObjectId mapValueInstance1 = map.get("value1");
        Assert.assertNotNull(mapValueInstance1);
        Assert.assertEquals(mapValueInstance1.getGuid(), mapValueGuid);
        String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(mapOwnerType, "map");
        String mapEntryLabel = edgeLabel + "." + "value1";
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(mapEntryLabel);
        AtlasVertex mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey(), Object.class);
        Assert.assertNotNull(object);

        // Verify deleting the target of required map attribute throws a AtlasBaseException.
        try {
            entityStore.deleteById(mapValueGuid);
            Assert.fail(AtlasBaseException.class.getSimpleName() + " was expected but none thrown.");
        }
        catch (Exception e) {
            verifyExceptionThrown(e, AtlasBaseException.class);
        }
    }

    @Test
    public void testLowerBoundsIgnoredWhenDeletingCompositeEntitesOwnedByMap() throws Exception {
        // Define MapValueReferencer type with required reference to CompositeMapValue.
        AtlasStructDef.AtlasAttributeDef[] mapValueAttributes = new AtlasStructDef.AtlasAttributeDef[]{
            new AtlasStructDef.AtlasAttributeDef("refToMapValue", "CompositeMapValue",
                false,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                false, false,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList())
        };

        AtlasEntityDef mapValueDef =
            new AtlasEntityDef("MapValueReferencer", "RequiredMapValue_description", "1.0",
                Arrays.asList(mapValueAttributes), Collections.<String>emptySet());


        AtlasStructDef.AtlasAttributeDef[] mapContainerAttributes = new AtlasStructDef.AtlasAttributeDef[]{
            new AtlasStructDef.AtlasAttributeDef("requiredMap", "map<string,MapValueReferencer>",
                false,
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                false, false,
                new ArrayList<AtlasStructDef.AtlasConstraintDef>() {{
                    add(new AtlasStructDef.AtlasConstraintDef(AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_OWNED_REF));
                }})
        };

        AtlasEntityDef mapContainerDef =
            new AtlasEntityDef("MapValueReferencerContainer", "MapValueReferencerContainer_description", "1.0",
                Arrays.asList(mapContainerAttributes), Collections.<String>emptySet());


        AtlasTypesDef typesDef = AtlasTypeUtil.getTypesDef(ImmutableList.<AtlasEnumDef>of(),
            ImmutableList.<AtlasStructDef>of(),
            ImmutableList.<AtlasClassificationDef>of(),
            ImmutableList.<AtlasEntityDef>of(mapValueDef, mapContainerDef));

        typeDefStore.createTypesDef(typesDef);

        // Create instances of CompositeMapOwner and CompositeMapValue.
        // Set MapOwner.map with one entry that references MapValue instance.
        AtlasEntity.AtlasEntityWithExtInfo entityDefinition = createMapOwnerAndValueEntities();
        String mapOwnerGuid = entityDefinition.getEntity().getGuid();

        // Verify MapOwner.map attribute has expected value.
        ITypedReferenceableInstance mapOwnerInstance = metadataService.getEntityDefinition(mapOwnerGuid);
        Object object = mapOwnerInstance.get("map");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof Map);
        Map<String, ITypedReferenceableInstance> map = (Map<String, ITypedReferenceableInstance>)object;
        Assert.assertEquals(map.size(), 1);
        ITypedReferenceableInstance mapValueInstance = map.get("value1");
        Assert.assertNotNull(mapValueInstance);
        String mapValueGuid = mapValueInstance.getId()._getId();

        // Create instance of MapValueReferencerContainer
        RequestContextV1.clear();
        AtlasEntity mapValueReferencer = new AtlasEntity(mapValueDef.getName());
        mapValueReferencer.setAttribute("refToMapValue", new AtlasObjectId(mapValueInstance.getId()._getId(), mapValueInstance.getTypeName()));
        AtlasEntity.AtlasEntitiesWithExtInfo entities = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entities.addEntity(mapValueReferencer);

        List<AtlasEntityHeader> createEntitiesResult = entityStore.createOrUpdate(new AtlasEntityStream(entities), false).getCreatedEntities();
        Assert.assertEquals(createEntitiesResult.size(), 1);

        // Create instance of MapValueReferencer, and update mapValueReferencerContainer
        // to reference it.
        AtlasEntity mapValueReferenceContainer = new AtlasEntity(mapContainerDef.getName());
        entities = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entities.addEntity(mapValueReferenceContainer);
        entities.addReferredEntity(mapValueReferencer);
        mapValueReferenceContainer.setAttribute("requiredMap", Collections.singletonMap("value1", AtlasTypeUtil.getAtlasObjectId(mapValueReferencer)));


        RequestContextV1.clear();
        EntityMutationResponse updateEntitiesResult = entityStore.createOrUpdate(new AtlasEntityStream(entities), false);

        String mapValueReferencerContainerGuid = updateEntitiesResult.getCreatedEntitiesByTypeName("MapValueReferencerContainer").get(0).getGuid();
        String mapValueReferencerGuid = updateEntitiesResult.getUpdatedEntitiesByTypeName("MapValueReferencer").get(0).getGuid();

        Assert.assertEquals(updateEntitiesResult.getCreatedEntities().size(), 1);
        Assert.assertEquals(updateEntitiesResult.getUpdatedEntities().size(), 1);
        Assert.assertEquals(updateEntitiesResult.getUpdatedEntities().get(0).getGuid(), mapValueReferencerGuid);


        // Delete map owner and map referencer container.  A total of 4 entities should be deleted,
        // including the composite entities.  The lower bound constraint on MapValueReferencer.refToMapValue
        // should not be enforced on the composite MapValueReferencer since it is being deleted.
        EntityMutationResponse deleteEntitiesResult = entityStore.deleteByIds(Arrays.asList(mapOwnerGuid, mapValueReferencerContainerGuid));
        Assert.assertEquals(deleteEntitiesResult.getDeletedEntities().size(), 4);
        Assert.assertTrue(extractGuids(deleteEntitiesResult.getDeletedEntities()).containsAll(
            Arrays.asList(mapOwnerGuid, mapValueGuid, mapValueReferencerContainerGuid, mapValueReferencerGuid)));
    }

    private AtlasEntity.AtlasEntityWithExtInfo createMapOwnerAndValueEntities()
        throws AtlasException, AtlasBaseException {

        final AtlasEntity mapOwnerInstance = new AtlasEntity(compositeMapOwnerType.getTypeName());
        mapOwnerInstance.setAttribute(NAME, TestUtils.randomString());
        AtlasEntity mapValueInstance = new AtlasEntity(compositeMapValueType.getTypeName());
        mapValueInstance.setAttribute(NAME, TestUtils.randomString());
        mapOwnerInstance.setAttribute("map", Collections.singletonMap("value1", AtlasTypeUtil.getAtlasObjectId(mapValueInstance)));

        AtlasEntity.AtlasEntitiesWithExtInfo entities = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entities.addReferredEntity(mapValueInstance);
        entities.addEntity(mapOwnerInstance);

        List<AtlasEntityHeader> createEntitiesResult = entityStore.createOrUpdate(new AtlasEntityStream(entities), false).getCreatedEntities();
        Assert.assertEquals(createEntitiesResult.size(), 2);
        AtlasEntity.AtlasEntityWithExtInfo entityDefinition = entityStore.getByUniqueAttributes(compositeMapOwnerType,
            new HashMap<String, Object>() {{
                put(NAME, mapOwnerInstance.getAttribute(NAME));
            }});
        return entityDefinition;
    }


    protected abstract void assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(
        String structContainerGuid) throws Exception;

    protected abstract void assertVerticesDeleted(List<AtlasVertex> vertices);

    protected List<AtlasVertex> getVertices(String propertyName, Object value) {
        AtlasGraph graph = TestUtils.getGraph();
        Iterable<AtlasVertex> vertices = graph.getVertices(propertyName, value);
        List<AtlasVertex> list = new ArrayList<>();
        for (AtlasVertex vertex : vertices) {
            list.add(vertex);
        }
        return list;
    }

    /**
     * Search exception cause chain for specified exception.
     *
     * @param thrown root of thrown exception chain
     * @param expected  class of expected exception
     */
    private void verifyExceptionThrown(Exception thrown, Class expected) {

        boolean exceptionFound = false;
        Throwable cause = thrown;
        while (cause != null) {
            if (expected.isInstance(cause)) {
                // good
                exceptionFound = true;
                break;
            }
            else {
                cause = cause.getCause();
            }
        }
        if (!exceptionFound) {
            Assert.fail(expected.getSimpleName() + " was expected but not thrown", thrown);
        }
    }

}
