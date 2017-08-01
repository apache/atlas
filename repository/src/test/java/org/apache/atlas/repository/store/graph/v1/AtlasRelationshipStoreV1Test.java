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
package org.apache.atlas.repository.store.graph.v1;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestRelationshipUtilsV2.EMPLOYEE_TYPE;
import static org.apache.atlas.TestRelationshipUtilsV2.getDepartmentEmployeeInstances;
import static org.apache.atlas.TestRelationshipUtilsV2.getDepartmentEmployeeTypes;
import static org.apache.atlas.TestRelationshipUtilsV2.getInverseReferenceTestTypes;
import static org.apache.atlas.TestUtils.NAME;
import static org.apache.atlas.type.AtlasTypeUtil.getAtlasObjectId;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public abstract class AtlasRelationshipStoreV1Test {

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    AtlasTypeDefStore typeDefStore;

    @Inject
    DeleteHandlerV1   deleteHandler;

    @Inject
    EntityGraphMapper graphMapper;

    AtlasEntityStore          entityStore;
    AtlasRelationshipStore    relationshipStore;
    AtlasEntityChangeNotifier mockChangeNotifier = mock(AtlasEntityChangeNotifier.class);

    protected Map<String, AtlasObjectId> employeeNameIdMap = new HashMap<>();

    @BeforeClass
    public void setUp() throws Exception {
        new GraphBackedSearchIndexer(typeRegistry);

        // create employee relationship types
        AtlasTypesDef employeeTypes = getDepartmentEmployeeTypes();
        typeDefStore.createTypesDef(employeeTypes);

        AtlasEntitiesWithExtInfo employeeInstances = getDepartmentEmployeeInstances();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(employeeInstances), false);

        for (AtlasEntityHeader entityHeader : response.getCreatedEntities()) {
            employeeNameIdMap.put((String) entityHeader.getAttribute(NAME), getAtlasObjectId(entityHeader));
        }

        init();
        AtlasTypesDef typesDef = getInverseReferenceTestTypes();

        AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(typesDef, typeRegistry);

        if (!typesToCreate.isEmpty()) {
            typeDefStore.createTypesDef(typesToCreate);
        }
    }

    @BeforeTest
    public void init() throws Exception {
        entityStore       = new AtlasEntityStoreV1(deleteHandler, typeRegistry, mockChangeNotifier, graphMapper);
        relationshipStore = new AtlasRelationshipStoreV1(typeRegistry);

        RequestContextV1.clear();
        RequestContextV1.get().setUser(TestUtilsV2.TEST_USER);
    }

    @AfterClass
    public void clear() {
        AtlasGraphProvider.cleanup();
    }

    @Test
    public void testDepartmentEmployeeEntitiesUsingRelationship() throws Exception  {
        AtlasObjectId hrId     = employeeNameIdMap.get("hr");
        AtlasObjectId maxId    = employeeNameIdMap.get("Max");
        AtlasObjectId johnId   = employeeNameIdMap.get("John");
        AtlasObjectId juliusId = employeeNameIdMap.get("Julius");
        AtlasObjectId janeId   = employeeNameIdMap.get("Jane");

        AtlasEntity hrDept = getEntityFromStore(hrId.getGuid());
        AtlasEntity max    = getEntityFromStore(maxId.getGuid());
        AtlasEntity john   = getEntityFromStore(johnId.getGuid());
        AtlasEntity julius = getEntityFromStore(juliusId.getGuid());
        AtlasEntity jane   = getEntityFromStore(janeId.getGuid());

        // Department relationship attributes
        List<AtlasObjectId> deptEmployees = toAtlasObjectIds(hrDept.getRelationshipAttribute("employees"));
        assertNotNull(deptEmployees);
        assertEquals(deptEmployees.size(), 4);
        assertObjectIdsContains(deptEmployees, maxId);
        assertObjectIdsContains(deptEmployees, johnId);
        assertObjectIdsContains(deptEmployees, juliusId);
        assertObjectIdsContains(deptEmployees, janeId);

        // Max employee validation
        AtlasObjectId maxDepartmentId = toAtlasObjectId(max.getRelationshipAttribute("department"));
        assertNotNull(maxDepartmentId);
        assertObjectIdEquals(maxDepartmentId, hrId);

        AtlasObjectId maxManagerId = toAtlasObjectId(max.getRelationshipAttribute("manager"));
        assertNotNull(maxManagerId);
        assertObjectIdEquals(maxManagerId, janeId);

        AtlasObjectId maxMentorId = toAtlasObjectId(max.getRelationshipAttribute("mentor"));
        assertNotNull(maxMentorId);
        assertObjectIdEquals(maxMentorId, juliusId);

        List<AtlasObjectId> maxMenteesId = toAtlasObjectIds(max.getRelationshipAttribute("mentees"));
        assertNotNull(maxMenteesId);
        assertEquals(maxMenteesId.size(), 1);
        assertObjectIdEquals(maxMenteesId.get(0), johnId);

        // John Employee validation
        AtlasObjectId johnDepartmentId = toAtlasObjectId(john.getRelationshipAttribute("department"));
        assertNotNull(johnDepartmentId);
        assertObjectIdEquals(johnDepartmentId, hrId);

        AtlasObjectId johnManagerId = toAtlasObjectId(john.getRelationshipAttribute("manager"));
        assertNotNull(johnManagerId);
        assertObjectIdEquals(johnManagerId, janeId);

        AtlasObjectId johnMentorId = toAtlasObjectId(john.getRelationshipAttribute("mentor"));
        assertNotNull(johnMentorId);
        assertObjectIdEquals(johnMentorId, maxId);

        List<AtlasObjectId> johnMenteesId = toAtlasObjectIds(john.getRelationshipAttribute("mentees"));
        assertEmpty(johnMenteesId);

        // Jane Manager validation
        AtlasObjectId janeDepartmentId = toAtlasObjectId(jane.getRelationshipAttribute("department"));
        assertNotNull(janeDepartmentId);
        assertObjectIdEquals(janeDepartmentId, hrId);

        AtlasObjectId janeManagerId = toAtlasObjectId(jane.getRelationshipAttribute("manager"));
        assertNull(janeManagerId);

        AtlasObjectId janeMentorId = toAtlasObjectId(jane.getRelationshipAttribute("mentor"));
        assertNull(janeMentorId);

        List<AtlasObjectId> janeMenteesId = toAtlasObjectIds(jane.getRelationshipAttribute("mentees"));
        assertEmpty(janeMenteesId);

        List<AtlasObjectId> janeSubordinateIds = toAtlasObjectIds(jane.getRelationshipAttribute("subordinates"));
        assertNotNull(janeSubordinateIds);
        assertEquals(janeSubordinateIds.size(), 2);
        assertObjectIdsContains(janeSubordinateIds, maxId);
        assertObjectIdsContains(janeSubordinateIds, johnId);

        // Julius Manager validation
        AtlasObjectId juliusDepartmentId = toAtlasObjectId(julius.getRelationshipAttribute("department"));
        assertNotNull(juliusDepartmentId);
        assertObjectIdEquals(juliusDepartmentId, hrId);

        AtlasObjectId juliusManagerId = toAtlasObjectId(julius.getRelationshipAttribute("manager"));
        assertNull(juliusManagerId);

        AtlasObjectId juliusMentorId = toAtlasObjectId(julius.getRelationshipAttribute("mentor"));
        assertNull(juliusMentorId);

        List<AtlasObjectId> juliusMenteesId = toAtlasObjectIds(julius.getRelationshipAttribute("mentees"));
        assertNotNull(juliusMenteesId);
        assertEquals(juliusMenteesId.size(), 1);
        assertObjectIdsContains(juliusMenteesId, maxId);

        List<AtlasObjectId> juliusSubordinateIds = toAtlasObjectIds(julius.getRelationshipAttribute("subordinates"));
        assertEmpty(juliusSubordinateIds);
    }

    @Test
    public void testRelationshipAttributeUpdate_NonComposite_OneToMany() throws Exception {
        AtlasObjectId maxId    = employeeNameIdMap.get("Max");
        AtlasObjectId juliusId = employeeNameIdMap.get("Julius");
        AtlasObjectId janeId   = employeeNameIdMap.get("Jane");

        // Change Max's Employee.manager reference to Julius and apply the change as a partial update.
        // This should also update Julius to add Max to the inverse Manager.subordinates reference.
        AtlasEntity maxEntityForUpdate = new AtlasEntity(EMPLOYEE_TYPE);
        maxEntityForUpdate.setRelationshipAttribute("manager", juliusId);

        AtlasEntityType        employeeType   = typeRegistry.getEntityTypeByName(EMPLOYEE_TYPE);
        Map<String, Object>    uniqAttributes = Collections.<String, Object>singletonMap("name", "Max");
        EntityMutationResponse updateResponse = entityStore.updateByUniqueAttributes(employeeType, uniqAttributes , new AtlasEntityWithExtInfo(maxEntityForUpdate));

        List<AtlasEntityHeader> partialUpdatedEntities = updateResponse.getPartialUpdatedEntities();
        assertEquals(partialUpdatedEntities.size(), 3);
        // 3 entities should have been updated:
        // * Max to change the Employee.manager reference
        // * Julius to add Max to Manager.subordinates
        // * Jane to remove Max from Manager.subordinates

        AtlasEntitiesWithExtInfo updatedEntities = entityStore.getByIds(ImmutableList.of(maxId.getGuid(), juliusId.getGuid(), janeId.getGuid()));

        // Max's manager updated as Julius
        AtlasEntity maxEntity = updatedEntities.getEntity(maxId.getGuid());
        verifyRelationshipAttributeValue(maxEntity, "manager", juliusId.getGuid());

        // Max added to the subordinate list of Julius
        AtlasEntity juliusEntity = updatedEntities.getEntity(juliusId.getGuid());
        verifyRelationshipAttributeList(juliusEntity, "subordinates", ImmutableList.of(maxId));

        // Max removed from the subordinate list of Julius
        AtlasEntity janeEntity = updatedEntities.getEntity(janeId.getGuid());

        // Jane's subordinates list includes John and Max for soft delete
        // Jane's subordinates list includes only John for hard delete
        verifyRelationshipAttributeUpdate_NonComposite_OneToMany(janeEntity);
    }

    @Test
    public void testRelationshipAttributeUpdate_NonComposite_ManyToOne() throws Exception {
        AtlasEntity a1 = new AtlasEntity("A");
        a1.setAttribute(NAME, "a1_name");

        AtlasEntity a2 = new AtlasEntity("A");
        a2.setAttribute(NAME, "a2_name");

        AtlasEntity a3 = new AtlasEntity("A");
        a3.setAttribute(NAME, "a3_name");

        AtlasEntity b = new AtlasEntity("B");
        b.setAttribute(NAME, "b_name");

        AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(a1);
        entitiesWithExtInfo.addEntity(a2);
        entitiesWithExtInfo.addEntity(a3);
        entitiesWithExtInfo.addEntity(b);
        entityStore.createOrUpdate(new AtlasEntityStream(entitiesWithExtInfo) , false);

        AtlasEntity bPartialUpdate = new AtlasEntity("B");
        bPartialUpdate.setRelationshipAttribute("manyA", ImmutableList.of(getAtlasObjectId(a1), getAtlasObjectId(a2)));

        init();
        EntityMutationResponse response = entityStore.updateByUniqueAttributes(typeRegistry.getEntityTypeByName("B"),
                                                                               Collections.singletonMap(NAME, b.getAttribute(NAME)),
                                                                               new AtlasEntityWithExtInfo(bPartialUpdate));
        // Verify 3 entities were updated:
        // * set b.manyA reference to a1 and a2
        // * set inverse a1.oneB reference to b
        // * set inverse a2.oneB reference to b
        assertEquals(response.getPartialUpdatedEntities().size(), 3);
        AtlasEntitiesWithExtInfo updatedEntities = entityStore.getByIds(ImmutableList.of(a1.getGuid(), a2.getGuid(), b.getGuid()));

        AtlasEntity a1Entity = updatedEntities.getEntity(a1.getGuid());
        verifyRelationshipAttributeValue(a1Entity, "oneB", b.getGuid());

        AtlasEntity a2Entity = updatedEntities.getEntity(a2.getGuid());
        verifyRelationshipAttributeValue(a2Entity, "oneB", b.getGuid());

        AtlasEntity bEntity = updatedEntities.getEntity(b.getGuid());
        verifyRelationshipAttributeList(bEntity, "manyA", ImmutableList.of(getAtlasObjectId(a1), getAtlasObjectId(a2)));


        bPartialUpdate.setRelationshipAttribute("manyA", ImmutableList.of(getAtlasObjectId(a3)));
        init();
        response = entityStore.updateByUniqueAttributes(typeRegistry.getEntityTypeByName("B"),
                                                        Collections.singletonMap(NAME, b.getAttribute(NAME)),
                                                        new AtlasEntityWithExtInfo(bPartialUpdate));
        // Verify 4 entities were updated:
        // * set b.manyA reference to a3
        // * set inverse a3.oneB reference to b
        // * disconnect inverse a1.oneB reference to b
        // * disconnect inverse a2.oneB reference to b
        assertEquals(response.getPartialUpdatedEntities().size(), 4);
        init();

        updatedEntities = entityStore.getByIds(ImmutableList.of(a1.getGuid(), a2.getGuid(), a3.getGuid(), b.getGuid()));
        a1Entity        = updatedEntities.getEntity(a1.getGuid());
        a2Entity        = updatedEntities.getEntity(a2.getGuid());
        bEntity         = updatedEntities.getEntity(b.getGuid());

        AtlasEntity a3Entity = updatedEntities.getEntity(a3.getGuid());
        verifyRelationshipAttributeValue(a3Entity, "oneB", b.getGuid());

        verifyRelationshipAttributeUpdate_NonComposite_ManyToOne(a1Entity, a2Entity, a3Entity, bEntity);
    }

    @Test
    public void testRelationshipAttributeUpdate_NonComposite_OneToOne() throws Exception {
        AtlasEntity a1 = new AtlasEntity("A");
        a1.setAttribute(NAME, "a1_name");

        AtlasEntity a2 = new AtlasEntity("A");
        a2.setAttribute(NAME, "a2_name");

        AtlasEntity b = new AtlasEntity("B");
        b.setAttribute(NAME, "b_name");

        AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(a1);
        entitiesWithExtInfo.addEntity(a2);
        entitiesWithExtInfo.addEntity(b);

        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesWithExtInfo) , false);

        AtlasEntity partialUpdateB = new AtlasEntity("B");
        partialUpdateB.setRelationshipAttribute("a", getAtlasObjectId(a1));

        init();
        AtlasEntityType bType = typeRegistry.getEntityTypeByName("B");

        response = entityStore.updateByUniqueAttributes(bType, Collections.singletonMap(NAME, b.getAttribute(NAME)), new AtlasEntityWithExtInfo(partialUpdateB));
        List<AtlasEntityHeader> partialUpdatedEntitiesHeader = response.getPartialUpdatedEntities();
        // Verify 2 entities were updated:
        // * set b.a reference to a1
        // * set inverse a1.b reference to b
        assertEquals(partialUpdatedEntitiesHeader.size(), 2);
        AtlasEntitiesWithExtInfo partialUpdatedEntities = entityStore.getByIds(ImmutableList.of(a1.getGuid(), b.getGuid()));

        AtlasEntity a1Entity = partialUpdatedEntities.getEntity(a1.getGuid());
        verifyRelationshipAttributeValue(a1Entity, "b", b.getGuid());

        AtlasEntity bEntity = partialUpdatedEntities.getEntity(b.getGuid());
        verifyRelationshipAttributeValue(bEntity, "a", a1.getGuid());

        init();

        // Update b.a to reference a2.
        partialUpdateB.setRelationshipAttribute("a", getAtlasObjectId(a2));
        response = entityStore.updateByUniqueAttributes(bType, Collections.<String, Object>singletonMap(NAME, b.getAttribute(NAME)), new AtlasEntityWithExtInfo(partialUpdateB));
        partialUpdatedEntitiesHeader = response.getPartialUpdatedEntities();
        // Verify 3 entities were updated:
        // * set b.a reference to a2
        // * set a2.b reference to b
        // * disconnect a1.b reference
        assertEquals(partialUpdatedEntitiesHeader.size(), 3);
        partialUpdatedEntities = entityStore.getByIds(ImmutableList.of(a1.getGuid(), a2.getGuid(), b.getGuid()));

        bEntity = partialUpdatedEntities.getEntity(b.getGuid());
        verifyRelationshipAttributeValue(bEntity, "a", a2.getGuid());

        AtlasEntity a2Entity = partialUpdatedEntities.getEntity(a2.getGuid());
        verifyRelationshipAttributeValue(a2Entity, "b", b.getGuid());

        a1Entity = partialUpdatedEntities.getEntity(a1.getGuid());
        verifyRelationshipAttributeUpdate_NonComposite_OneToOne(a1Entity, bEntity);
    }

    @Test
    public void testRelationshipAttributeUpdate_NonComposite_ManyToMany() throws Exception {
        AtlasEntity a1 = new AtlasEntity("A");
        a1.setAttribute(NAME, "a1_name");

        AtlasEntity a2 = new AtlasEntity("A");
        a2.setAttribute(NAME, "a2_name");

        AtlasEntity a3 = new AtlasEntity("A");
        a3.setAttribute(NAME, "a3_name");

        AtlasEntity b1 = new AtlasEntity("B");
        b1.setAttribute(NAME, "b1_name");

        AtlasEntity b2 = new AtlasEntity("B");
        b2.setAttribute(NAME, "b2_name");

        AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(a1);
        entitiesWithExtInfo.addEntity(a2);
        entitiesWithExtInfo.addEntity(a3);
        entitiesWithExtInfo.addEntity(b1);
        entitiesWithExtInfo.addEntity(b2);
        entityStore.createOrUpdate(new AtlasEntityStream(entitiesWithExtInfo) , false);

        AtlasEntity b1PartialUpdate = new AtlasEntity("B");
        b1PartialUpdate.setRelationshipAttribute("manyToManyA", ImmutableList.of(getAtlasObjectId(a1), getAtlasObjectId(a2)));

        init();
        EntityMutationResponse response = entityStore.updateByUniqueAttributes(typeRegistry.getEntityTypeByName("B"),
                                                                               Collections.singletonMap(NAME, b1.getAttribute(NAME)),
                                                                               new AtlasEntityWithExtInfo(b1PartialUpdate));

        List<AtlasEntityHeader> updatedEntityHeaders = response.getPartialUpdatedEntities();
        assertEquals(updatedEntityHeaders.size(), 3);

        AtlasEntitiesWithExtInfo updatedEntities = entityStore.getByIds(ImmutableList.of(a1.getGuid(), a2.getGuid(), b1.getGuid()));

        AtlasEntity b1Entity = updatedEntities.getEntity(b1.getGuid());
        verifyRelationshipAttributeList(b1Entity, "manyToManyA", ImmutableList.of(getAtlasObjectId(a1), getAtlasObjectId(a2)));

        AtlasEntity a1Entity = updatedEntities.getEntity(a1.getGuid());
        verifyRelationshipAttributeList(a1Entity, "manyB", ImmutableList.of(getAtlasObjectId(b1)));

        AtlasEntity a2Entity = updatedEntities.getEntity(a2.getGuid());
        verifyRelationshipAttributeList(a2Entity, "manyB", ImmutableList.of(getAtlasObjectId(b1)));
    }

    protected abstract void verifyRelationshipAttributeUpdate_NonComposite_OneToOne(AtlasEntity a1, AtlasEntity b);

    protected abstract void verifyRelationshipAttributeUpdate_NonComposite_OneToMany(AtlasEntity entity) throws Exception;

    protected abstract void verifyRelationshipAttributeUpdate_NonComposite_ManyToOne(AtlasEntity a1, AtlasEntity a2, AtlasEntity a3, AtlasEntity b);

    private static void assertObjectIdsContains(List<AtlasObjectId> objectIds, AtlasObjectId objectId) {
        assertTrue(CollectionUtils.isNotEmpty(objectIds));
        assertTrue(objectIds.contains(objectId));
    }

    private static void assertObjectIdEquals(AtlasObjectId objId1, AtlasObjectId objId2) {
        assertTrue(objId1.equals(objId2));
    }

    private static void assertEmpty(List collection) {
        assertTrue(collection != null && collection.isEmpty());
    }

    private static List<AtlasObjectId> toAtlasObjectIds(Object objectIds) {
        if (objectIds instanceof List) {
            return (List<AtlasObjectId>) objectIds;
        }

        return null;
    }

    private static AtlasObjectId toAtlasObjectId(Object objectId) {
        if (objectId instanceof AtlasObjectId) {
            return (AtlasObjectId) objectId;
        }

        return null;
    }

    private AtlasEntity getEntityFromStore(String guid) throws AtlasBaseException {
        AtlasEntityWithExtInfo entity = guid != null ? entityStore.getById(guid) : null;

        return entity != null ? entity.getEntity() : null;
    }

    protected static void verifyRelationshipAttributeList(AtlasEntity entity, String relationshipAttrName, List<AtlasObjectId> expectedValues) {
        Object refValue = entity.getRelationshipAttribute(relationshipAttrName);
        assertTrue(refValue instanceof List);

        List<AtlasObjectId> refList = (List<AtlasObjectId>) refValue;
        assertEquals(refList.size(), expectedValues.size());

        if (expectedValues.size() > 0) {
            assertTrue(refList.containsAll(expectedValues));
        }
    }

    protected static void verifyRelationshipAttributeValue(AtlasEntity entity, String relationshipAttrName, String expectedGuid) {
        Object refValue = entity.getRelationshipAttribute(relationshipAttrName);
        if (expectedGuid == null) {
            assertNull(refValue);
        }
        else {
            assertTrue(refValue instanceof AtlasObjectId);
            AtlasObjectId referencedObjectId = (AtlasObjectId) refValue;
            assertEquals(referencedObjectId.getGuid(), expectedGuid);
        }
    }
}