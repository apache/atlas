/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.CreateUpdateEntitiesResult;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtils;
import org.apache.atlas.model.legacy.EntityResult;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.NullRequiredAttributeException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.*;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test for GraphBackedMetadataRepository.deleteEntities
 *
 * Guice loads the dependencies and injects the necessary objects
 *
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public abstract class GraphBackedMetadataRepositoryDeleteTestBase {

    protected MetadataRepository repositoryService;

    private TypeSystem typeSystem;

    private ClassType compositeMapOwnerType;

    private ClassType compositeMapValueType;

    @Inject
    AtlasGraph atlasGraph;

    @BeforeClass
    public void setUp() throws Exception {

        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        new GraphBackedSearchIndexer(new AtlasTypeRegistry());
        final GraphBackedMetadataRepository delegate = new GraphBackedMetadataRepository(getDeleteHandler(typeSystem), atlasGraph);

        repositoryService = TestUtils.addTransactionWrapper(delegate);

        TestUtils.defineDeptEmployeeTypes(typeSystem);
        TestUtils.createHiveTypes(typeSystem);

        // Define type for map value.
        HierarchicalTypeDefinition<ClassType> mapValueDef = TypesUtil.createClassTypeDef("CompositeMapValue",
            ImmutableSet.<String>of(),
            TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE));

        // Define type with map where the value is a composite class reference to MapValue.
        HierarchicalTypeDefinition<ClassType> mapOwnerDef = TypesUtil.createClassTypeDef("CompositeMapOwner",
            ImmutableSet.<String>of(),
            TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
            new AttributeDefinition("map", DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                        "CompositeMapValue"), Multiplicity.OPTIONAL, true, null));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
            ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
            ImmutableList.of(mapOwnerDef, mapValueDef));
        typeSystem.defineTypes(typesDef);
        compositeMapOwnerType = typeSystem.getDataType(ClassType.class, "CompositeMapOwner");
        compositeMapValueType = typeSystem.getDataType(ClassType.class, "CompositeMapValue");
    }

    abstract DeleteHandler getDeleteHandler(TypeSystem typeSystem);

    @BeforeMethod
    public void setupContext() {
        TestUtils.resetRequestContext();
    }

    @AfterClass
    public void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
//        AtlasGraphProvider.cleanup();
    }

    @Test
    public void testDeleteAndCreate() throws Exception {
        Referenceable entity = createDBEntity();
        String id = createInstance(entity);

        //get entity by unique attribute should return the created entity
        ITypedReferenceableInstance instance =
                repositoryService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", entity.get("name"));
        assertEquals(instance.getId()._getId(), id);

        //delete entity should mark it as deleted
        List<String> results = deleteEntities(id).getDeletedEntities();
        assertEquals(results.get(0), id);
        assertEntityDeleted(id);

        //get entity by unique attribute should throw EntityNotFoundException
        try {
            repositoryService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", entity.get("name"));
            fail("Expected EntityNotFoundException");
        } catch(EntityNotFoundException e) {
            //expected
        }

        //Create the same entity again, should create new entity
        String newId = createInstance(entity);
        assertNotEquals(id, newId);

        //get by unique attribute should return the new entity
        instance = repositoryService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", entity.get("name"));
        assertEquals(instance.getId()._getId(), newId);
    }

    @Test
    public void testDeleteEntityWithTraits() throws Exception {
        Referenceable entity = createDBEntity();
        String id = createInstance(entity);

        TraitType dataType = typeSystem.getDataType(TraitType.class, PII);
        ITypedStruct trait = dataType.convert(new Struct(TestUtils.PII), Multiplicity.REQUIRED);
        repositoryService.addTrait(id, trait);

        ITypedReferenceableInstance instance = repositoryService.getEntityDefinition(id);
        assertTrue(instance.getTraits().contains(PII));

        deleteEntities(id);
        assertEntityDeleted(id);
        assertTestDeleteEntityWithTraits(id);
    }

    protected abstract void assertTestDeleteEntityWithTraits(String guid)
            throws EntityNotFoundException, RepositoryException, Exception;

    @Test
    public void testDeleteReference() throws Exception {
        //Deleting column should update table
        Referenceable db = createDBEntity();
        String dbId = createInstance(db);

        Referenceable column = createColumnEntity();
        String colId = createInstance(column);

        Referenceable table = createTableEntity(dbId);
        table.set(COLUMNS_ATTR_NAME, Arrays.asList(new Id(colId, 0, COLUMN_TYPE)));
        String tableId = createInstance(table);

        EntityResult entityResult = deleteEntities(colId);
        assertEquals(entityResult.getDeletedEntities().size(), 1);
        assertEquals(entityResult.getDeletedEntities().get(0), colId);
        assertEquals(entityResult.getUpdateEntities().size(), 1);
        assertEquals(entityResult.getUpdateEntities().get(0), tableId);

        assertEntityDeleted(colId);

        ITypedReferenceableInstance tableInstance = repositoryService.getEntityDefinition(tableId);
        assertColumnForTestDeleteReference(tableInstance);

        //Deleting table should update process
        Referenceable process = new Referenceable(PROCESS_TYPE);
        process.set(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS, Arrays.asList(new Id(tableId, 0, TABLE_TYPE)));
        String processId = createInstance(process);
        ITypedReferenceableInstance processInstance = repositoryService.getEntityDefinition(processId);

        deleteEntities(tableId);
        assertEntityDeleted(tableId);

        assertTableForTestDeleteReference(tableId);
        assertProcessForTestDeleteReference(processInstance);
    }

    protected abstract void assertTableForTestDeleteReference(String tableId) throws Exception;

    protected abstract void assertColumnForTestDeleteReference(ITypedReferenceableInstance tableInstance)
            throws AtlasException;

    protected abstract void assertProcessForTestDeleteReference(ITypedReferenceableInstance processInstance) throws Exception;

    protected abstract void assertEntityDeleted(String id) throws Exception;

    private EntityResult deleteEntities(String... id) throws Exception {
        RequestContext.createContext();
        return repositoryService.deleteEntities(Arrays.asList(id));
    }

    private String createInstance(Referenceable entity) throws Exception {
        ClassType                   dataType = typeSystem.getDataType(ClassType.class, entity.getTypeName());
        ITypedReferenceableInstance instance = dataType.convert(entity, Multiplicity.REQUIRED);
        CreateUpdateEntitiesResult  result   = repositoryService.createEntities(instance);
        List<String>                results  = result.getCreatedEntities();
        return results.get(results.size() - 1);
    }

    @Test
    public void testDeleteEntities() throws Exception {
        // Create a table entity, with 3 composite column entities
        Referenceable dbEntity = createDBEntity();
        String dbGuid = createInstance(dbEntity);
        Referenceable table1Entity = createTableEntity(dbGuid);
        Referenceable col1 = createColumnEntity();
        Referenceable col2 = createColumnEntity();
        Referenceable col3 = createColumnEntity();
        table1Entity.set(COLUMNS_ATTR_NAME, ImmutableList.of(col1, col2, col3));
        createInstance(table1Entity);

        // Retrieve the table entities from the Repository, to get their guids and the composite column guids.
        ITypedReferenceableInstance tableInstance = repositoryService.getEntityDefinition(TestUtils.TABLE_TYPE,
                NAME, table1Entity.get(NAME));
        List<IReferenceableInstance> columns = (List<IReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME);

        //Delete column
        String colId = columns.get(0).getId()._getId();
        String tableId = tableInstance.getId()._getId();

        EntityResult entityResult = deleteEntities(colId);
        assertEquals(entityResult.getDeletedEntities().size(), 1);
        assertEquals(entityResult.getDeletedEntities().get(0), colId);
        assertEquals(entityResult.getUpdateEntities().size(), 1);
        assertEquals(entityResult.getUpdateEntities().get(0), tableId);
        assertEntityDeleted(colId);

        tableInstance = repositoryService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, table1Entity.get(NAME));
        assertDeletedColumn(tableInstance);

        //update by removing a column
        tableInstance.set(COLUMNS_ATTR_NAME, ImmutableList.of(col3));
        entityResult = updatePartial(tableInstance);
        colId = columns.get(1).getId()._getId();
        assertEquals(entityResult.getDeletedEntities().size(), 1);
        assertEquals(entityResult.getDeletedEntities().get(0), colId);
        assertEntityDeleted(colId);

        // Delete the table entities.  The deletion should cascade to their composite columns.
        tableInstance = repositoryService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, table1Entity.get(NAME));
        List<String> deletedGuids = deleteEntities(tableInstance.getId()._getId()).getDeletedEntities();
        assertEquals(deletedGuids.size(), 2);

        // Verify that deleteEntities() response has guids for tables and their composite columns.
        Assert.assertTrue(deletedGuids.contains(tableInstance.getId()._getId()));
        Assert.assertTrue(deletedGuids.contains(columns.get(2).getId()._getId()));

        // Verify that tables and their composite columns have been deleted from the graph Repository.
        for (String guid : deletedGuids) {
            assertEntityDeleted(guid);
        }
        assertTestDeleteEntities(tableInstance);
    }

    protected abstract void assertDeletedColumn(ITypedReferenceableInstance tableInstance) throws AtlasException;

    protected abstract void assertTestDeleteEntities(ITypedReferenceableInstance tableInstance) throws Exception;

    /**
     * Verify deleting entities with composite references to other entities.
     * The composite entities should also be deleted.
     */
    @Test
    public void testDeleteEntitiesWithCompositeArrayReference() throws Exception {
        String hrDeptGuid = createHrDeptGraph();

        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        List<ITypedReferenceableInstance> employees = (List<ITypedReferenceableInstance>) hrDept.get("employees");
        Assert.assertEquals(employees.size(), 4);

        List<String> employeeGuids = new ArrayList(4);
        for (ITypedReferenceableInstance employee : employees) {
            employeeGuids.add(employee.getId()._getId());
        }

        // There should be 4 vertices for Address structs (one for each Person.address attribute value).
        int vertexCount = getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "Address").size();
        Assert.assertEquals(vertexCount, 4);
        vertexCount = getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "SecurityClearance").size();
        Assert.assertEquals(vertexCount, 1);

        List<String> deletedEntities = deleteEntities(hrDeptGuid).getDeletedEntities();
        assertTrue(deletedEntities.contains(hrDeptGuid));
        assertEntityDeleted(hrDeptGuid);

        // Verify Department entity and its contained Person entities were deleted.
        for (String employeeGuid : employeeGuids) {
            assertTrue(deletedEntities.contains(employeeGuid));
            assertEntityDeleted(employeeGuid);
        }

        // Verify all Person.address struct vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "Address"));

        // Verify all SecurityClearance trait vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "SecurityClearance"));
    }

    protected abstract void assertVerticesDeleted(List<AtlasVertex> vertices);

    @Test
    public void testDeleteEntitiesWithCompositeMapReference() throws Exception {
        // Create instances of MapOwner and MapValue.
        // Set MapOwner.map with one entry that references MapValue instance.
        ITypedReferenceableInstance entityDefinition = createMapOwnerAndValueEntities();
        String mapOwnerGuid = entityDefinition.getId()._getId();

        // Verify MapOwner.map attribute has expected value.
        ITypedReferenceableInstance mapOwnerInstance = repositoryService.getEntityDefinition(mapOwnerGuid);
        Object object = mapOwnerInstance.get("map");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof Map);
        Map<String, ITypedReferenceableInstance> map = (Map<String, ITypedReferenceableInstance>)object;
        Assert.assertEquals(map.size(), 1);
        ITypedReferenceableInstance mapValueInstance = map.get("value1");
        Assert.assertNotNull(mapValueInstance);
        String mapValueGuid = mapValueInstance.getId()._getId();
        String edgeLabel = GraphHelper.getEdgeLabel(compositeMapOwnerType, compositeMapOwnerType.fieldMapping.fields.get("map"));
        String mapEntryLabel = edgeLabel + "." + "value1";
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(mapEntryLabel);
        AtlasVertex mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey(), Object.class);
        Assert.assertNotNull(object);

        List<String> deletedEntities = deleteEntities(mapOwnerGuid).getDeletedEntities();
        Assert.assertEquals(deletedEntities.size(), 2);
        Assert.assertTrue(deletedEntities.contains(mapOwnerGuid));
        Assert.assertTrue(deletedEntities.contains(mapValueGuid));

        assertEntityDeleted(mapOwnerGuid);
        assertEntityDeleted(mapValueGuid);
    }

    private ITypedReferenceableInstance createMapOwnerAndValueEntities()
        throws AtlasException, RepositoryException, EntityExistsException {

        ITypedReferenceableInstance mapOwnerInstance = compositeMapOwnerType.createInstance();
        mapOwnerInstance.set(NAME, TestUtils.randomString());
        ITypedReferenceableInstance mapValueInstance = compositeMapValueType.createInstance();
        mapValueInstance.set(NAME, TestUtils.randomString());
        mapOwnerInstance.set("map", Collections.singletonMap("value1", mapValueInstance));
        List<String> createEntitiesResult = repositoryService.createEntities(mapOwnerInstance, mapValueInstance).getCreatedEntities();
        Assert.assertEquals(createEntitiesResult.size(), 2);
        ITypedReferenceableInstance entityDefinition = repositoryService.getEntityDefinition("CompositeMapOwner",
            NAME, mapOwnerInstance.get(NAME));
        return entityDefinition;
    }

    private EntityResult updatePartial(ITypedReferenceableInstance entity) throws RepositoryException {
        RequestContext.createContext();
        return repositoryService.updatePartial(entity).getEntityResult();
    }

    @Test
    public void testUpdateEntity_MultiplicityOneNonCompositeReference() throws Exception {
        String hrDeptGuid = createHrDeptGraph();
        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Map<String, String> nameGuidMap = getEmployeeNameGuidMap(hrDept);

        ITypedReferenceableInstance john = repositoryService.getEntityDefinition(nameGuidMap.get("John"));
        Id johnGuid = john.getId();

        ITypedReferenceableInstance max = repositoryService.getEntityDefinition(nameGuidMap.get("Max"));
        String maxGuid = max.getId()._getId();
        AtlasVertex vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long creationTimestamp = AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(creationTimestamp);

        Long modificationTimestampPreUpdate = AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(modificationTimestampPreUpdate);

        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition(nameGuidMap.get("Jane"));
        Id janeId = jane.getId();

        // Update max's mentor reference to john.
        ClassType personType = typeSystem.getDataType(ClassType.class, "Person");
        ITypedReferenceableInstance maxEntity = personType.createInstance(max.getId());
        maxEntity.set("mentor", johnGuid);
        EntityResult entityResult = updatePartial(maxEntity);
        assertEquals(entityResult.getUpdateEntities().size(), 1);
        assertTrue(entityResult.getUpdateEntities().contains(maxGuid));

        // Verify the update was applied correctly - john should now be max's mentor.
        max = repositoryService.getEntityDefinition(maxGuid);
        ITypedReferenceableInstance refTarget = (ITypedReferenceableInstance) max.get("mentor");
        Assert.assertEquals(refTarget.getId()._getId(), johnGuid._getId());

        // Verify modification timestamp was updated.
        vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long modificationTimestampPostUpdate = AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(modificationTimestampPostUpdate);
        Assert.assertTrue(creationTimestamp < modificationTimestampPostUpdate);

        // Update max's mentor reference to jane.
        maxEntity.set("mentor", janeId);
        entityResult = updatePartial(maxEntity);
        assertEquals(entityResult.getUpdateEntities().size(), 1);
        assertTrue(entityResult.getUpdateEntities().contains(maxGuid));

        // Verify the update was applied correctly - jane should now be max's mentor.
        max = repositoryService.getEntityDefinition(maxGuid);
        refTarget = (ITypedReferenceableInstance) max.get("mentor");
        Assert.assertEquals(refTarget.getId()._getId(), janeId._getId());

        // Verify modification timestamp was updated.
        vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long modificationTimestampPost2ndUpdate = AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(modificationTimestampPost2ndUpdate);
        Assert.assertTrue(modificationTimestampPostUpdate < modificationTimestampPost2ndUpdate);

        ITypedReferenceableInstance julius = repositoryService.getEntityDefinition(nameGuidMap.get("Julius"));
        Id juliusId = julius.getId();
        maxEntity = personType.createInstance(max.getId());
        maxEntity.set("manager", juliusId);
        entityResult = updatePartial(maxEntity);
        // Verify julius' subordinates were updated.
        assertEquals(entityResult.getUpdateEntities().size(), 3);
        assertTrue(entityResult.getUpdateEntities().contains(maxGuid));
        assertTrue(entityResult.getUpdateEntities().containsAll(Arrays.asList(maxGuid, janeId._getId(), juliusId._getId())));

        // Verify the update was applied correctly - julius should now be max's manager.
        max = repositoryService.getEntityDefinition(maxGuid);
        refTarget = (ITypedReferenceableInstance) max.get("manager");
        Assert.assertEquals(refTarget.getId()._getId(), juliusId._getId());
        Assert.assertEquals(refTarget.getId()._getId(), juliusId._getId());
        julius = repositoryService.getEntityDefinition(nameGuidMap.get("Julius"));
        Object object = julius.get("subordinates");
        Assert.assertTrue(object instanceof List);
        List<ITypedReferenceableInstance> refValues = (List<ITypedReferenceableInstance>) object;
        Assert.assertEquals(refValues.size(), 1);
        Assert.assertTrue(refValues.contains(max.getId()));

        assertTestUpdateEntity_MultiplicityOneNonCompositeReference(janeId._getId());
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
        String hrDeptGuid = createHrDeptGraph();
        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Map<String, String> nameGuidMap = getEmployeeNameGuidMap(hrDept);
        String maxGuid = nameGuidMap.get("Max");
        String janeGuid = nameGuidMap.get("Jane");
        String johnGuid = nameGuidMap.get("John");

        Assert.assertNotNull(maxGuid);
        Assert.assertNotNull(janeGuid);
        Assert.assertNotNull(johnGuid);

        // Verify that Max is one of Jane's subordinates.
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition(janeGuid);
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
        Assert.assertTrue(subordinateIds.contains(maxGuid));


        EntityResult entityResult = deleteEntities(maxGuid);
        ITypedReferenceableInstance john = repositoryService.getEntityDefinition("Person", "name", "John");

        assertEquals(entityResult.getDeletedEntities().size(), 1);
        assertTrue(entityResult.getDeletedEntities().contains(maxGuid));
        assertEquals(entityResult.getUpdateEntities().size(), 3);
        assertTrue(entityResult.getUpdateEntities().containsAll(Arrays.asList(jane.getId()._getId(), hrDeptGuid,
                john.getId()._getId())));
        assertEntityDeleted(maxGuid);

        assertMaxForTestDisconnectBidirectionalReferences(nameGuidMap);

        // Now delete jane - this should disconnect the manager reference from her
        // subordinate.
        entityResult = deleteEntities(janeGuid);
        assertEquals(entityResult.getDeletedEntities().size(), 1);
        assertTrue(entityResult.getDeletedEntities().contains(janeGuid));
        assertEquals(entityResult.getUpdateEntities().size(), 2);
        assertTrue(entityResult.getUpdateEntities().containsAll(Arrays.asList(hrDeptGuid, john.getId()._getId())));

        assertEntityDeleted(janeGuid);

        john = repositoryService.getEntityDefinition("Person", "name", "John");
        assertJohnForTestDisconnectBidirectionalReferences(john, janeGuid);
    }

    protected abstract void assertJohnForTestDisconnectBidirectionalReferences(ITypedReferenceableInstance john,
                                                                               String janeGuid) throws Exception;

    protected abstract void assertMaxForTestDisconnectBidirectionalReferences(Map<String, String> nameGuidMap)
            throws Exception;

    /**
     * Verify deleting entity that is the target of a unidirectional class array reference
     * from a class instance.
     */
    @Test
    public void testDisconnectUnidirectionalArrayReferenceFromClassType() throws Exception {
        createDbTableGraph(TestUtils.DATABASE_NAME, TestUtils.TABLE_NAME);

        // Get the guid for one of the table's columns.
        ITypedReferenceableInstance table = repositoryService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", TestUtils.TABLE_NAME);
        String tableGuid = table.getId()._getId();
        List<ITypedReferenceableInstance> columns = (List<ITypedReferenceableInstance>) table.get("columns");
        Assert.assertEquals(columns.size(), 5);
        String columnGuid = columns.get(0).getId()._getId();

        // Delete the column.
        EntityResult entityResult = deleteEntities(columnGuid);
        assertEquals(entityResult.getDeletedEntities().size(), 1);
        Assert.assertTrue(entityResult.getDeletedEntities().contains(columnGuid));
        assertEquals(entityResult.getUpdateEntities().size(), 1);
        Assert.assertTrue(entityResult.getUpdateEntities().contains(tableGuid));
        assertEntityDeleted(columnGuid);

        // Verify table.columns reference to the deleted column has been disconnected.
        table = repositoryService.getEntityDefinition(tableGuid);
        assertTestDisconnectUnidirectionalArrayReferenceFromClassType(
                (List<ITypedReferenceableInstance>) table.get("columns"), columnGuid);
    }

    protected abstract void assertTestDisconnectUnidirectionalArrayReferenceFromClassType(
            List<ITypedReferenceableInstance> columns, String columnGuid);

    /**
     * Verify deleting entities that are the target of a unidirectional class array reference
     * from a struct or trait instance.
     */
    @Test
    public void testDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes() throws Exception {
        // Define class types.
        HierarchicalTypeDefinition<ClassType> structTargetDef = TypesUtil.createClassTypeDef("StructTarget",
            ImmutableSet.<String>of(), TypesUtil.createOptionalAttrDef("attr1", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> traitTargetDef = TypesUtil.createClassTypeDef("TraitTarget",
            ImmutableSet.<String>of(), TypesUtil.createOptionalAttrDef("attr1", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> structContainerDef = TypesUtil.createClassTypeDef("StructContainer",
            ImmutableSet.<String>of(), TypesUtil.createOptionalAttrDef("struct", "TestStruct"));

        // Define struct and trait types which have a unidirectional array reference
        // to a class type.
        StructTypeDefinition structDef = TypesUtil.createStructTypeDef("TestStruct",
            new AttributeDefinition("target", DataTypes.arrayTypeName("StructTarget"), Multiplicity.OPTIONAL, false, null),
            new AttributeDefinition("nestedStructs", DataTypes.arrayTypeName("NestedStruct"), Multiplicity.OPTIONAL, false, null));
        StructTypeDefinition nestedStructDef = TypesUtil.createStructTypeDef("NestedStruct",
            TypesUtil.createOptionalAttrDef("attr1", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> traitDef = TypesUtil.createTraitTypeDef("TestTrait", ImmutableSet.<String>of(),
            new AttributeDefinition("target", DataTypes.arrayTypeName("TraitTarget"), Multiplicity.OPTIONAL, false, null));

        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.of(structDef, nestedStructDef),
            ImmutableList.of(traitDef), ImmutableList.of(structTargetDef, traitTargetDef, structContainerDef));
        typeSystem.defineTypes(typesDef);

        // Create instances of class, struct, and trait types.
        Referenceable structTargetEntity = new Referenceable("StructTarget");
        Referenceable traitTargetEntity = new Referenceable("TraitTarget");
        Referenceable structContainerEntity = new Referenceable("StructContainer");
        Struct structInstance = new Struct("TestStruct");
        Struct nestedStructInstance = new Struct("NestedStruct");
        Referenceable traitInstance = new Referenceable("TestTrait");
        structContainerEntity.set("struct", structInstance);
        structInstance.set("target", ImmutableList.of(structTargetEntity));
        structInstance.set("nestedStructs", ImmutableList.of(nestedStructInstance));

        ClassType structTargetType = typeSystem.getDataType(ClassType.class, "StructTarget");
        ClassType traitTargetType = typeSystem.getDataType(ClassType.class, "TraitTarget");
        ClassType structContainerType = typeSystem.getDataType(ClassType.class, "StructContainer");

        ITypedReferenceableInstance structTargetConvertedEntity =
            structTargetType.convert(structTargetEntity, Multiplicity.REQUIRED);
        ITypedReferenceableInstance traitTargetConvertedEntity =
            traitTargetType.convert(traitTargetEntity, Multiplicity.REQUIRED);
        ITypedReferenceableInstance structContainerConvertedEntity =
            structContainerType.convert(structContainerEntity, Multiplicity.REQUIRED);

        List<String> guids = repositoryService.createEntities(
            structTargetConvertedEntity, traitTargetConvertedEntity, structContainerConvertedEntity).getCreatedEntities();
        Assert.assertEquals(guids.size(), 3);

        guids = repositoryService.getEntityList("StructTarget");
        Assert.assertEquals(guids.size(), 1);
        String structTargetGuid = guids.get(0);

        guids = repositoryService.getEntityList("TraitTarget");
        Assert.assertEquals(guids.size(), 1);
        String traitTargetGuid = guids.get(0);

        guids = repositoryService.getEntityList("StructContainer");
        Assert.assertEquals(guids.size(), 1);
        String structContainerGuid = guids.get(0);

        // Add TestTrait to StructContainer instance
        traitInstance.set("target", ImmutableList.of(new Id(traitTargetGuid, 0, "TraitTarget")));
        TraitType traitType = typeSystem.getDataType(TraitType.class, "TestTrait");
        ITypedStruct convertedTrait = traitType.convert(traitInstance, Multiplicity.REQUIRED);
        repositoryService.addTrait(structContainerGuid, convertedTrait);

        // Verify that the unidirectional references from the struct and trait instances
        // are pointing at the target entities.
        structContainerConvertedEntity = repositoryService.getEntityDefinition(structContainerGuid);
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

        // Delete the entities that are targets of the struct and trait instances.
        EntityResult entityResult = deleteEntities(structTargetGuid, traitTargetGuid);
        Assert.assertEquals(entityResult.getDeletedEntities().size(), 2);
        Assert.assertTrue(entityResult.getDeletedEntities().containsAll(Arrays.asList(structTargetGuid, traitTargetGuid)));
        assertEntityDeleted(structTargetGuid);
        assertEntityDeleted(traitTargetGuid);

        assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(structContainerGuid);

        // Delete the entity which contains nested structs and has the TestTrait trait.
        entityResult = deleteEntities(structContainerGuid);
        Assert.assertEquals(entityResult.getDeletedEntities().size(), 1);
        Assert.assertTrue(entityResult.getDeletedEntities().contains(structContainerGuid));
        assertEntityDeleted(structContainerGuid);

        // Verify all TestStruct struct vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "TestStruct"));

        // Verify all NestedStruct struct vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "NestedStruct"));

        // Verify all TestTrait trait vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "TestTrait"));
    }

    protected abstract void assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(
            String structContainerGuid) throws Exception;

    /**
     * Verify deleting entities that are the target of class map references.
     */
    @Test
    public void testDisconnectMapReferenceFromClassType() throws Exception {
        // Define type for map value.
        HierarchicalTypeDefinition<ClassType> mapValueDef = TypesUtil.createClassTypeDef("MapValue",
            ImmutableSet.<String>of(),
            new AttributeDefinition("biMapOwner", "MapOwner", Multiplicity.OPTIONAL, false, "biMap"));

        // Define type with unidirectional and bidirectional map references,
        // where the map value is a class reference to MapValue.
        HierarchicalTypeDefinition<ClassType> mapOwnerDef = TypesUtil.createClassTypeDef("MapOwner",
            ImmutableSet.<String>of(),
            new AttributeDefinition("map", DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                        "MapValue"), Multiplicity.OPTIONAL, false, null),
            new AttributeDefinition("biMap", DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                "MapValue"), Multiplicity.OPTIONAL, false, "biMapOwner"));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
            ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
            ImmutableList.of(mapOwnerDef, mapValueDef));
        typeSystem.defineTypes(typesDef);
        ClassType mapOwnerType = typeSystem.getDataType(ClassType.class, "MapOwner");
        ClassType mapValueType = typeSystem.getDataType(ClassType.class, "MapValue");

        // Create instances of MapOwner and MapValue.
        // Set MapOwner.map and MapOwner.biMap with one entry that references MapValue instance.
        ITypedReferenceableInstance mapOwnerInstance = mapOwnerType.createInstance();
        ITypedReferenceableInstance mapValueInstance = mapValueType.createInstance();
        mapOwnerInstance.set("map", Collections.singletonMap("value1", mapValueInstance));
        mapOwnerInstance.set("biMap", Collections.singletonMap("value1", mapValueInstance));
        // Set biMapOwner reverse reference on MapValue.
        mapValueInstance.set("biMapOwner", mapOwnerInstance);
        List<String> createEntitiesResult = repositoryService.createEntities(mapOwnerInstance, mapValueInstance).getCreatedEntities();
        Assert.assertEquals(createEntitiesResult.size(), 2);
        List<String> guids = repositoryService.getEntityList("MapOwner");
        Assert.assertEquals(guids.size(), 1);
        String mapOwnerGuid = guids.get(0);

        String edgeLabel = GraphHelper.getEdgeLabel(mapOwnerType, mapOwnerType.fieldMapping.fields.get("map"));
        String mapEntryLabel = edgeLabel + "." + "value1";
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(mapEntryLabel);

        // Verify MapOwner.map attribute has expected value.
        String mapValueGuid = null;
        AtlasVertex mapOwnerVertex = null;
        mapOwnerInstance = repositoryService.getEntityDefinition(mapOwnerGuid);
        for (String mapAttrName : Arrays.asList("map", "biMap")) {
            Object object = mapOwnerInstance.get(mapAttrName);
            Assert.assertNotNull(object);
            Assert.assertTrue(object instanceof Map);
            Map<String, ITypedReferenceableInstance> map = (Map<String, ITypedReferenceableInstance>)object;
            Assert.assertEquals(map.size(), 1);
            mapValueInstance = map.get("value1");
            Assert.assertNotNull(mapValueInstance);
            mapValueGuid = mapValueInstance.getId()._getId();
            mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
            object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey(), Object.class);
            Assert.assertNotNull(object);
        }

        // Delete the map value instance.
        // This should disconnect the references from the map owner instance.
        deleteEntities(mapValueGuid);
        assertEntityDeleted(mapValueGuid);
        assertTestDisconnectMapReferenceFromClassType(mapOwnerGuid);
    }

    protected abstract void assertTestDisconnectMapReferenceFromClassType(String mapOwnerGuid) throws Exception;

    @Test
    public void testDeleteTargetOfMultiplicityOneRequiredReference() throws Exception {
        createDbTableGraph("db1", "table1");
        ITypedReferenceableInstance db = repositoryService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", "db1");
        try {
            // table1 references db1 through the required reference hive_table.database.
            // Attempt to delete db1 should cause a NullRequiredAttributeException,
            // as that would violate the lower bound on table1's database attribute.
            deleteEntities(db.getId()._getId());
            Assert.fail("Lower bound on attribute hive_table.database was not enforced - " +
                NullRequiredAttributeException.class.getSimpleName() + " was expected but none thrown");
        }
        catch (Exception e) {
            verifyExceptionThrown(e, NullRequiredAttributeException.class);
        }

        // Delete table1.
        ITypedReferenceableInstance table1 = repositoryService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", "table1");
        Assert.assertNotNull(table1);
        deleteEntities(table1.getId()._getId());

        // Now delete of db1 should succeed, since it is no longer the target
        // of the required reference from the deleted table1.
        deleteEntities(db.getId()._getId());
    }

    @Test
    public void testDeleteTargetOfMultiplicityManyRequiredReference() throws Exception {
        String deptGuid = createHrDeptGraph();
        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(deptGuid);
        Map<String, String> nameGuidMap = getEmployeeNameGuidMap(hrDept);

        // Delete John - this should work, as it would reduce the cardinality of Jane's subordinates reference
        // from 2 to 1.
        deleteEntities(nameGuidMap.get("John"));

        // Attempt to delete Max - this should cause a NullRequiredAttributeException,
        // as that would reduce the cardinality on Jane's subordinates reference from 1 to 0
        // and violate the lower bound.
        try {
            deleteEntities(nameGuidMap.get("Max"));
            assertTestDeleteTargetOfMultiplicityRequiredReference();
        }
        catch (Exception e) {
            verifyExceptionThrown(e, NullRequiredAttributeException.class);
        }
    }

    protected abstract void assertTestDeleteTargetOfMultiplicityRequiredReference() throws Exception;

    @Test
    public void testDeleteTargetOfRequiredMapReference() throws Exception {
        // Define type for map value.
        HierarchicalTypeDefinition<ClassType> mapValueDef = TypesUtil.createClassTypeDef("RequiredMapValue",
            ImmutableSet.<String>of());

        // Define type with required map references where the map value is a class reference to RequiredMapValue.
        HierarchicalTypeDefinition<ClassType> mapOwnerDef = TypesUtil.createClassTypeDef("RequiredMapOwner",
            ImmutableSet.<String>of(),
            new AttributeDefinition("map", DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                        "RequiredMapValue"), Multiplicity.REQUIRED, false, null));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
            ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
            ImmutableList.of(mapOwnerDef, mapValueDef));
        typeSystem.defineTypes(typesDef);
        ClassType mapOwnerType = typeSystem.getDataType(ClassType.class, "RequiredMapOwner");
        ClassType mapValueType = typeSystem.getDataType(ClassType.class, "RequiredMapValue");

        // Create instances of RequiredMapOwner and RequiredMapValue.
        // Set RequiredMapOwner.map with one entry that references RequiredMapValue instance.
        ITypedReferenceableInstance mapOwnerInstance = mapOwnerType.createInstance();
        ITypedReferenceableInstance mapValueInstance = mapValueType.createInstance();
        mapOwnerInstance.set("map", Collections.singletonMap("value1", mapValueInstance));
        List<String> createEntitiesResult = repositoryService.createEntities(mapOwnerInstance, mapValueInstance).getCreatedEntities();
        Assert.assertEquals(createEntitiesResult.size(), 2);
        List<String> guids = repositoryService.getEntityList("RequiredMapOwner");
        Assert.assertEquals(guids.size(), 1);
        String mapOwnerGuid = guids.get(0);
        guids = repositoryService.getEntityList("RequiredMapValue");
        Assert.assertEquals(guids.size(), 1);
        String mapValueGuid = guids.get(0);

        // Verify MapOwner.map attribute has expected value.
        mapOwnerInstance = repositoryService.getEntityDefinition(mapOwnerGuid);
        Object object = mapOwnerInstance.get("map");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof Map);
        Map<String, ITypedReferenceableInstance> map = (Map<String, ITypedReferenceableInstance>)object;
        Assert.assertEquals(map.size(), 1);
        mapValueInstance = map.get("value1");
        Assert.assertNotNull(mapValueInstance);
        Assert.assertEquals(mapValueInstance.getId()._getId(), mapValueGuid);
        String edgeLabel = GraphHelper.getEdgeLabel(mapOwnerType, mapOwnerType.fieldMapping.fields.get("map"));
        String mapEntryLabel = edgeLabel + "." + "value1";
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(mapEntryLabel);
        AtlasVertex mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey(), Object.class);
        Assert.assertNotNull(object);

        // Verify deleting the target of required map attribute throws a NullRequiredAttributeException.
        try {
            deleteEntities(mapValueGuid);
            Assert.fail(NullRequiredAttributeException.class.getSimpleName() + " was expected but none thrown.");
        }
        catch (Exception e) {
            verifyExceptionThrown(e, NullRequiredAttributeException.class);
        }
    }

    @Test
    public void testLowerBoundsIgnoredOnDeletedEntities() throws Exception {

        String hrDeptGuid = createHrDeptGraph();
        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Map<String, String> nameGuidMap = getEmployeeNameGuidMap(hrDept);

        ITypedReferenceableInstance john = repositoryService.getEntityDefinition(nameGuidMap.get("John"));
        String johnGuid = john.getId()._getId();

        ITypedReferenceableInstance max = repositoryService.getEntityDefinition(nameGuidMap.get("Max"));
        String maxGuid = max.getId()._getId();

        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition(nameGuidMap.get("Jane"));
        String janeGuid = jane.getId()._getId();

        // The lower bound constraint on Manager.subordinates should not be enforced on Jane since that entity is being deleted.
        // Prior to the fix for ATLAS-991, this call would fail with a NullRequiredAttributeException.
        EntityResult deleteResult = deleteEntities(johnGuid, maxGuid, janeGuid);
        Assert.assertEquals(deleteResult.getDeletedEntities().size(), 3);
        Assert.assertTrue(deleteResult.getDeletedEntities().containsAll(Arrays.asList(johnGuid, maxGuid, janeGuid)));
        Assert.assertEquals(deleteResult.getUpdateEntities().size(), 1);

        // Verify that Department entity was updated to disconnect its references to the deleted employees.
        Assert.assertEquals(deleteResult.getUpdateEntities().get(0), hrDeptGuid);
        hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Object object = hrDept.get("employees");
        Assert.assertTrue(object instanceof List);
        List<ITypedReferenceableInstance> employees = (List<ITypedReferenceableInstance>) object;
        assertTestLowerBoundsIgnoredOnDeletedEntities(employees);
    }

    protected abstract void assertTestLowerBoundsIgnoredOnDeletedEntities(List<ITypedReferenceableInstance> employees);

    @Test
    public void testLowerBoundsIgnoredOnCompositeDeletedEntities() throws Exception {
        String hrDeptGuid = createHrDeptGraph();
        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Map<String, String> nameGuidMap = getEmployeeNameGuidMap(hrDept);
        ITypedReferenceableInstance john = repositoryService.getEntityDefinition(nameGuidMap.get("John"));
        String johnGuid = john.getId()._getId();
        ITypedReferenceableInstance max = repositoryService.getEntityDefinition(nameGuidMap.get("Max"));
        String maxGuid = max.getId()._getId();

        // The lower bound constraint on Manager.subordinates should not be enforced on the composite entity
        // for Jane owned by the Department entity, since that entity is being deleted.
        // Prior to the fix for ATLAS-991, this call would fail with a NullRequiredAttributeException.
        EntityResult deleteResult = deleteEntities(johnGuid, maxGuid, hrDeptGuid);
        Assert.assertEquals(deleteResult.getDeletedEntities().size(), 5);
        Assert.assertTrue(deleteResult.getDeletedEntities().containsAll(nameGuidMap.values()));
        Assert.assertTrue(deleteResult.getDeletedEntities().contains(hrDeptGuid));
        assertTestLowerBoundsIgnoredOnCompositeDeletedEntities(hrDeptGuid);
    }


    protected abstract void assertTestLowerBoundsIgnoredOnCompositeDeletedEntities(String hrDeptGuid) throws Exception;

    @Test
    public void testLowerBoundsIgnoredWhenDeletingCompositeEntitesOwnedByMap() throws Exception {
        // Define MapValueReferencer type with required reference to CompositeMapValue.
        HierarchicalTypeDefinition<ClassType> mapValueReferencerTypeDef = TypesUtil.createClassTypeDef("MapValueReferencer",
            ImmutableSet.<String>of(),
            new AttributeDefinition("refToMapValue", "CompositeMapValue", Multiplicity.REQUIRED, false, null));

        // Define MapValueReferencerContainer type with required composite map reference to MapValueReferencer.
        HierarchicalTypeDefinition<ClassType> mapValueReferencerContainerTypeDef =
            TypesUtil.createClassTypeDef("MapValueReferencerContainer",
            ImmutableSet.<String>of(),
            new AttributeDefinition("requiredMap", DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(), "MapValueReferencer"), Multiplicity.REQUIRED, true, null));

        Map<String, IDataType> definedClassTypes = typeSystem.defineClassTypes(mapValueReferencerTypeDef, mapValueReferencerContainerTypeDef);
        ClassType mapValueReferencerClassType = (ClassType) definedClassTypes.get("MapValueReferencer");
        ClassType mapValueReferencerContainerType = (ClassType) definedClassTypes.get("MapValueReferencerContainer");

        // Create instances of CompositeMapOwner and CompositeMapValue.
        // Set MapOwner.map with one entry that references MapValue instance.
        ITypedReferenceableInstance entityDefinition = createMapOwnerAndValueEntities();
        String mapOwnerGuid = entityDefinition.getId()._getId();

        // Verify MapOwner.map attribute has expected value.
        ITypedReferenceableInstance mapOwnerInstance = repositoryService.getEntityDefinition(mapOwnerGuid);
        Object object = mapOwnerInstance.get("map");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof Map);
        Map<String, ITypedReferenceableInstance> map = (Map<String, ITypedReferenceableInstance>)object;
        Assert.assertEquals(map.size(), 1);
        ITypedReferenceableInstance mapValueInstance = map.get("value1");
        Assert.assertNotNull(mapValueInstance);
        String mapValueGuid = mapValueInstance.getId()._getId();

        // Create instance of MapValueReferencerContainer
        RequestContext.createContext();
        ITypedReferenceableInstance mapValueReferencerContainer = mapValueReferencerContainerType.createInstance();
        List<String> createdEntities = repositoryService.createEntities(mapValueReferencerContainer).getCreatedEntities();
        Assert.assertEquals(createdEntities.size(), 1);
        String mapValueReferencerContainerGuid = createdEntities.get(0);
        mapValueReferencerContainer = repositoryService.getEntityDefinition(createdEntities.get(0));

        // Create instance of MapValueReferencer, and update mapValueReferencerContainer
        // to reference it.
        ITypedReferenceableInstance mapValueReferencer = mapValueReferencerClassType.createInstance();
        mapValueReferencerContainer.set("requiredMap", Collections.singletonMap("value1", mapValueReferencer));
        mapValueReferencer.set("refToMapValue", mapValueInstance.getId());

        RequestContext.createContext();
        EntityResult updateEntitiesResult = repositoryService.updateEntities(mapValueReferencerContainer).getEntityResult();
        Assert.assertEquals(updateEntitiesResult.getCreatedEntities().size(), 1);
        Assert.assertEquals(updateEntitiesResult.getUpdateEntities().size(), 1);
        Assert.assertEquals(updateEntitiesResult.getUpdateEntities().get(0), mapValueReferencerContainerGuid);
        String mapValueReferencerGuid = updateEntitiesResult.getCreatedEntities().get(0);

        // Delete map owner and map referencer container.  A total of 4 entities should be deleted,
        // including the composite entities.  The lower bound constraint on MapValueReferencer.refToMapValue
        // should not be enforced on the composite MapValueReferencer since it is being deleted.
        EntityResult deleteEntitiesResult = repositoryService.deleteEntities(Arrays.asList(mapOwnerGuid, mapValueReferencerContainerGuid));
        Assert.assertEquals(deleteEntitiesResult.getDeletedEntities().size(), 4);
        Assert.assertTrue(deleteEntitiesResult.getDeletedEntities().containsAll(
            Arrays.asList(mapOwnerGuid, mapValueGuid, mapValueReferencerContainerGuid, mapValueReferencerGuid)));
    }

    @Test
    public void testDeleteMixOfExistentAndNonExistentEntities() throws Exception {
        ITypedReferenceableInstance entity1 = compositeMapValueType.createInstance();
        ITypedReferenceableInstance entity2 = compositeMapValueType.createInstance();
        List<String> createEntitiesResult = repositoryService.createEntities(entity1, entity2).getCreatedEntities();
        Assert.assertEquals(createEntitiesResult.size(), 2);
        List<String> guids = Arrays.asList(createEntitiesResult.get(0), "non-existent-guid1", "non-existent-guid2", createEntitiesResult.get(1));
        EntityResult deleteEntitiesResult = repositoryService.deleteEntities(guids);
        Assert.assertEquals(deleteEntitiesResult.getDeletedEntities().size(), 2);
        Assert.assertTrue(deleteEntitiesResult.getDeletedEntities().containsAll(createEntitiesResult));
    }

    @Test
    public void testDeleteMixOfNullAndNonNullGuids() throws Exception {
        ITypedReferenceableInstance entity1 = compositeMapValueType.createInstance();
        ITypedReferenceableInstance entity2 = compositeMapValueType.createInstance();
        List<String> createEntitiesResult = repositoryService.createEntities(entity1, entity2).getCreatedEntities();
        Assert.assertEquals(createEntitiesResult.size(), 2);
        List<String> guids = Arrays.asList(createEntitiesResult.get(0), null, null, createEntitiesResult.get(1));
        EntityResult deleteEntitiesResult = repositoryService.deleteEntities(guids);
        Assert.assertEquals(deleteEntitiesResult.getDeletedEntities().size(), 2);
        Assert.assertTrue(deleteEntitiesResult.getDeletedEntities().containsAll(createEntitiesResult));
    }

    @Test
    public void testDeleteCompositeEntityAndContainer() throws Exception {
        Referenceable db = createDBEntity();
        String dbId = createInstance(db);

        Referenceable column = createColumnEntity();
        String colId = createInstance(column);

        Referenceable table1 = createTableEntity(dbId);
        table1.set(COLUMNS_ATTR_NAME, Arrays.asList(new Id(colId, 0, COLUMN_TYPE)));
        String table1Id = createInstance(table1);
        Referenceable table2 = createTableEntity(dbId);
        String table2Id = createInstance(table2);

        // Delete the tables and column
        EntityResult entityResult = deleteEntities(table1Id, colId, table2Id);
        Assert.assertEquals(entityResult.getDeletedEntities().size(), 3);
        Assert.assertTrue(entityResult.getDeletedEntities().containsAll(Arrays.asList(colId, table1Id, table2Id)));
        assertEntityDeleted(table1Id);
        assertEntityDeleted(colId);
        assertEntityDeleted(table2Id);
    }

    @Test
    public void testDeleteEntityWithDuplicateReferenceListElements() throws Exception {
        // Create a table entity, with 2 composite column entities
        Referenceable dbEntity = createDBEntity();
        String dbGuid = createInstance(dbEntity);
        Referenceable table1Entity = createTableEntity(dbGuid);
        String tableName = TestUtils.randomString();
        table1Entity.set(NAME, tableName);
        Referenceable col1 = createColumnEntity();
        col1.set(NAME, TestUtils.randomString());
        Referenceable col2 = createColumnEntity();
        col2.set(NAME, TestUtils.randomString());
        // Populate columns reference list with duplicates.
        table1Entity.set(COLUMNS_ATTR_NAME, ImmutableList.of(col1, col2, col1, col2));
        ClassType dataType = typeSystem.getDataType(ClassType.class, table1Entity.getTypeName());
        ITypedReferenceableInstance instance = dataType.convert(table1Entity, Multiplicity.REQUIRED);
        TestUtils.resetRequestContext();
        List<String> result = repositoryService.createEntities(instance).getCreatedEntities();
        Assert.assertEquals(result.size(), 3);
        ITypedReferenceableInstance entityDefinition = repositoryService.getEntityDefinition(TABLE_TYPE, NAME, tableName);
        String tableGuid = entityDefinition.getId()._getId();
        Object attrValue = entityDefinition.get(COLUMNS_ATTR_NAME);
        assertTrue(attrValue instanceof List);
        List<ITypedReferenceableInstance> columns = (List<ITypedReferenceableInstance>) attrValue;
        Assert.assertEquals(columns.size(), 4);
        TestUtils.resetRequestContext();
        String columnGuid = columns.get(0).getId()._getId();

        // Delete one of the columns.
        EntityResult deleteResult = repositoryService.deleteEntities(Collections.singletonList(columnGuid));
        Assert.assertEquals(deleteResult.getDeletedEntities().size(), 1);
        Assert.assertTrue(deleteResult.getDeletedEntities().contains(columnGuid));
        Assert.assertEquals(deleteResult.getUpdateEntities().size(), 1);
        Assert.assertTrue(deleteResult.getUpdateEntities().contains(tableGuid));

        // Verify the duplicate edge IDs were all removed from reference property list.
        AtlasVertex tableVertex = GraphHelper.getInstance().getVertexForGUID(tableGuid);
        String columnsPropertyName = GraphHelper.getQualifiedFieldName(dataType, COLUMNS_ATTR_NAME);
        List columnsPropertyValue = tableVertex.getProperty(columnsPropertyName, List.class);
        verifyTestDeleteEntityWithDuplicateReferenceListElements(columnsPropertyValue);
    }

    protected abstract void verifyTestDeleteEntityWithDuplicateReferenceListElements(List columnsPropertyValue);

    private String createHrDeptGraph() throws Exception {
        ITypedReferenceableInstance hrDept = TestUtils.createDeptEg1(typeSystem);

        List<String> guids = repositoryService.createEntities(hrDept).getCreatedEntities();
        Assert.assertNotNull(guids);
        Assert.assertEquals(guids.size(), 5);

        return getDepartmentGuid(guids);
    }

    private String getDepartmentGuid(List<String> guids)
        throws RepositoryException, EntityNotFoundException {

        String hrDeptGuid = null;
        for (String guid : guids) {
            ITypedReferenceableInstance entityDefinition = repositoryService.getEntityDefinition(guid);
            Id id = entityDefinition.getId();
            if (id.getTypeName().equals("Department")) {
                hrDeptGuid = id._getId();
                break;
            }
        }
        if (hrDeptGuid == null) {
            Assert.fail("Entity for type Department not found");
        }
        return hrDeptGuid;
    }

    private void createDbTableGraph(String dbName, String tableName) throws Exception {
        Referenceable databaseInstance = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance.set("name", dbName);
        databaseInstance.set("description", "foo database");

        ClassType dbType = typeSystem.getDataType(ClassType.class, TestUtils.DATABASE_TYPE);
        ITypedReferenceableInstance db = dbType.convert(databaseInstance, Multiplicity.REQUIRED);
        Referenceable tableInstance = new Referenceable(TestUtils.TABLE_TYPE, TestUtils.CLASSIFICATION);
        tableInstance.set("name", tableName);
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
        Struct traitInstance = (Struct) tableInstance.getTrait(TestUtils.CLASSIFICATION);
        traitInstance.set("tag", "foundation_etl");
        tableInstance.set("tableType", 1); // enum

        tableInstance.set("database", databaseInstance);
        ArrayList<Referenceable> columns = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            Referenceable columnInstance = new Referenceable("column_type");
            final String name = "column_" + index;
            columnInstance.set("name", name);
            columnInstance.set("type", "string");
            columns.add(columnInstance);
        }
        tableInstance.set("columns", columns);
        ClassType tableType = typeSystem.getDataType(ClassType.class, TestUtils.TABLE_TYPE);
        ITypedReferenceableInstance table = tableType.convert(tableInstance, Multiplicity.REQUIRED);
        repositoryService.createEntities(db, table);
    }

    protected List<AtlasVertex> getVertices(String propertyName, Object value) {
        AtlasGraph graph = TestUtils.getGraph();
        Iterable<AtlasVertex> vertices = graph.getVertices(propertyName, value);
        List<AtlasVertex> list = new ArrayList<>();
        for (AtlasVertex vertex : vertices) {
            list.add(vertex);
        }
        return list;
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
