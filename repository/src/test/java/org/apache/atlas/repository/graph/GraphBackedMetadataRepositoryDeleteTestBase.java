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
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Vertex;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.NullRequiredAttributeException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
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

import static org.apache.atlas.TestUtils.COLUMNS_ATTR_NAME;
import static org.apache.atlas.TestUtils.COLUMN_TYPE;
import static org.apache.atlas.TestUtils.NAME;
import static org.apache.atlas.TestUtils.PROCESS_TYPE;
import static org.apache.atlas.TestUtils.TABLE_TYPE;
import static org.apache.atlas.TestUtils.createColumnEntity;
import static org.apache.atlas.TestUtils.createDBEntity;
import static org.apache.atlas.TestUtils.createTableEntity;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test for GraphBackedMetadataRepository.deleteEntities
 *
 * Guice loads the dependencies and injects the necessary objects
 *
 */
@Guice(modules = RepositoryMetadataModule.class)
public abstract class GraphBackedMetadataRepositoryDeleteTestBase {

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    protected GraphBackedMetadataRepository repositoryService;

    private TypeSystem typeSystem;

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        new GraphBackedSearchIndexer(graphProvider);

        repositoryService = new GraphBackedMetadataRepository(graphProvider, getDeleteHandler(typeSystem));

        TestUtils.defineDeptEmployeeTypes(typeSystem);
        TestUtils.createHiveTypes(typeSystem);
    }

    abstract DeleteHandler getDeleteHandler(TypeSystem typeSystem);

    @BeforeMethod
    public void setupContext() {
        RequestContext.createContext();
    }

    @AfterClass
    public void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
        try {
            graphProvider.get().shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        List<String> results = deleteEntities(id);
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
    public void testDeleteReference() throws Exception {
        //Deleting column should update table
        Referenceable db = createDBEntity();
        String dbId = createInstance(db);

        Referenceable column = createColumnEntity();
        String colId = createInstance(column);

        Referenceable table = createTableEntity(dbId);
        table.set(COLUMNS_ATTR_NAME, Arrays.asList(new Id(colId, 0, COLUMN_TYPE)));
        String tableId = createInstance(table);

        deleteEntities(colId);
        assertEntityDeleted(colId);

        ITypedReferenceableInstance tableInstance = repositoryService.getEntityDefinition(tableId);
        List<ITypedReferenceableInstance> columns =
                (List<ITypedReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME);
        assertNull(columns);

        //Deleting table should update process
        Referenceable process = new Referenceable(PROCESS_TYPE);
        process.set(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS, Arrays.asList(new Id(tableId, 0, TABLE_TYPE)));
        String processId = createInstance(process);
        ITypedReferenceableInstance processInstance = repositoryService.getEntityDefinition(processId);

        deleteEntities(tableId);
        assertEntityDeleted(tableId);
        assertTestDeleteReference(processInstance);
    }

    protected abstract void assertTestDeleteReference(ITypedReferenceableInstance processInstance) throws Exception;

    protected abstract void assertEntityDeleted(String id) throws Exception;

    private List<String> deleteEntities(String... id) throws Exception {
        RequestContext.createContext();
        List<String> response = repositoryService.deleteEntities(Arrays.asList(id)).left;
        assertNotNull(response);
        return response;
    }

    private String createInstance(Referenceable entity) throws Exception {
        ClassType dataType = typeSystem.getDataType(ClassType.class, entity.getTypeName());
        ITypedReferenceableInstance instance = dataType.convert(entity, Multiplicity.REQUIRED);
        List<String> results = repositoryService.createEntities(instance);
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

        // Retrieve the table entities from the auditRepository,
        // to get their guids and the composite column guids.
        ITypedReferenceableInstance tableInstance = repositoryService.getEntityDefinition(TestUtils.TABLE_TYPE,
                NAME, table1Entity.get(NAME));
        List<IReferenceableInstance> table1Columns = (List<IReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME);

        // Delete the table entities.  The deletion should cascade
        // to their composite columns.
        List<String> deletedGuids = deleteEntities(tableInstance.getId()._getId());

        // Verify that deleteEntities() response has guids for tables and their composite columns.
        Assert.assertTrue(deletedGuids.contains(tableInstance.getId()._getId()));
        for (IReferenceableInstance column : table1Columns) {
            Assert.assertTrue(deletedGuids.contains(column.getId()._getId()));
        }

        // Verify that tables and their composite columns have been deleted from the graph Repository.
        for (String guid : deletedGuids) {
            assertEntityDeleted(guid);
        }
        assertTestDeleteEntities(tableInstance);
    }

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

        List<String> deletedEntities = deleteEntities(hrDeptGuid);
        assertTrue(deletedEntities.contains(hrDeptGuid));

        // Verify Department entity and its contained Person entities were deleted.
        assertEntityDeleted(hrDeptGuid);
        for (String employeeGuid : employeeGuids) {
            assertEntityDeleted(employeeGuid);
        }

        // Verify all Person.address struct vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "Address"));

        // Verify all SecurityClearance trait vertices were removed.
        assertVerticesDeleted(getVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "SecurityClearance"));
    }

    protected abstract void assertVerticesDeleted(List<Vertex> vertices);

    @Test
    public void testDeleteEntitiesWithCompositeMapReference() throws Exception {
        // Define type for map value.
        HierarchicalTypeDefinition<ClassType> mapValueDef = TypesUtil.createClassTypeDef("CompositeMapValue", 
            ImmutableSet.<String>of(),
            TypesUtil.createOptionalAttrDef("attr1", DataTypes.STRING_TYPE));

        // Define type with map where the value is a composite class reference to MapValue.
        HierarchicalTypeDefinition<ClassType> mapOwnerDef = TypesUtil.createClassTypeDef("CompositeMapOwner", 
            ImmutableSet.<String>of(),
            new AttributeDefinition("map", DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                        "CompositeMapValue"), Multiplicity.OPTIONAL, true, null));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
            ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
            ImmutableList.of(mapOwnerDef, mapValueDef));
        typeSystem.defineTypes(typesDef);
        ClassType mapOwnerType = typeSystem.getDataType(ClassType.class, "CompositeMapOwner");
        ClassType mapValueType = typeSystem.getDataType(ClassType.class, "CompositeMapValue");

        // Create instances of MapOwner and MapValue.
        // Set MapOwner.map with one entry that references MapValue instance.
        ITypedReferenceableInstance mapOwnerInstance = mapOwnerType.createInstance();
        ITypedReferenceableInstance mapValueInstance = mapValueType.createInstance();
        mapOwnerInstance.set("map", Collections.singletonMap("value1", mapValueInstance));
        List<String> createEntitiesResult = repositoryService.createEntities(mapOwnerInstance, mapValueInstance);
        Assert.assertEquals(createEntitiesResult.size(), 2);
        List<String> guids = repositoryService.getEntityList("CompositeMapOwner");
        Assert.assertEquals(guids.size(), 1);
        String mapOwnerGuid = guids.get(0);

        // Verify MapOwner.map attribute has expected value.
        mapOwnerInstance = repositoryService.getEntityDefinition(mapOwnerGuid);
        Object object = mapOwnerInstance.get("map");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof Map);
        Map<String, ITypedReferenceableInstance> map = (Map<String, ITypedReferenceableInstance>)object;
        Assert.assertEquals(map.size(), 1);
        mapValueInstance = map.get("value1");
        Assert.assertNotNull(mapValueInstance);
        String mapValueGuid = mapValueInstance.getId()._getId();
        String edgeLabel = GraphHelper.getEdgeLabel(mapOwnerType, mapOwnerType.fieldMapping.fields.get("map"));
        String mapEntryLabel = edgeLabel + "." + "value1";
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(mapEntryLabel);
        Vertex mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey());
        Assert.assertNotNull(object);

        List<String> deletedEntities = deleteEntities(mapOwnerGuid);
        Assert.assertEquals(deletedEntities.size(), 2);
        Assert.assertTrue(deletedEntities.containsAll(guids));

        assertEntityDeleted(mapOwnerGuid);
        assertEntityDeleted(mapValueGuid);
    }

    private TypeUtils.Pair<List<String>, List<String>> updatePartial(ITypedReferenceableInstance entity) throws RepositoryException {
        RequestContext.createContext();
        return repositoryService.updatePartial(entity);
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
        Vertex vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long creationTimestamp = vertex.getProperty(Constants.TIMESTAMP_PROPERTY_KEY);
        Assert.assertNotNull(creationTimestamp);

        Long modificationTimestampPreUpdate = vertex.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);
        Assert.assertNotNull(modificationTimestampPreUpdate);

        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition(nameGuidMap.get("Jane"));
        Id janeGuid = jane.getId();

        // Update max's mentor reference to john.
        ClassType personType = typeSystem.getDataType(ClassType.class, "Person");
        ITypedReferenceableInstance maxEntity = personType.createInstance(max.getId());
        maxEntity.set("mentor", johnGuid);
        updatePartial(maxEntity);

        // Verify the update was applied correctly - john should now be max's mentor.
        max = repositoryService.getEntityDefinition(maxGuid);
        ITypedReferenceableInstance refTarget = (ITypedReferenceableInstance) max.get("mentor");
        Assert.assertEquals(refTarget.getId()._getId(), johnGuid._getId());

        // Verify modification timestamp was updated.
        vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long modificationTimestampPostUpdate = vertex.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);
        Assert.assertNotNull(modificationTimestampPostUpdate);
        Assert.assertTrue(creationTimestamp < modificationTimestampPostUpdate);

        // Update max's mentor reference to jane.
        maxEntity.set("mentor", janeGuid);
        updatePartial(maxEntity);

        // Verify the update was applied correctly - jane should now be max's mentor.
        max = repositoryService.getEntityDefinition(maxGuid);
        refTarget = (ITypedReferenceableInstance) max.get("mentor");
        Assert.assertEquals(refTarget.getId()._getId(), janeGuid._getId());

        // Verify modification timestamp was updated.
        vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long modificationTimestampPost2ndUpdate = vertex.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);
        Assert.assertNotNull(modificationTimestampPost2ndUpdate);
        Assert.assertTrue(modificationTimestampPostUpdate < modificationTimestampPost2ndUpdate);

        ITypedReferenceableInstance julius = repositoryService.getEntityDefinition(nameGuidMap.get("Julius"));
        Id juliusGuid = julius.getId();
        maxEntity = personType.createInstance(max.getId());
        maxEntity.set("manager", juliusGuid);
        updatePartial(maxEntity);

        // Verify the update was applied correctly - julius should now be max's manager.
        max = repositoryService.getEntityDefinition(maxGuid);
        refTarget = (ITypedReferenceableInstance) max.get("manager");
        Assert.assertEquals(refTarget.getId()._getId(), juliusGuid._getId());

        assertTestUpdateEntity_MultiplicityOneNonCompositeReference(janeGuid._getId());
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

        List<String> deletedEntities = deleteEntities(maxGuid);
        Assert.assertTrue(deletedEntities.contains(maxGuid));
        assertEntityDeleted(maxGuid);

        // Verify that the Department.employees reference to the deleted employee
        // was disconnected.
        hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        refValue = hrDept.get("employees");
        Assert.assertTrue(refValue instanceof List);
        List<Object> employees = (List<Object>)refValue;
        Assert.assertEquals(employees.size(), 3);
        for (Object listValue : employees) {
            Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
            ITypedReferenceableInstance employee = (ITypedReferenceableInstance) listValue;
            Assert.assertNotEquals(employee.getId()._getId(), maxGuid);
        }

        // Verify that max's Person.mentor unidirectional reference to john was disconnected.
        ITypedReferenceableInstance john = repositoryService.getEntityDefinition(johnGuid);
        refValue = john.get("mentor");
        Assert.assertNull(refValue);

        assertTestDisconnectBidirectionalReferences(janeGuid);

        // Now delete jane - this should disconnect the manager reference from her
        // subordinate.
        deletedEntities = deleteEntities(janeGuid);
        Assert.assertTrue(deletedEntities.contains(janeGuid));
        assertEntityDeleted(janeGuid);

        john = repositoryService.getEntityDefinition(johnGuid);
        Assert.assertNull(john.get("manager"));
    }

    protected abstract void assertTestDisconnectBidirectionalReferences(String janeGuid) throws Exception;

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
        Object refValues = table.get("columns");
        Assert.assertTrue(refValues instanceof List);
        List<Object> refList = (List<Object>) refValues;
        Assert.assertEquals(refList.size(), 5);
        Assert.assertTrue(refList.get(0) instanceof ITypedReferenceableInstance);
        ITypedReferenceableInstance column = (ITypedReferenceableInstance) refList.get(0);
        String columnGuid = column.getId()._getId();

        // Delete the column.
        List<String> deletedEntities = deleteEntities(columnGuid);
        Assert.assertTrue(deletedEntities.contains(columnGuid));
        assertEntityDeleted(columnGuid);

        // Verify table.columns reference to the deleted column has been disconnected.
        table = repositoryService.getEntityDefinition(tableGuid);
        refList = (List<Object>) table.get("columns");
        Assert.assertEquals(refList.size(), 4);
        for (Object refValue : refList) {
            Assert.assertTrue(refValue instanceof ITypedReferenceableInstance);
            column = (ITypedReferenceableInstance)refValue;
            Assert.assertFalse(column.getId()._getId().equals(columnGuid));
        }
    }

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
        Referenceable structInstance = new Referenceable("TestStruct");
        Referenceable nestedStructInstance = new Referenceable("NestedStruct");
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
            structTargetConvertedEntity, traitTargetConvertedEntity, structContainerConvertedEntity);
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
        List<String> deletedEntities = deleteEntities(structTargetGuid, traitTargetGuid);
        assertEntityDeleted(structTargetGuid);
        assertEntityDeleted(traitTargetGuid);
        Assert.assertEquals(deletedEntities.size(), 2);
        Assert.assertTrue(deletedEntities.containsAll(Arrays.asList(structTargetGuid, traitTargetGuid)));

        assertTestDisconnectUnidirectionalArrayReferenceFromStructAndTraitTypes(structContainerGuid);

        // Delete the entity which contains nested structs and has the TestTrait trait.
        deletedEntities = deleteEntities(structContainerGuid);
        assertEntityDeleted(structContainerGuid);
        Assert.assertEquals(deletedEntities.size(), 1);
        Assert.assertTrue(deletedEntities.contains(structContainerGuid));

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
        List<String> createEntitiesResult = repositoryService.createEntities(mapOwnerInstance, mapValueInstance);
        Assert.assertEquals(createEntitiesResult.size(), 2);
        List<String> guids = repositoryService.getEntityList("MapOwner");
        Assert.assertEquals(guids.size(), 1);
        String mapOwnerGuid = guids.get(0);

        String edgeLabel = GraphHelper.getEdgeLabel(mapOwnerType, mapOwnerType.fieldMapping.fields.get("map"));
        String mapEntryLabel = edgeLabel + "." + "value1";
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(mapEntryLabel);

        // Verify MapOwner.map attribute has expected value.
        String mapValueGuid = null;
        Vertex mapOwnerVertex = null;
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
            object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey());
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
        List<String> createEntitiesResult = repositoryService.createEntities(mapOwnerInstance, mapValueInstance);
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
        Vertex mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey());
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

    private String createHrDeptGraph() throws Exception {
        ITypedReferenceableInstance hrDept = TestUtils.createDeptEg1(typeSystem);

        List<String> guids = repositoryService.createEntities(hrDept);
        Assert.assertNotNull(guids);
        Assert.assertEquals(guids.size(), 5);

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

    protected List<Vertex> getVertices(String propertyName, Object value) {
        Iterable<Vertex> vertices = graphProvider.get().getVertices(propertyName, value);
        List<Vertex> list = new ArrayList<>();
        for (Vertex vertex : vertices) {
            list.add(vertex);
        }
        return list;
    }

    private Map<String, String> getEmployeeNameGuidMap(ITypedReferenceableInstance hrDept) throws AtlasException {

        Object refValue = hrDept.get("employees");
        Assert.assertTrue(refValue instanceof List);
        List<Object> employees = (List<Object>)refValue;
        Assert.assertEquals(employees.size(), 4);
        Map<String, String> nameGuidMap = new HashMap<String, String>();

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
