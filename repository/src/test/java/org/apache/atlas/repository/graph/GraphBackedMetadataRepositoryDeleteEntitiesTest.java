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

import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
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
import org.apache.atlas.typesystem.types.TypeUtils.Pair;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test for GraphBackedMetadataRepository.deleteEntities
 *
 * Guice loads the dependencies and injects the necessary objects
 *
 */
@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedMetadataRepositoryDeleteEntitiesTest {

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @Inject
    private GraphBackedMetadataRepository repositoryService;

    @Inject
    private GraphBackedDiscoveryService discoveryService;

    private TypeSystem typeSystem;

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        new GraphBackedSearchIndexer(graphProvider);

        TestUtils.defineDeptEmployeeTypes(typeSystem);
        TestUtils.createHiveTypes(typeSystem);
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



    /**
     * Verify deleting entities with composite references to other entities.
     * The composite entities should also be deleted.
     */
    @Test
    public void testDeleteEntitiesWithCompositeArrayReference() throws Exception {
        String hrDeptGuid = createHrDeptGraph();

        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Object refValue = hrDept.get("employees");
        Assert.assertTrue(refValue instanceof List);
        List<Object> employees = (List<Object>)refValue;
        Assert.assertEquals(employees.size(), 4);

        List<String> employeeGuids = new ArrayList(4);
        for (Object listValue : employees) {
            Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
            ITypedReferenceableInstance employee = (ITypedReferenceableInstance) listValue;
            employeeGuids.add(employee.getId()._getId());
        }
        
        // There should be 4 vertices for Address structs (one for each Person.address attribute value).
        int vertexCount = countVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "Address");
        Assert.assertEquals(vertexCount, 4);
        vertexCount = countVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "SecurityClearance");
        Assert.assertEquals(vertexCount, 1);
        
        Pair<List<String>, List<ITypedReferenceableInstance>> deletedEntities = repositoryService.deleteEntities(Arrays.asList(hrDeptGuid));
        Assert.assertTrue(deletedEntities.left.contains(hrDeptGuid));
        
        // Verify Department entity and its contained Person entities were deleted.
        verifyEntityDoesNotExist(hrDeptGuid);
        for (String employeeGuid : employeeGuids) {
            verifyEntityDoesNotExist(employeeGuid);
        }

        // Verify all Person.address struct vertices were removed.
        vertexCount = countVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "Address");
        Assert.assertEquals(vertexCount, 0);
        
        // Verify all SecurityClearance trait vertices were removed.
        vertexCount = countVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "SecurityClearance");
        Assert.assertEquals(vertexCount, 0);
    }

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
        
        Pair<List<String>, List<ITypedReferenceableInstance>> deleteEntitiesResult = 
            repositoryService.deleteEntities(Arrays.asList(mapOwnerGuid));
        Assert.assertEquals(deleteEntitiesResult.left.size(), 2);
        Assert.assertTrue(deleteEntitiesResult.left.containsAll(guids));
        verifyEntityDoesNotExist(mapOwnerGuid);
        verifyEntityDoesNotExist(mapValueGuid);
    }
    
    /**
     * Verify deleting an entity which is contained by another
     * entity through a bi-directional composite reference.
     * 
     * @throws Exception
     */
    @Test(dependsOnMethods = "testDeleteEntitiesWithCompositeArrayReference")
    public void testDisconnectBidirectionalReferences() throws Exception {
        String hrDeptGuid = createHrDeptGraph();
        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Object refValue = hrDept.get("employees");
        Assert.assertTrue(refValue instanceof List);
        List<Object> employees = (List<Object>)refValue;
        Assert.assertEquals(employees.size(), 4);
        String employeeGuid = null;
        for (Object listValue : employees) {
            Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
            ITypedReferenceableInstance employee = (ITypedReferenceableInstance) listValue;
            if (employee.get("name").equals("Max")) {
                employeeGuid = employee.getId()._getId();
            }
        }
        Assert.assertNotNull(employeeGuid);
        
        // Verify that Max is one of Jane's subordinates.
        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition("Manager", "name", "Jane");
        refValue = jane.get("subordinates");
        Assert.assertTrue(refValue instanceof List);
        List<Object> subordinates = (List<Object>)refValue;
        Assert.assertEquals(subordinates.size(), 2);
        List<String> subordinateIds = new ArrayList<>(2);
        for (Object listValue : employees) {
            Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
            ITypedReferenceableInstance employee = (ITypedReferenceableInstance) listValue;
            subordinateIds.add(employee.getId()._getId());
        }
        Assert.assertTrue(subordinateIds.contains(employeeGuid));
        
        Pair<List<String>, List<ITypedReferenceableInstance>> deletedEntities = repositoryService.deleteEntities(Arrays.asList(employeeGuid));
        Assert.assertTrue(deletedEntities.left.contains(employeeGuid));
        verifyEntityDoesNotExist(employeeGuid);
        
        // Verify that the Department.employees reference to the deleted employee
        // was disconnected.
        hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        refValue = hrDept.get("employees");
        Assert.assertTrue(refValue instanceof List);
        employees = (List<Object>)refValue;
        Assert.assertEquals(employees.size(), 3);
        for (Object listValue : employees) {
            Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
            ITypedReferenceableInstance employee = (ITypedReferenceableInstance) listValue;
            Assert.assertNotEquals(employee.getId()._getId(), employeeGuid);
        }
        
        // Verify that the Manager.subordinates reference to the deleted employee
        // Max was disconnected.
        jane = repositoryService.getEntityDefinition("Manager", "name", "Jane");
        refValue = jane.get("subordinates");
        Assert.assertTrue(refValue instanceof List);
        subordinates = (List<Object>)refValue;
        Assert.assertEquals(subordinates.size(), 1);
        Object listValue = subordinates.get(0);
        Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
        ITypedReferenceableInstance subordinate = (ITypedReferenceableInstance) listValue;
        String subordinateGuid = subordinate.getId()._getId();
        Assert.assertNotEquals(subordinateGuid, employeeGuid);
        
        // Verify that max's Person.mentor unidirectional reference to john was disconnected.
        ITypedReferenceableInstance john = repositoryService.getEntityDefinition("Manager", "name", "John");
        refValue = john.get("mentor");
        Assert.assertNull(refValue);
        
        // Now delete jane - this should disconnect the manager reference from her
        // subordinate.
        String janeGuid = jane.getId()._getId();
        deletedEntities = repositoryService.deleteEntities(Arrays.asList(janeGuid));
        Assert.assertTrue(deletedEntities.left.contains(janeGuid));
        verifyEntityDoesNotExist(janeGuid);
        subordinate = repositoryService.getEntityDefinition(subordinateGuid);
        Assert.assertNull(subordinate.get("manager"));
    }

    /**
     * Verify deleting entity that is the target of a unidirectional class array reference
     * from a class instance.
     */
    @Test
    public void testDisconnectUnidirectionalArrayReferenceFromClassType() throws Exception {
        createDbTableGraph();
        
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
        Pair<List<String>, List<ITypedReferenceableInstance>> deletedEntities = 
            repositoryService.deleteEntities(Arrays.asList(columnGuid));
        Assert.assertEquals(deletedEntities.left.size(), 1);
        Assert.assertEquals(deletedEntities.right.size(), 1);
        verifyEntityDoesNotExist(columnGuid);
        
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
        Pair<List<String>, List<ITypedReferenceableInstance>> deleteEntitiesResult = 
            repositoryService.deleteEntities(Arrays.asList(structTargetGuid, traitTargetGuid));
        verifyEntityDoesNotExist(structTargetGuid);
        verifyEntityDoesNotExist(traitTargetGuid);
        Assert.assertEquals(deleteEntitiesResult.left.size(), 2);
        Assert.assertTrue(deleteEntitiesResult.left.containsAll(Arrays.asList(structTargetGuid, traitTargetGuid)));
        
        // Verify that the unidirectional references from the struct and trait instances
        // to the deleted entities were disconnected.
        structContainerConvertedEntity = repositoryService.getEntityDefinition(structContainerGuid);
        object = structContainerConvertedEntity.get("struct");
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof ITypedStruct);
        struct = (ITypedStruct) object;
        Assert.assertNull(struct.get("target"));
        trait = structContainerConvertedEntity.getTrait("TestTrait");
        Assert.assertNotNull(trait);
        Assert.assertNull(trait.get("target"));
        
        // Delete the entity which contains nested structs and has the TestTrait trait.
        deleteEntitiesResult = 
            repositoryService.deleteEntities(Arrays.asList(structContainerGuid));
        verifyEntityDoesNotExist(structContainerGuid);
        Assert.assertEquals(deleteEntitiesResult.left.size(), 1);
        Assert.assertTrue(deleteEntitiesResult.left.contains(structContainerGuid));
        
        // Verify all TestStruct struct vertices were removed.
        int vertexCount = countVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "TestStruct");
        Assert.assertEquals(vertexCount, 0);
        
        // Verify all NestedStruct struct vertices were removed.
        vertexCount = countVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "NestedStruct");
        Assert.assertEquals(vertexCount, 0);

        // Verify all TestTrait trait vertices were removed.
        vertexCount = countVertices(Constants.ENTITY_TYPE_PROPERTY_KEY, "TestTrait");
        Assert.assertEquals(vertexCount, 0);
    }
    
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
        edgeLabel = GraphHelper.getEdgeLabel(mapOwnerType, mapOwnerType.fieldMapping.fields.get("biMap"));
        mapEntryLabel = edgeLabel + "." + "value1";
        AtlasEdgeLabel biMapAtlasEdgeLabel = new AtlasEdgeLabel(mapEntryLabel);
        
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
        Pair<List<String>, List<ITypedReferenceableInstance>> deleteEntitiesResult = 
            repositoryService.deleteEntities(Arrays.asList(mapValueGuid));
        verifyEntityDoesNotExist(mapValueGuid);
        
        // Verify map references from mapOwner were disconnected.
        mapOwnerInstance = repositoryService.getEntityDefinition(mapOwnerGuid);
        Assert.assertNull(mapOwnerInstance.get("map"));
        Assert.assertNull(mapOwnerInstance.get("biMap"));
        mapOwnerVertex = GraphHelper.getInstance().getVertexForGUID(mapOwnerGuid);
        Object object = mapOwnerVertex.getProperty(atlasEdgeLabel.getQualifiedMapKey());
        Assert.assertNull(object);
        object = mapOwnerVertex.getProperty(biMapAtlasEdgeLabel.getQualifiedMapKey());
        Assert.assertNull(object);
    }
    
    private String createHrDeptGraph() throws Exception {
        Referenceable deptEg1 = TestUtils.createDeptEg1(typeSystem);
        ClassType deptType = typeSystem.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(deptEg1, Multiplicity.REQUIRED);

        List<String> guids = repositoryService.createEntities(hrDept2);
        Assert.assertNotNull(guids);
        Assert.assertEquals(guids.size(), 5);

        List<String> entityList = repositoryService.getEntityList("Department");
        Assert.assertNotNull(entityList);
        Assert.assertEquals(entityList.size(), 1);
        return entityList.get(0);
    }
    
    private void createDbTableGraph() throws Exception {
        Referenceable databaseInstance = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance.set("name", TestUtils.DATABASE_NAME);
        databaseInstance.set("description", "foo database");
 
        ClassType dbType = typeSystem.getDataType(ClassType.class, TestUtils.DATABASE_TYPE);
        ITypedReferenceableInstance db = dbType.convert(databaseInstance, Multiplicity.REQUIRED);
        Referenceable tableInstance = new Referenceable(TestUtils.TABLE_TYPE, TestUtils.CLASSIFICATION);
        tableInstance.set("name", TestUtils.TABLE_NAME);
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
    
    private int countVertices(String propertyName, Object value) {
        Iterable<Vertex> vertices = graphProvider.get().getVertices(propertyName, value);
        int vertexCount = 0;
        for (Vertex vertex : vertices) {
            vertexCount++;
        }
        return vertexCount;
    }
    
    private void verifyEntityDoesNotExist(String guid) throws RepositoryException {
        try {
            repositoryService.getEntityDefinition(guid);
            Assert.fail("EntityNotFoundException was expected but none thrown");
        } catch(EntityNotFoundException e) {
            // good
        }
    }

}
