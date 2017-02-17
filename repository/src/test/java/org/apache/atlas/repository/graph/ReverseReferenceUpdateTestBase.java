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
package org.apache.atlas.repository.graph;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.atlas.CreateUpdateEntitiesResult;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

/**
 * Verifies automatic update of reverse references
 *
 */
@Guice(modules = RepositoryMetadataModule.class)
public abstract class ReverseReferenceUpdateTestBase {

    @Inject
    MetadataRepository repositoryService;

    private TypeSystem typeSystem;

    protected ClassType typeA;
    protected ClassType typeB;

    abstract DeleteHandler getDeleteHandler(TypeSystem typeSystem);

    abstract void assertTestOneToOneReference(Object actual, ITypedReferenceableInstance expectedValue, ITypedReferenceableInstance referencingInstance) throws Exception;
    abstract void assertTestOneToManyReference(Object refValue, ITypedReferenceableInstance referencingInstance) throws Exception;

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        new GraphBackedSearchIndexer(new AtlasTypeRegistry());

        HierarchicalTypeDefinition<ClassType> aDef = TypesUtil.createClassTypeDef("A", ImmutableSet.<String>of(),
            TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
            new AttributeDefinition("b", "B", Multiplicity.OPTIONAL, false, "a"), // 1-1
            new AttributeDefinition("oneB", "B", Multiplicity.OPTIONAL, false, "manyA"), // 1-*
            new AttributeDefinition("manyB", DataTypes.arrayTypeName("B"), Multiplicity.OPTIONAL,  false, "manyToManyA"), // *-*
            new AttributeDefinition("map", DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                "B"), Multiplicity.OPTIONAL, false, "backToMap"));
        HierarchicalTypeDefinition<ClassType> bDef = TypesUtil.createClassTypeDef("B", ImmutableSet.<String>of(),
            TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
            new AttributeDefinition("a", "A", Multiplicity.OPTIONAL, false, "b"),
            new AttributeDefinition("manyA", DataTypes.arrayTypeName("A"), Multiplicity.OPTIONAL, false, "oneB"),
            new AttributeDefinition("manyToManyA", DataTypes.arrayTypeName("A"), Multiplicity.OPTIONAL, false, "manyB"),
            new AttributeDefinition("backToMap", "A", Multiplicity.OPTIONAL, false, "map"));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
            ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
            ImmutableList.of(aDef, bDef));
        typeSystem.defineTypes(typesDef);
        typeA = typeSystem.getDataType(ClassType.class, "A");
        typeB = typeSystem.getDataType(ClassType.class, "B");

        repositoryService = new GraphBackedMetadataRepository(getDeleteHandler(typeSystem));
        repositoryService = TestUtils.addTransactionWrapper(repositoryService);
    }

    @BeforeMethod
    public void setupContext() {
        TestUtils.resetRequestContext();
    }

    @Test
    public void testOneToOneReference() throws Exception {
        ITypedReferenceableInstance a = typeA.createInstance();
        a.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance b1 = typeB.createInstance();
        b1.setString("name", TestUtils.randomString());
        a.set("b", b1);
        // Create a.  This should also create b1 and set the reverse b1->a reference.
        repositoryService.createEntities(a);
        a = repositoryService.getEntityDefinition("A", "name", a.getString("name"));
        b1 = repositoryService.getEntityDefinition("B", "name", b1.getString("name"));
        Object object = a.get("b");
        Assert.assertTrue(object instanceof ITypedReferenceableInstance);
        ITypedReferenceableInstance refValue = (ITypedReferenceableInstance) object;
        Assert.assertEquals(refValue.getId()._getId(), b1.getId()._getId());
        object = b1.get("a");
        Assert.assertTrue(object instanceof ITypedReferenceableInstance);
        refValue = (ITypedReferenceableInstance) object;
        Assert.assertEquals(refValue.getId()._getId(), a.getId()._getId());

        ITypedReferenceableInstance b2 = typeB.createInstance();
        b2.setString("name", TestUtils.randomString());
        b2.set("a", a.getId());
        // Create b2.  This should set the reverse a->b2 reference
        // and disconnect b1->a.
        repositoryService.createEntities(b2);
        a = repositoryService.getEntityDefinition(a.getId()._getId());
        b2 = repositoryService.getEntityDefinition("B", "name", b2.getString("name"));
        object = a.get("b");
        Assert.assertTrue(object instanceof ITypedReferenceableInstance);
        refValue = (ITypedReferenceableInstance) object;
        Assert.assertEquals(refValue.getId()._getId(), b2.getId()._getId());
        object = b2.get("a");
        Assert.assertTrue(object instanceof ITypedReferenceableInstance);
        refValue = (ITypedReferenceableInstance) object;
        Assert.assertEquals(refValue.getId()._getId(), a.getId()._getId());
        // Verify b1->a was disconnected.
        b1 = repositoryService.getEntityDefinition("B", "name", b1.getString("name"));
        object = b1.get("a");
        assertTestOneToOneReference(object, a, b1);
    }

    @Test
    public void testOneToManyReference() throws Exception {
        ITypedReferenceableInstance a1 = typeA.createInstance();
        a1.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance a2 = typeA.createInstance();
        a2.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance b1 = typeB.createInstance();
        b1.setString("name", TestUtils.randomString());
        a1.set("oneB", b1);
        ITypedReferenceableInstance b2 = typeB.createInstance();
        b2.setString("name", TestUtils.randomString());
        repositoryService.createEntities(a1, a2, b2);
        a1 = repositoryService.getEntityDefinition("A", "name", a1.getString("name"));
        a2 = repositoryService.getEntityDefinition("A", "name", a2.getString("name"));
        b1 = repositoryService.getEntityDefinition("B", "name", b1.getString("name"));
        b2 = repositoryService.getEntityDefinition("B", "name", b2.getString("name"));
        Object object = b1.get("manyA");
        Assert.assertTrue(object instanceof List);
        List<ITypedReferenceableInstance> refValues = (List<ITypedReferenceableInstance>) object;
        Assert.assertEquals(refValues.size(), 1);
        Assert.assertTrue(refValues.contains(a1.getId()));

        a2.set("oneB", b1.getId());
        repositoryService.updateEntities(a2);
        b1 = repositoryService.getEntityDefinition(b1.getId()._getId());
        object = b1.get("manyA");
        Assert.assertTrue(object instanceof List);
        refValues = (List<ITypedReferenceableInstance>) object;
        Assert.assertEquals(refValues.size(), 2);
        Assert.assertTrue(refValues.containsAll(Arrays.asList(a1.getId(), a2.getId())));

        b2.set("manyA", Collections.singletonList(a2));
        repositoryService.updateEntities(b2);
        a2 = repositoryService.getEntityDefinition("A", "name", a2.getString("name"));

        // Verify reverse a2.oneB reference was set to b2.
        object = a2.get("oneB");
        Assert.assertTrue(object instanceof ITypedReferenceableInstance);
        ITypedReferenceableInstance refValue = (ITypedReferenceableInstance) object;
        Assert.assertEquals(refValue.getId()._getId(), b2.getId()._getId());

        // Verify a2 was removed from b1.manyA reference list.
        b1 = repositoryService.getEntityDefinition(b1.getId()._getId());
        object = b1.get("manyA");
        assertTestOneToManyReference(object, b1);
    }

    @Test
    public void testManyToManyReference() throws Exception {
        ITypedReferenceableInstance a1 = typeA.createInstance();
        a1.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance a2 = typeA.createInstance();
        a2.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance b1 = typeB.createInstance();
        b1.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance b2 = typeB.createInstance();
        b2.setString("name", TestUtils.randomString());
        repositoryService.createEntities(a1, a2, b1, b2);
        a1 = repositoryService.getEntityDefinition("A", "name", a1.getString("name"));
        a2 = repositoryService.getEntityDefinition("A", "name", a2.getString("name"));
        b1 = repositoryService.getEntityDefinition("B", "name", b1.getString("name"));
        b2 = repositoryService.getEntityDefinition("B", "name", b2.getString("name"));

        // Update a1 to add b1 to its manyB reference.
        // This should update b1.manyToManyA.
        a1.set("manyB", Arrays.asList(b1.getId()));
        repositoryService.updateEntities(a1);

        // Verify reverse b1.manyToManyA reference was updated.
        b1 = repositoryService.getEntityDefinition(b1.getId()._getId());
        Object object = b1.get("manyToManyA");
        Assert.assertTrue(object instanceof List);
        List<ITypedReferenceableInstance> refValues = (List<ITypedReferenceableInstance>) object;
        Assert.assertEquals(refValues.size(), 1);
        Assert.assertTrue(refValues.contains(a1.getId()));
    }

    /**
     * Auto-update of bi-directional references where one end is a map reference is
     * not currently supported.  Verify that the auto-update is not applied in this case.
     */
    @Test
    public void testMapReference() throws Exception {
        ITypedReferenceableInstance a1 = typeA.createInstance();
        a1.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance a2 = typeA.createInstance();
        a2.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance b1 = typeB.createInstance();
        b1.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance b2 = typeB.createInstance();
        b2.setString("name", TestUtils.randomString());
        repositoryService.createEntities(a1, a2, b1, b2);
        a1 = repositoryService.getEntityDefinition("A", "name", a1.getString("name"));
        a2 = repositoryService.getEntityDefinition("A", "name", a2.getString("name"));
        b1 = repositoryService.getEntityDefinition("B", "name", b1.getString("name"));
        b2 = repositoryService.getEntityDefinition("B", "name", b2.getString("name"));
        a1.set("map", Collections.singletonMap("b1", b1));
        repositoryService.updateEntities(a1);
        // Verify reverse b1.manyToManyA reference was not updated.
        b1 = repositoryService.getEntityDefinition(b1.getId()._getId());
        Object object = b1.get("backToMap");
        Assert.assertNull(object);
    }

    /**
     * Verify that explicitly setting both ends of a reference
     * does not cause duplicate entries due to auto-update of
     * reverse reference.
     */
    @Test
    public void testCallerHasSetBothEnds() throws Exception {
        ITypedReferenceableInstance a = typeA.createInstance();
        a.setString("name", TestUtils.randomString());
        ITypedReferenceableInstance b1 = typeB.createInstance();
        b1.setString("name", TestUtils.randomString());
        // Set both sides of the reference.
        a.set("oneB", b1);
        b1.set("manyA", Collections.singletonList(a));

        CreateUpdateEntitiesResult result = repositoryService.createEntities(a);
        Map<String, String> guidAssignments = result.getGuidMapping().getGuidAssignments();
        String aGuid = a.getId()._getId();
        String b1Guid = guidAssignments.get(b1.getId()._getId());

        a = repositoryService.getEntityDefinition(aGuid);
        Object object = a.get("oneB");
        Assert.assertTrue(object instanceof ITypedReferenceableInstance);
        Assert.assertEquals(((ITypedReferenceableInstance)object).getId()._getId(), b1Guid);

        b1 = repositoryService.getEntityDefinition(b1Guid);
        object = b1.get("manyA");
        Assert.assertTrue(object instanceof List);
        List<ITypedReferenceableInstance> refValues = (List<ITypedReferenceableInstance>)object;
        Assert.assertEquals(refValues.size(), 1);
        Assert.assertEquals(refValues.get(0).getId()._getId(), aGuid);
    }
}
