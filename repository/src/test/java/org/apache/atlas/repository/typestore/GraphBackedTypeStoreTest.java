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

package org.apache.atlas.repository.typestore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasException;
import org.apache.atlas.TestOnlyModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.*;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createStructTypeDef;

@Guice(modules = TestOnlyModule.class)
public class GraphBackedTypeStoreTest {
    
    private static final String DESCRIPTION = "_description";

    @Inject
    private ITypeStore typeStore;

    private TypeSystem ts;

    @BeforeClass
    public void setUp() throws Exception {
        ts = TypeSystem.getInstance();
        ts.reset();
        TestUtils.defineDeptEmployeeTypes(ts);
    }

    @AfterClass
    public void tearDown() throws Exception {
        ts.reset();
        AtlasGraphProvider.cleanup();
    }


    @Test
    public void testStore() throws AtlasException {
        ImmutableList<String> typeNames = ts.getTypeNames();
        typeStore.store(ts, typeNames);
        dumpGraph();
    }

    @Test(dependsOnMethods = "testStore")
    public void testRestoreType() throws Exception {
        TypesDef typesDef = typeStore.restoreType("Manager");
        verifyRestoredClassType(typesDef, "Manager");
    }

    private void dumpGraph() {
        AtlasGraph<?, ?> graph = TestUtils.getGraph();
        for (AtlasVertex<?,?> v : graph.getVertices()) {
            System.out.println("****v = " + GraphHelper.vertexString(v));
            for (AtlasEdge<?,?> e : v.getEdges(AtlasEdgeDirection.OUT)) {
                System.out.println("****e = " + GraphHelper.edgeString(e));
            }
        }
    }

    @Test(dependsOnMethods = "testStore")
    public void testRestore() throws Exception {
        TypesDef types = typeStore.restore();

        //validate enum
        List<EnumTypeDefinition> enumTypes = types.enumTypesAsJavaList();
        Assert.assertEquals(1, enumTypes.size());
        EnumTypeDefinition orgLevel = enumTypes.get(0);
        Assert.assertEquals(orgLevel.name, "OrgLevel");
        Assert.assertEquals(orgLevel.description, "OrgLevel"+DESCRIPTION);
        Assert.assertEquals(orgLevel.enumValues.length, 2);
        EnumValue enumValue = orgLevel.enumValues[0];
        Assert.assertEquals(enumValue.value, "L1");
        Assert.assertEquals(enumValue.ordinal, 1);

        //validate class
        List<StructTypeDefinition> structTypes = types.structTypesAsJavaList();
        Assert.assertEquals(1, structTypes.size());

        verifyRestoredClassType(types, "Manager");

        //validate trait
        List<HierarchicalTypeDefinition<TraitType>> traitTypes = types.traitTypesAsJavaList();
        Assert.assertEquals(1, traitTypes.size());
        HierarchicalTypeDefinition<TraitType> trait = traitTypes.get(0);
        Assert.assertEquals("SecurityClearance", trait.typeName);
        Assert.assertEquals(trait.typeName+DESCRIPTION, trait.typeDescription);
        Assert.assertEquals(1, trait.attributeDefinitions.length);
        AttributeDefinition attribute = trait.attributeDefinitions[0];
        Assert.assertEquals("level", attribute.name);
        Assert.assertEquals(DataTypes.INT_TYPE.getName(), attribute.dataTypeName);

        //validate the new types
        ts.reset();
        ts.defineTypes(types);
    }

    @Test
    public void testTypeWithSpecialChars() throws AtlasException {
        HierarchicalTypeDefinition<ClassType> specialTypeDef1 = createClassTypeDef("SpecialTypeDef1", "Typedef with special character",
                ImmutableSet.<String>of(), createRequiredAttrDef("attribute$", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> specialTypeDef2 = createClassTypeDef("SpecialTypeDef2", "Typedef with special character",
                ImmutableSet.<String>of(), createRequiredAttrDef("attribute%", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> specialTypeDef3 = createClassTypeDef("SpecialTypeDef3", "Typedef with special character",
                ImmutableSet.<String>of(), createRequiredAttrDef("attribute{", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> specialTypeDef4 = createClassTypeDef("SpecialTypeDef4", "Typedef with special character",
                ImmutableSet.<String>of(), createRequiredAttrDef("attribute}", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> specialTypeDef5 = createClassTypeDef("SpecialTypeDef5", "Typedef with special character",
                ImmutableSet.<String>of(), createRequiredAttrDef("attribute$%{}", DataTypes.STRING_TYPE));

        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(specialTypeDef1, specialTypeDef2, specialTypeDef3, specialTypeDef4, specialTypeDef5));

        Map<String, IDataType> createdTypes = ts.defineTypes(typesDef);
        typeStore.store(ts, ImmutableList.copyOf(createdTypes.keySet()));

        //Validate the updated types
        TypesDef types = typeStore.restore();
        ts.reset();
        ts.defineTypes(types);
    }

    @Test(dependsOnMethods = "testStore")
    public void testTypeUpdate() throws Exception {
        //Add enum value
        String _description = "_description_updated";
        EnumTypeDefinition orgLevelEnum = new EnumTypeDefinition("OrgLevel", "OrgLevel"+_description, new EnumValue("L1", 1),
                new EnumValue("L2", 2), new EnumValue("L3", 3));

        //Add attribute
        StructTypeDefinition addressDetails =
                createStructTypeDef("Address", createRequiredAttrDef("street", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("city", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("state", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef("Department", "Department"+_description,
            ImmutableSet.<String>of(), createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("employees", String.format("array<%s>", "Person"), Multiplicity.OPTIONAL,
                        true, "department"),
                new AttributeDefinition("positions", String.format("map<%s,%s>", DataTypes.STRING_TYPE.getName(), "Person"), Multiplicity.OPTIONAL, false, null));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.of(orgLevelEnum), ImmutableList.of(addressDetails),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(deptTypeDef));

        Map<String, IDataType> typesAdded = ts.updateTypes(typesDef);
        typeStore.store(ts, ImmutableList.copyOf(typesAdded.keySet()));

        verifyEdges();
        
        //Validate the updated types
        TypesDef types = typeStore.restore();
        ts.reset();
        ts.defineTypes(types);

        //Assert new enum value
        EnumType orgLevel = ts.getDataType(EnumType.class, orgLevelEnum.name);
        Assert.assertEquals(orgLevel.name, orgLevelEnum.name);
        Assert.assertEquals(orgLevel.description, orgLevelEnum.description);
        Assert.assertEquals(orgLevel.values().size(), orgLevelEnum.enumValues.length);
        Assert.assertEquals(orgLevel.fromValue("L3").ordinal, 3);

        //Assert new attribute
        StructType addressType = ts.getDataType(StructType.class, addressDetails.typeName);
        Assert.assertEquals(addressType.numFields, 3);
        Assert.assertEquals(addressType.fieldMapping.fields.get("state").dataType(), DataTypes.STRING_TYPE);

        //Updating the definition again shouldn't add another edge
        typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(deptTypeDef));
        typesAdded = ts.updateTypes(typesDef);
        typeStore.store(ts, ImmutableList.copyOf(typesAdded.keySet()));
        verifyEdges();
    }

    private void verifyEdges() throws RepositoryException {
        // ATLAS-474: verify that type update did not write duplicate edges to the type store.
        if (typeStore instanceof GraphBackedTypeStore) {
            GraphBackedTypeStore gbTypeStore = (GraphBackedTypeStore) typeStore;
            AtlasVertex typeVertex = gbTypeStore.findVertices(Collections.singletonList("Department")).get("Department");
            int edgeCount = countOutgoingEdges(typeVertex, gbTypeStore.getEdgeLabel("Department", "employees"));
            Assert.assertEquals(edgeCount, 1, "Should only be 1 edge for employees attribute on Department type AtlasVertex");
        }
    }

    private int countOutgoingEdges(AtlasVertex typeVertex, String edgeLabel) {

        Iterator<AtlasEdge> outGoingEdgesByLabel = GraphHelper.getInstance().getOutGoingEdgesByLabel(typeVertex, edgeLabel);
        int edgeCount = 0;
        for (; outGoingEdgesByLabel.hasNext();) {
            outGoingEdgesByLabel.next();
            edgeCount++;
        }
        return edgeCount;
    }

    private void verifyRestoredClassType(TypesDef types, String typeName) throws AtlasException {
        boolean clsTypeFound = false;
        List<HierarchicalTypeDefinition<ClassType>> classTypes = types.classTypesAsJavaList();
        for (HierarchicalTypeDefinition<ClassType> classType : classTypes) {
            if (classType.typeName.equals(typeName)) {
                ClassType expectedType = ts.getDataType(ClassType.class, classType.typeName);
                Assert.assertEquals(expectedType.immediateAttrs.size(), classType.attributeDefinitions.length);
                Assert.assertEquals(expectedType.superTypes.size(), classType.superTypes.size());
                Assert.assertEquals(classType.typeDescription, classType.typeName+DESCRIPTION);
                clsTypeFound = true;
            }
        }
        Assert.assertTrue(clsTypeFound, typeName + " type not restored");
    }
}
