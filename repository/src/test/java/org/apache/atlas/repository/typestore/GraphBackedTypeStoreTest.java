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
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.List;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createStructTypeDef;

@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedTypeStoreTest {
    @Inject
    private GraphProvider<TitanGraph> graphProvider;

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
        graphProvider.get().shutdown();
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStore() throws AtlasException {
        typeStore.store(ts);
        dumpGraph();
    }

    private void dumpGraph() {
        TitanGraph graph = graphProvider.get();
        for (Vertex v : graph.getVertices()) {
            System.out.println("****v = " + GraphHelper.vertexString(v));
            for (Edge e : v.getEdges(Direction.OUT)) {
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
        Assert.assertEquals(orgLevel.enumValues.length, 2);
        EnumValue enumValue = orgLevel.enumValues[0];
        Assert.assertEquals(enumValue.value, "L1");
        Assert.assertEquals(enumValue.ordinal, 1);

        //validate class
        List<StructTypeDefinition> structTypes = types.structTypesAsJavaList();
        Assert.assertEquals(1, structTypes.size());

        boolean clsTypeFound = false;
        List<HierarchicalTypeDefinition<ClassType>> classTypes = types.classTypesAsJavaList();
        for (HierarchicalTypeDefinition<ClassType> classType : classTypes) {
            if (classType.typeName.equals("Manager")) {
                ClassType expectedType = ts.getDataType(ClassType.class, classType.typeName);
                Assert.assertEquals(expectedType.immediateAttrs.size(), classType.attributeDefinitions.length);
                Assert.assertEquals(expectedType.superTypes.size(), classType.superTypes.size());
                clsTypeFound = true;
            }
        }
        Assert.assertTrue(clsTypeFound, "Manager type not restored");

        //validate trait
        List<HierarchicalTypeDefinition<TraitType>> traitTypes = types.traitTypesAsJavaList();
        Assert.assertEquals(1, traitTypes.size());
        HierarchicalTypeDefinition<TraitType> trait = traitTypes.get(0);
        Assert.assertEquals("SecurityClearance", trait.typeName);
        Assert.assertEquals(1, trait.attributeDefinitions.length);
        AttributeDefinition attribute = trait.attributeDefinitions[0];
        Assert.assertEquals("level", attribute.name);
        Assert.assertEquals(DataTypes.INT_TYPE.getName(), attribute.dataTypeName);

        //validate the new types
        ts.reset();
        ts.defineTypes(types);
    }

    @Test(dependsOnMethods = "testStore")
    public void testTypeUpdate() throws Exception {
        //Add enum value
        EnumTypeDefinition orgLevelEnum = new EnumTypeDefinition("OrgLevel", new EnumValue("L1", 1),
                new EnumValue("L2", 2), new EnumValue("L3", 3));

        //Add attribute
        StructTypeDefinition addressDetails =
                createStructTypeDef("Address", createRequiredAttrDef("street", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("city", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("state", DataTypes.STRING_TYPE));

        //Add supertype
        HierarchicalTypeDefinition<ClassType> superTypeDef = createClassTypeDef("Division", ImmutableList.<String>of(),
                createOptionalAttrDef("dname", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef("Department",
                ImmutableList.of(superTypeDef.typeName), createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("employees", String.format("array<%s>", "Person"), Multiplicity.COLLECTION,
                        true, "department"));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.of(orgLevelEnum), ImmutableList.of(addressDetails),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(deptTypeDef, superTypeDef));

        ts.updateTypes(typesDef);
        typeStore.store(ts, ImmutableList.of(orgLevelEnum.name, addressDetails.typeName, superTypeDef.typeName,
                deptTypeDef.typeName));

        //Validate the updated types
        TypesDef types = typeStore.restore();
        ts.reset();
        ts.defineTypes(types);

        //Assert new enum value
        EnumType orgLevel = ts.getDataType(EnumType.class, orgLevelEnum.name);
        Assert.assertEquals(orgLevel.name, orgLevelEnum.name);
        Assert.assertEquals(orgLevel.values().size(), orgLevelEnum.enumValues.length);
        Assert.assertEquals(orgLevel.fromValue("L3").ordinal, 3);

        //Assert new attribute
        StructType addressType = ts.getDataType(StructType.class, addressDetails.typeName);
        Assert.assertEquals(addressType.numFields, 3);
        Assert.assertEquals(addressType.fieldMapping.fields.get("state").dataType(), DataTypes.STRING_TYPE);

        //Assert new super type
        ClassType deptType = ts.getDataType(ClassType.class, deptTypeDef.typeName);
        Assert.assertTrue(deptType.superTypes.contains(superTypeDef.typeName));
        Assert.assertNotNull(ts.getDataType(ClassType.class, superTypeDef.typeName));
    }
}
