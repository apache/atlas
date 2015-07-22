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

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import junit.framework.Assert;
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.List;

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
    @GraphTransaction
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
        Assert.assertTrue("Manager type not restored", clsTypeFound);

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
}
