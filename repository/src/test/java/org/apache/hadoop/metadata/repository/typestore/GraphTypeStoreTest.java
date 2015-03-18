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

package org.apache.hadoop.metadata.repository.typestore;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import junit.framework.Assert;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.TestUtils;
import org.apache.hadoop.metadata.repository.graph.GraphHelper;
import org.apache.hadoop.metadata.repository.graph.TitanGraphService;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.List;

@Guice(modules = RepositoryMetadataModule.class)
public class GraphTypeStoreTest {
    @Inject
    private TitanGraphService titanGraphService;
    @Inject
    private ITypeStore typeStore;

    private TypeSystem ts;

    @BeforeClass
    public void setUp() throws Exception {
        // start the injected graph service
        titanGraphService.initialize();

        ts = TypeSystem.getInstance();
        ts.reset();
        TestUtils.defineDeptEmployeeTypes(ts);
    }

    @Test
    public void testStore() throws MetadataException {
        typeStore.store(ts);
        dumpGraph();
    }

    private void dumpGraph() {
        TitanGraph graph = titanGraphService.getTitanGraph();
        for (Vertex v : graph.getVertices()) {
            System.out.println("****v = " + GraphHelper.vertexString(v));
            for (Edge e : v.getEdges(Direction.OUT)) {
                System.out.println("****e = " + GraphHelper.edgeString(e));
            }
        }
    }

    @Test (dependsOnMethods = "testStore")
    public void testRestore() throws Exception {
        TypesDef types = typeStore.restore();

        //validate enum
        List<EnumTypeDefinition> enumTypes = types.enumTypesAsJavaList();
        Assert.assertEquals(1, enumTypes.size());

        //validate class
        Assert.assertTrue(types.structTypesAsJavaList().isEmpty());
        List<HierarchicalTypeDefinition<ClassType>> classTypes = types.classTypesAsJavaList();
        Assert.assertEquals(3, classTypes.size());
        for (HierarchicalTypeDefinition<ClassType> classType : classTypes) {
            ClassType expectedType = ts.getDataType(ClassType.class, classType.typeName);
            Assert.assertEquals(expectedType.immediateAttrs.size(), classType.attributeDefinitions.length);
        }

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
