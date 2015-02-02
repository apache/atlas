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

package org.apache.hadoop.metadata.repository.graph;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.Referenceable;
import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.TestUtils;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.List;

/**
 * GraphBackedMetadataRepository test
 * 
 * Guice loads the dependencies and injects the necessary objects
 *
 */
@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedMetadataRepositoryTest {

    private static final String ENTITY_TYPE = "Department";

    @Inject
    private TitanGraphService titanGraphService;
    @Inject
    private GraphBackedMetadataRepository repositoryService;

    private TypeSystem ts;
    private String guid;

    @BeforeClass
    public void setUp() throws Exception {
    	// start the injected graph service
        titanGraphService.start();
        // start the injected repository service
        repositoryService.start();

        new GraphBackedSearchIndexer(titanGraphService);

        ts = TypeSystem.getInstance();

        TestUtils.defineDeptEmployeeTypes(ts);
    }

    @Test
    public void testSubmitEntity() throws Exception {
        Referenceable hrDept = TestUtils.createDeptEg1(ts);
        ClassType deptType = ts.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        guid = repositoryService.createEntity(hrDept2, ENTITY_TYPE);
        Assert.assertNotNull(guid);

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

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinition() throws Exception {
        ITypedReferenceableInstance entity = repositoryService.getEntityDefinition(guid);
        Assert.assertNotNull(entity);
    }

    @Test
    public void testGetEntityDefinitionNonExistent() throws Exception {
        ITypedReferenceableInstance entity = repositoryService.getEntityDefinition("blah");
        Assert.assertNull(entity);
    }

    @Test(enabled = false)
    public void testGetEntityList() throws Exception {
        List<String> entityList = repositoryService.getEntityList(ENTITY_TYPE);
        System.out.println("entityList = " + entityList);
        Assert.assertNotNull(entityList);
        Assert.assertEquals(entityList.size(), 1); // one department
    }
}
