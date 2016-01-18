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

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Vertex;

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



    @Test
    public void testDeleteEntities() throws Exception {
        String hrDeptGuid = createHrDeptGraph();

        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Object refValue = hrDept.get("employees");
        Assert.assertTrue(refValue instanceof List);
        List<Object> employees = (List<Object>)refValue;
        Assert.assertEquals(employees.size(), 4);
        List<String> employeeGuids = new ArrayList<String>(4);
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
        
        List<String> deletedEntities = repositoryService.deleteEntities(hrDeptGuid);
        Assert.assertTrue(deletedEntities.contains(hrDeptGuid));
        
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

    @Test(dependsOnMethods = "testDeleteEntities")
    public void testDeleteContainedEntity() throws Exception {
        String hrDeptGuid = createHrDeptGraph();
        ITypedReferenceableInstance hrDept = repositoryService.getEntityDefinition(hrDeptGuid);
        Object refValue = hrDept.get("employees");
        Assert.assertTrue(refValue instanceof List);
        List<Object> employees = (List<Object>)refValue;
        Assert.assertEquals(employees.size(), 4);
        Object listValue = employees.get(2);
        Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
        ITypedReferenceableInstance employee = (ITypedReferenceableInstance) listValue;
        String employeeGuid = employee.getId()._getId();
        
        List<String> deletedEntities = repositoryService.deleteEntities(employeeGuid);
        Assert.assertTrue(deletedEntities.contains(employeeGuid));
        verifyEntityDoesNotExist(employeeGuid);
        
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
        String hrDeptGuid = entityList.get(0);
        return hrDeptGuid;
    }
    
    private int countVertices(String propertyName, Object value) {
        Iterable<Vertex> vertices = graphProvider.get().getVertices(propertyName, value);
        int vertexCount = 0;
        for (Vertex vertex : vertices) {
            vertexCount++;
        }
        return vertexCount;
    }
    
    private void verifyEntityDoesNotExist(String hrDeptGuid) throws RepositoryException {

        try {
            repositoryService.getEntityDefinition(hrDeptGuid);
            Assert.fail("EntityNotFoundException was expected but none thrown");
        }
        catch(EntityNotFoundException e) {
            // good
        }
    }

}
