/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.tagpropagation;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.impexp.ImportService;
import org.apache.atlas.repository.impexp.ZipFileResourceTestUtils;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.AtlasErrorCode.PROPAGATED_CLASSIFICATION_REMOVAL_NOT_SUPPORTED;
import static org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.BOTH;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.NONE;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.ONE_TO_TWO;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.TWO_TO_ONE;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runImportWithNoParameters;
import static org.apache.atlas.utils.TestLoadModelUtils.loadModelFromJson;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ClassificationPropagationTest extends AtlasTestBase {
    public static final String HDFS_PATH_EMPLOYEES     = "HDFS_PATH_EMPLOYEES";
    public static final String EMPLOYEES1_TABLE        = "EMPLOYEES1_TABLE";
    public static final String EMPLOYEES2_TABLE        = "EMPLOYEES2_TABLE";
    public static final String EMPLOYEES_UNION_TABLE   = "EMPLOYEES_UNION_TABLE";
    public static final String EMPLOYEES1_PROCESS      = "EMPLOYEES1_PROCESS";
    public static final String EMPLOYEES2_PROCESS      = "EMPLOYEES2_PROCESS";
    public static final String EMPLOYEES_UNION_PROCESS = "EMPLOYEES_UNION_PROCESS";
    public static final String EMPLOYEES_TABLE         = "EMPLOYEES_TABLE";
    public static final String US_EMPLOYEES_TABLE      = "US_EMPLOYEES2_TABLE";
    public static final String EMPLOYEES_PROCESS       = "EMPLOYEES_PROCESS";
    public static final String ORDERS_TABLE            = "ORDERS_TABLE";
    public static final String US_ORDERS_TABLE         = "US_ORDERS_TABLE";
    public static final String ORDERS_PROCESS          = "ORDERS_PROCESS";
    public static final String IMPORT_FILE             = "tag-propagation-data.zip";

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasEntityStore entityStore;

    @Inject
    private AtlasRelationshipStore relationshipStore;

    @Inject
    private ImportService importService;

    @Inject
    private AtlasLineageService lineageService;

    private Map<String, String> entitiesMap;

    private AtlasLineageInfo lineageInfo;

    public static InputStream getZipSource(String fileName) throws IOException {
        return ZipFileResourceTestUtils.getFileInputStream(fileName);
    }

    @BeforeClass
    public void setup() throws Exception {
        RequestContext.clear();

        super.initialize();

        loadModelFilesAndImportTestData();
    }

    @AfterClass
    public void clear() throws Exception {
        AtlasGraphProvider.cleanup();

        super.cleanup();
    }

    /**
     * This test uses the lineage graph:
     * <p>
     * <p>
     * Lineage - 1
     * -----------
     * [Process1] ----> [Employees1]
     * /                              \
     * /                                \
     * [hdfs_employees]                                  [Process3] ----> [ EmployeesUnion ]
     * \                                /
     * \                             /
     * [Process2] ----> [Employees2]
     * <p>
     * <p>
     * Lineage - 2
     * -----------
     * <p>
     * [Employees] ----> [Process] ----> [ US_Employees ]
     * <p>
     * <p>
     * Lineage - 3
     * -----------
     * <p>
     * [Orders] ----> [Process] ----> [ US_Orders ]
     */

    @Test
    public void addClassification_PropagateFalse() throws AtlasBaseException {
        AtlasEntity         hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag2          = new AtlasClassification("tag2");

        tag2.setPropagate(false);
        tag2.setEntityGuid(hdfsEmployees.getGuid());

        // add classification with propagate to 'false'
        addClassification(hdfsEmployees, tag2);

        List<String> propagatedToEntities = Arrays.asList(EMPLOYEES1_PROCESS, EMPLOYEES2_PROCESS, EMPLOYEES1_TABLE, EMPLOYEES2_TABLE, EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        assertClassificationNotExistInEntities(propagatedToEntities, tag2);
    }

    @Test(dependsOnMethods = "addClassification_PropagateFalse")
    public void updateClassification_PropagateFalseToTrue() throws AtlasBaseException {
        AtlasEntity         hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag2          = new AtlasClassification("tag2");

        tag2.setEntityGuid(hdfsEmployees.getGuid());

        //update tag2 propagate to 'true'
        tag2 = getClassification(hdfsEmployees, tag2);

        tag2.setPropagate(true);

        updateClassifications(hdfsEmployees, tag2);

        List<String> propagatedToEntities = Arrays.asList(EMPLOYEES1_PROCESS, EMPLOYEES2_PROCESS, EMPLOYEES1_TABLE, EMPLOYEES2_TABLE, EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        assertClassificationExistInEntities(propagatedToEntities, tag2);

        deleteClassification(hdfsEmployees, tag2);
    }

    @Test(dependsOnMethods = "updateClassification_PropagateFalseToTrue")
    public void addClassification_PropagateTrue() throws AtlasBaseException {
        AtlasEntity         hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag1          = new AtlasClassification("tag1");

        tag1.setPropagate(true);
        tag1.setEntityGuid(hdfsEmployees.getGuid());

        // add classification with propagate flag to 'true'
        addClassification(hdfsEmployees, tag1);

        List<String> propagatedToEntities = Arrays.asList(EMPLOYEES1_PROCESS, EMPLOYEES2_PROCESS, EMPLOYEES1_TABLE, EMPLOYEES2_TABLE, EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        assertClassificationExistInEntities(propagatedToEntities, tag1);
    }

    @Test(dependsOnMethods = "addClassification_PropagateTrue")
    public void updateClassification_PropagateTrueToFalse() throws AtlasBaseException {
        AtlasEntity         hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag1          = new AtlasClassification("tag1");

        tag1.setEntityGuid(hdfsEmployees.getGuid());

        List<String> propagatedToEntities = Arrays.asList(EMPLOYEES1_PROCESS, EMPLOYEES2_PROCESS, EMPLOYEES1_TABLE, EMPLOYEES2_TABLE, EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        // update propagate flag to 'false'
        tag1 = getClassification(hdfsEmployees, tag1);

        tag1.setPropagate(false);

        updateClassifications(hdfsEmployees, tag1);

        assertClassificationNotExistInEntities(propagatedToEntities, tag1);
    }

    @Test(dependsOnMethods = "updateClassification_PropagateTrueToFalse")
    public void deleteClassification_PropagateTrue() throws AtlasBaseException {
        AtlasEntity         hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag1          = new AtlasClassification("tag1");

        tag1.setPropagate(true);
        tag1.setEntityGuid(hdfsEmployees.getGuid());

        deleteClassification(hdfsEmployees, tag1);

        List<String> propagatedToEntities = Arrays.asList(EMPLOYEES1_PROCESS, EMPLOYEES2_PROCESS, EMPLOYEES1_TABLE, EMPLOYEES2_TABLE, EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        assertClassificationNotExistInEntities(propagatedToEntities, tag1);
    }

    @Test(dependsOnMethods = "deleteClassification_PropagateTrue")
    public void propagateSameTagFromDifferentEntities() throws AtlasBaseException {
        // add tag1 to hdfs_employees
        AtlasEntity         hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tag1          = new AtlasClassification("tag1");

        tag1.setPropagate(true);
        tag1.setEntityGuid(hdfsEmployees.getGuid());

        addClassification(hdfsEmployees, tag1);

        // add tag1 to employees2
        AtlasEntity employees2Table = getEntity(EMPLOYEES2_TABLE);

        tag1 = new AtlasClassification("tag1");

        tag1.setPropagate(true);
        tag1.setEntityGuid(employees2Table.getGuid());

        addClassification(employees2Table, tag1);

        // employees_union table should have two tags 'tag1' propagated from hdfs_employees and employees2 table
        AtlasEntity               employeesUnionTable = getEntity(EMPLOYEES_UNION_TABLE);
        List<AtlasClassification> classifications     = employeesUnionTable.getClassifications();

        assertNotNull(classifications);
        assertEquals(classifications.size(), 2);

        // assert same tag propagated from hdfs_employees and employees2
        assertEquals(classifications.get(0).getTypeName(), tag1.getTypeName());
        assertEquals(classifications.get(1).getTypeName(), tag1.getTypeName());

        if (classifications.get(0).getEntityGuid().equals(hdfsEmployees.getGuid())) {
            assertEquals(classifications.get(1).getEntityGuid(), employees2Table.getGuid());
        }

        if (classifications.get(0).getEntityGuid().equals(employees2Table.getGuid())) {
            assertEquals(classifications.get(1).getEntityGuid(), hdfsEmployees.getGuid());
        }

        // cleanup
        deleteClassification(hdfsEmployees, tag1);
        deleteClassification(employees2Table, tag1);
    }

    @Test(dependsOnMethods = "propagateSameTagFromDifferentEntities")
    public void updatePropagateTagsValue() throws AtlasBaseException {
        AtlasEntity hdfsEmployees         = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasEntity employees2Table       = getEntity(EMPLOYEES2_TABLE);
        AtlasEntity employeesUnionProcess = getEntity(EMPLOYEES_UNION_PROCESS);
        AtlasEntity employeesUnionTable   = getEntity(EMPLOYEES_UNION_TABLE);

        AtlasClassification tag1 = new AtlasClassification("tag1");

        tag1.setPropagate(true);
        tag1.setEntityGuid(hdfsEmployees.getGuid());

        AtlasClassification tag2 = new AtlasClassification("tag2");

        tag2.setPropagate(true);
        tag2.setEntityGuid(employees2Table.getGuid());

        AtlasClassification tag3 = new AtlasClassification("tag3");

        tag3.setPropagate(true);
        tag3.setEntityGuid(employeesUnionProcess.getGuid());

        AtlasClassification tag4 = new AtlasClassification("tag4");

        tag4.setPropagate(true);
        tag4.setEntityGuid(employeesUnionTable.getGuid());

        // add tag1 to hdfs_employees, tag2 to employees2, tag3 to process3, tag4 to employees_union
        addClassification(hdfsEmployees, tag1);
        addClassification(employees2Table, tag2);
        addClassification(employeesUnionProcess, tag3);
        addClassification(employeesUnionTable, tag4);

        //validate if tag1, tag2, tag3 propagated to employees_union table
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, tag1);
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, tag2);
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, tag3);
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, tag4);

        // change propagation between employees2 -> process3 from TWO_TO_ONE to NONE
        AtlasRelationship employees2ProcessRelationship = getRelationship(EMPLOYEES2_TABLE, EMPLOYEES_UNION_PROCESS);

        assertEquals(employees2ProcessRelationship.getPropagateTags(), TWO_TO_ONE);

        employees2ProcessRelationship.setPropagateTags(NONE);

        relationshipStore.update(employees2ProcessRelationship);

        // validate tag1 propagated to employees_union through other path
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, tag1);

        // validate tag2 is no more propagated to employees_union
        assertClassificationNotExistInEntity(EMPLOYEES_UNION_TABLE, tag2);

        // change propagation between employees2 -> process3 from NONE to TWO_TO_ONE
        employees2ProcessRelationship = getRelationship(EMPLOYEES2_TABLE, EMPLOYEES_UNION_PROCESS);
        assertEquals(employees2ProcessRelationship.getPropagateTags(), NONE);
        employees2ProcessRelationship.setPropagateTags(TWO_TO_ONE);
        relationshipStore.update(employees2ProcessRelationship);

        // validate tag2 is propagated to employees_union
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, tag2);

        //update propagation to BOTH for edge process3 --> employee_union. This should fail
        AtlasRelationship process3EmployeeUnionRelationship = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        assertEquals(process3EmployeeUnionRelationship.getPropagateTags(), ONE_TO_TWO);

        process3EmployeeUnionRelationship.setPropagateTags(BOTH);

        try {
            relationshipStore.update(process3EmployeeUnionRelationship);
        } catch (AtlasBaseException ex) {
            assertEquals(ex.getAtlasErrorCode(), AtlasErrorCode.INVALID_PROPAGATION_TYPE);
        }

        //cleanup
        deleteClassification(hdfsEmployees, tag1);
        deleteClassification(employees2Table, tag2);
        deleteClassification(employeesUnionProcess, tag3);
        deleteClassification(employeesUnionTable, tag4);
    }

    @Test(dependsOnMethods = "updatePropagateTagsValue")
    public void addBlockedPropagatedClassifications() throws AtlasBaseException {
        AtlasEntity hdfsPath       = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasEntity employees1     = getEntity(EMPLOYEES1_TABLE);
        AtlasEntity employees2     = getEntity(EMPLOYEES2_TABLE);
        AtlasEntity employeesUnion = getEntity(EMPLOYEES_UNION_TABLE);

        AtlasClassification piiTag1 = new AtlasClassification("PII");

        piiTag1.setPropagate(true);
        piiTag1.setAttribute("type", "from hdfs_path entity");
        piiTag1.setAttribute("valid", true);

        AtlasClassification piiTag2 = new AtlasClassification("PII");

        piiTag2.setPropagate(true);
        piiTag2.setAttribute("type", "from employees1 entity");
        piiTag2.setAttribute("valid", true);

        AtlasClassification piiTag3 = new AtlasClassification("PII");

        piiTag3.setPropagate(true);
        piiTag3.setAttribute("type", "from employees2 entity");
        piiTag3.setAttribute("valid", true);

        AtlasClassification piiTag4 = new AtlasClassification("PII");

        piiTag4.setPropagate(true);
        piiTag4.setAttribute("type", "from employees_union entity");
        piiTag4.setAttribute("valid", true);

        // add PII to hdfs_path, employees1, employees2 and employee_union
        addClassification(hdfsPath, piiTag1);
        addClassification(employees1, piiTag2);
        addClassification(employees2, piiTag3);

        // check 4 PII tags exists in employee_union table
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag1.getTypeName(), hdfsPath.getGuid());
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag2.getTypeName(), employees1.getGuid());
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag3.getTypeName(), employees2.getGuid());

        AtlasRelationship        process3EmployeeUnionRelationship = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);
        Set<AtlasClassification> propagatedClassifications         = process3EmployeeUnionRelationship.getPropagatedClassifications();
        Set<AtlasClassification> blockedClassifications            = process3EmployeeUnionRelationship.getBlockedPropagatedClassifications();

        assertNotNull(propagatedClassifications);
        assertClassificationEquals(propagatedClassifications, piiTag1);
        assertClassificationEquals(propagatedClassifications, piiTag2);
        assertClassificationEquals(propagatedClassifications, piiTag3);
        assertTrue(blockedClassifications.isEmpty());

        // block PII tag propagating from employees1 and employees2
        piiTag2.setEntityGuid(employees1.getGuid());
        piiTag3.setEntityGuid(employees2.getGuid());

        process3EmployeeUnionRelationship.setBlockedPropagatedClassifications(new HashSet<>(Arrays.asList(piiTag2, piiTag3)));
        relationshipStore.update(process3EmployeeUnionRelationship);

        process3EmployeeUnionRelationship = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);
        propagatedClassifications            = process3EmployeeUnionRelationship.getPropagatedClassifications();
        blockedClassifications               = process3EmployeeUnionRelationship.getBlockedPropagatedClassifications();

        assertClassificationEquals(propagatedClassifications, piiTag1);
        assertFalse(blockedClassifications.isEmpty());
        assertClassificationEquals(blockedClassifications, piiTag2);
        assertClassificationEquals(blockedClassifications, piiTag3);

        assertClassificationNotExistInEntity(EMPLOYEES_UNION_TABLE, piiTag2);
        assertClassificationNotExistInEntity(EMPLOYEES_UNION_TABLE, piiTag3);

        // assert only PII from hdfs_path is propagated to employees_union, PII from employees1 and employees2 is blocked.
        assertEquals(getEntity(EMPLOYEES_UNION_TABLE).getClassifications().size(), 1);
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag1.getTypeName(), hdfsPath.getGuid());
    }

    @Test(dependsOnMethods = "addBlockedPropagatedClassifications")
    public void removeBlockedPropagatedClassifications() throws AtlasBaseException {
        AtlasEntity hdfsPath   = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasEntity employees1 = getEntity(EMPLOYEES1_TABLE);
        AtlasEntity employees2 = getEntity(EMPLOYEES2_TABLE);

        AtlasClassification piiTag1 = new AtlasClassification("PII");

        piiTag1.setPropagate(true);
        piiTag1.setEntityGuid(hdfsPath.getGuid());
        piiTag1.setAttribute("type", "from hdfs_path entity");
        piiTag1.setAttribute("valid", true);

        AtlasClassification piiTag2 = new AtlasClassification("PII");

        piiTag2.setPropagate(true);
        piiTag2.setEntityGuid(employees1.getGuid());
        piiTag2.setAttribute("type", "from employees1 entity");
        piiTag2.setAttribute("valid", true);

        AtlasClassification piiTag3 = new AtlasClassification("PII");

        piiTag3.setPropagate(true);
        piiTag3.setEntityGuid(employees2.getGuid());
        piiTag3.setAttribute("type", "from employees2 entity");
        piiTag3.setAttribute("valid", true);

        AtlasRelationship process3EmployeeUnionRelationship = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        // remove blocked propagated classification entry for PII (from employees2) - allow PII from employees2 to propagate to employee_union
        process3EmployeeUnionRelationship.setBlockedPropagatedClassifications(new HashSet<>(Arrays.asList(piiTag3)));

        relationshipStore.update(process3EmployeeUnionRelationship);

        process3EmployeeUnionRelationship = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        Set<AtlasClassification> propagatedClassifications = process3EmployeeUnionRelationship.getPropagatedClassifications();
        Set<AtlasClassification> blockedClassifications    = process3EmployeeUnionRelationship.getBlockedPropagatedClassifications();

        assertClassificationExistInList(propagatedClassifications, piiTag1);
        assertClassificationExistInList(propagatedClassifications, piiTag2);
        assertClassificationExistInList(blockedClassifications, piiTag3);

        // remove all blocked propagated classification entry
        process3EmployeeUnionRelationship.setBlockedPropagatedClassifications(Collections.emptySet());

        relationshipStore.update(process3EmployeeUnionRelationship);

        process3EmployeeUnionRelationship = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);
        propagatedClassifications            = process3EmployeeUnionRelationship.getPropagatedClassifications();
        blockedClassifications               = process3EmployeeUnionRelationship.getBlockedPropagatedClassifications();

        assertClassificationExistInList(propagatedClassifications, piiTag1);
        assertClassificationExistInList(propagatedClassifications, piiTag2);
        assertClassificationExistInList(propagatedClassifications, piiTag3);
        assertTrue(blockedClassifications.isEmpty());
    }

    @Test(dependsOnMethods = "removeBlockedPropagatedClassifications")
    public void addClassification_removePropagationsTrue_DeleteCase() throws AtlasBaseException {
        AtlasEntity         orders = getEntity(ORDERS_TABLE);
        AtlasClassification tag2   = new AtlasClassification("tag2");

        tag2.setEntityGuid(orders.getGuid());
        tag2.setPropagate(true);
        tag2.setRemovePropagationsOnEntityDelete(true);

        addClassification(orders, tag2);

        List<String> propagatedEntities = Arrays.asList(EMPLOYEES_PROCESS, US_EMPLOYEES_TABLE);

        assertClassificationExistInEntities(propagatedEntities, tag2);

        AtlasEntity ordersProcess = getEntity(ORDERS_PROCESS);
        AtlasEntity usOrders      = getEntity(US_ORDERS_TABLE);

        deletePropagatedClassificationExpectFail(ordersProcess, tag2);
        deletePropagatedClassificationExpectFail(usOrders, tag2);

        deleteEntity(ORDERS_TABLE);
        assertClassificationNotExistInEntity(ORDERS_PROCESS, tag2);
        assertClassificationNotExistInEntity(US_ORDERS_TABLE, tag2);
    }

    @Test(dependsOnMethods = "addClassification_removePropagationsTrue_DeleteCase")
    public void addClassification_removePropagationsFalse_DeleteCase() throws AtlasBaseException {
        AtlasEntity         employees = getEntity(EMPLOYEES_TABLE);
        AtlasClassification tag1      = new AtlasClassification("tag1");

        tag1.setEntityGuid(employees.getGuid());
        tag1.setPropagate(true);
        tag1.setRemovePropagationsOnEntityDelete(false);

        addClassification(employees, tag1);

        List<String> propagatedEntities = Arrays.asList(EMPLOYEES_PROCESS, US_EMPLOYEES_TABLE);

        assertClassificationExistInEntities(propagatedEntities, tag1);

        AtlasEntity employeesProcess = getEntity(EMPLOYEES_PROCESS);
        AtlasEntity usEmployees      = getEntity(US_EMPLOYEES_TABLE);

        deletePropagatedClassificationExpectFail(employeesProcess, tag1);
        deletePropagatedClassificationExpectFail(usEmployees, tag1);

        deleteEntity(EMPLOYEES_PROCESS);
        assertClassificationExistInEntity(EMPLOYEES_PROCESS, tag1);
        assertClassificationExistInEntity(US_EMPLOYEES_TABLE, tag1);
    }

    private void assertClassificationEquals(Set<AtlasClassification> propagatedClassifications, AtlasClassification expected) {
        String expectedTypeName = expected.getTypeName();
        for (AtlasClassification c : propagatedClassifications) {
            if (c.getTypeName().equals(expectedTypeName)) {
                assertTrue(c.isPropagate() == expected.isPropagate(), "isPropgate does not match");
                assertTrue(c.getValidityPeriods() == expected.getValidityPeriods(), "validityPeriods do not match");
                return;
            }
        }

        fail(expectedTypeName + " could not be found");
    }

    private void assertClassificationExistInList(Set<AtlasClassification> classifications, AtlasClassification classification) {
        String  classificationName  = classification.getTypeName();
        String  entityGuid          = classification.getEntityGuid();
        boolean foundClassification = false;

        for (AtlasClassification c : classifications) {
            if (c.getTypeName().equals(classificationName) && c.getEntityGuid().equals(entityGuid)) {
                foundClassification = true;
            }
        }

        if (!foundClassification) {
            fail("Propagated classification is not present in classifications list!");
        }
    }

    private void assertClassificationExistInEntities(List<String> entityNames, AtlasClassification classification) throws AtlasBaseException {
        for (String entityName : entityNames) {
            assertClassificationExistInEntity(entityName, classification);
        }
    }

    private void assertClassificationExistInEntity(String entityName, AtlasClassification classification) throws AtlasBaseException {
        assertClassificationExistInEntity(entityName, classification.getTypeName(), classification.getEntityGuid());
    }

    private void assertClassificationExistInEntity(String entityName, String tagName, String sourceEntityGuid) throws AtlasBaseException {
        List<AtlasClassification> classifications    = getEntity(entityName).getClassifications();
        String                    classificationName = tagName;
        String                    entityGuid         = sourceEntityGuid;

        if (CollectionUtils.isNotEmpty(classifications)) {
            boolean foundClassification = false;

            for (AtlasClassification c : classifications) {
                if (c.getTypeName().equals(classificationName) && c.getEntityGuid().equals(entityGuid)) {
                    foundClassification = true;
                }
            }

            if (!foundClassification) {
                fail("Propagated classification is not present in entity!");
            }
        }
    }

    private void assertClassificationNotExistInEntities(List<String> entityNames, AtlasClassification classification) throws AtlasBaseException {
        for (String entityName : entityNames) {
            assertClassificationNotExistInEntity(entityName, classification);
        }
    }

    private void assertClassificationNotExistInEntity(String entityName, AtlasClassification classification) throws AtlasBaseException {
        List<AtlasClassification> classifications    = getEntity(entityName).getClassifications();
        String                    classificationName = classification.getTypeName();
        String                    entityGuid         = classification.getEntityGuid();

        if (CollectionUtils.isNotEmpty(classifications)) {
            for (AtlasClassification c : classifications) {
                if (c.getTypeName().equals(classificationName) && c.getEntityGuid().equals(entityGuid)) {
                    fail("Propagated classification should not be present in entity!");
                }
            }
        }
    }

    private void loadModelFilesAndImportTestData() {
        try {
            loadModelFromJson("0000-Area0/0010-base_model.json", typeDefStore, typeRegistry);
            loadModelFromJson("1000-Hadoop/1020-fs_model.json", typeDefStore, typeRegistry);
            loadModelFromJson("1000-Hadoop/1030-hive_model.json", typeDefStore, typeRegistry);

            loadSampleClassificationDefs();

            runImportWithNoParameters(importService, getZipSource(IMPORT_FILE));

            initializeEntitiesMap();
        } catch (AtlasBaseException | IOException e) {
            throw new SkipException("Model loading failed!");
        }
    }

    private void loadSampleClassificationDefs() throws AtlasBaseException {
        AtlasClassificationDef tag1 = new AtlasClassificationDef("tag1");
        AtlasClassificationDef tag2 = new AtlasClassificationDef("tag2");
        AtlasClassificationDef tag3 = new AtlasClassificationDef("tag3");
        AtlasClassificationDef tag4 = new AtlasClassificationDef("tag4");

        AtlasClassificationDef pii = new AtlasClassificationDef("PII");

        pii.addAttribute(new AtlasAttributeDef("type", "string"));
        pii.addAttribute(new AtlasAttributeDef("valid", "boolean"));

        typeDefStore.createTypesDef(new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(tag1, tag2, tag3, tag4, pii),
                Collections.emptyList(), Collections.emptyList()));
    }

    private void initializeEntitiesMap() throws AtlasBaseException {
        entitiesMap = new HashMap<>();

        entitiesMap.put(HDFS_PATH_EMPLOYEES, "a3955120-ac17-426f-a4af-972ec8690e5f");
        entitiesMap.put(EMPLOYEES1_TABLE, "cdf0040e-739e-4590-a137-964d10e73573");
        entitiesMap.put(EMPLOYEES2_TABLE, "0a3e66b6-472c-48b3-8453-abdd24f9494f");
        entitiesMap.put(EMPLOYEES_UNION_TABLE, "1ceac963-1a2b-476a-a269-10396187d406");

        entitiesMap.put(EMPLOYEES1_PROCESS, "26dae763-85b7-40af-8516-71056d91d2de");
        entitiesMap.put(EMPLOYEES2_PROCESS, "c0201260-dbeb-45f4-930d-5129eab31dc9");
        entitiesMap.put(EMPLOYEES_UNION_PROCESS, "470a2d1e-b1fd-47de-8f2d-8dfd0a0275a7");

        entitiesMap.put(EMPLOYEES_TABLE, "b4edad46-d00f-4e94-be39-8d2619d17e6c");
        entitiesMap.put(US_EMPLOYEES_TABLE, "44acef8e-fefe-491c-87d9-e2ea6a9ad3b0");
        entitiesMap.put(EMPLOYEES_PROCESS, "a1c9a281-d30b-419c-8199-7434b245d7fe");

        entitiesMap.put(ORDERS_TABLE, "ab995a8d-1f87-4908-91e4-d4e8e376ba22");
        entitiesMap.put(US_ORDERS_TABLE, "70268a81-f145-4a37-ae39-b09daa85a928");
        entitiesMap.put(ORDERS_PROCESS, "da016ad9-456a-4c99-895a-fa00f2de49ba");

        lineageInfo = lineageService.getAtlasLineageInfo(entitiesMap.get(HDFS_PATH_EMPLOYEES), LineageDirection.BOTH, 3);
    }

    private AtlasEntity getEntity(String entityName) throws AtlasBaseException {
        String                 entityGuid        = entitiesMap.get(entityName);
        AtlasEntityWithExtInfo entityWithExtInfo = entityStore.getById(entityGuid);

        return entityWithExtInfo.getEntity();
    }

    private boolean deleteEntity(String entityName) throws AtlasBaseException {
        String                 entityGuid = entitiesMap.get(entityName);
        EntityMutationResponse response   = entityStore.deleteById(entityGuid);

        return CollectionUtils.isNotEmpty(response.getDeletedEntities());
    }

    private AtlasClassification getClassification(AtlasEntity hdfsEmployees, AtlasClassification tag2) throws AtlasBaseException {
        return entityStore.getClassification(hdfsEmployees.getGuid(), tag2.getTypeName());
    }

    private void addClassification(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        addClassifications(entity, Collections.singletonList(classification));
    }

    private void addClassifications(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        entityStore.addClassifications(entity.getGuid(), classifications);
    }

    private void updateClassifications(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        updateClassifications(entity, Collections.singletonList(classification));
    }

    private void updateClassifications(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        entityStore.updateClassifications(entity.getGuid(), classifications);
    }

    private void deleteClassification(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        deleteClassifications(entity, Collections.singletonList(classification.getTypeName()));
    }

    private void deleteClassifications(AtlasEntity entity, List<String> classificationNames) throws AtlasBaseException {
        for (String classificationName : classificationNames) {
            entityStore.deleteClassification(entity.getGuid(), classificationName);
        }
    }

    private void deletePropagatedClassification(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        deletePropagatedClassification(entity, classification.getTypeName(), classification.getEntityGuid());
    }

    private void deletePropagatedClassification(AtlasEntity entity, String classificationName, String associatedEntityGuid) throws AtlasBaseException {
        entityStore.deleteClassification(entity.getGuid(), classificationName, associatedEntityGuid);
    }

    private void deletePropagatedClassificationExpectFail(AtlasEntity entity, AtlasClassification classification) {
        try {
            deletePropagatedClassification(entity, classification);
            fail();
        } catch (AtlasBaseException ex) {
            assertEquals(ex.getAtlasErrorCode(), PROPAGATED_CLASSIFICATION_REMOVAL_NOT_SUPPORTED);
        }
    }

    private AtlasRelationship getRelationship(String fromEntityName, String toEntityName) throws AtlasBaseException {
        String               fromEntityId     = entitiesMap.get(fromEntityName);
        String               toEntityId       = entitiesMap.get(toEntityName);
        Set<LineageRelation> relations        = lineageInfo.getRelations();
        String               relationshipGuid = null;

        for (AtlasLineageInfo.LineageRelation relation : relations) {
            if (relation.getFromEntityId().equals(fromEntityId) && relation.getToEntityId().equals(toEntityId)) {
                relationshipGuid = relation.getRelationshipId();
            }
        }

        return relationshipStore.getById(relationshipGuid);
    }
}
