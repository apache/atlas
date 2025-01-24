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
package org.apache.atlas.type;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.RelationshipCategory;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry.AtlasTransientTypeRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAtlasRelationshipType {
    private static final String EMPLOYEE_TYPE                  = "employee";
    private static final String DEPARTMENT_TYPE                = "department";
    private static final String ADDRESS_TYPE                   = "address";
    private static final String PHONE_TYPE                     = "phone";
    private static final String DEPT_EMPLOYEE_RELATION_TYPE    = "departmentEmployee";
    private static final String EMPLOYEE_ADDRESS_RELATION_TYPE = "employeeAddress";
    private static final String EMPLOYEE_PHONE_RELATION_TYPE   = "employeePhone";
    private AtlasTypeRegistry typeRegistry;

    @BeforeMethod
    public void setUp() throws AtlasBaseException {
        typeRegistry = new AtlasTypeRegistry();
        createEmployeeTypes();
        createRelationshipTypes();
    }

    @Test
    public void testvalidateAtlasRelationshipDef() throws AtlasBaseException {
        AtlasRelationshipEndDef epSingle           = new AtlasRelationshipEndDef("typeA", "attr1", Cardinality.SINGLE);
        AtlasRelationshipEndDef epSingleContainer  = new AtlasRelationshipEndDef("typeB", "attr2", Cardinality.SINGLE);
        AtlasRelationshipEndDef epSingleContainer2 = new AtlasRelationshipEndDef("typeC", "attr3", Cardinality.SINGLE, true);
        AtlasRelationshipEndDef epSingleContainer3 = new AtlasRelationshipEndDef("typeD", "attr4", Cardinality.SINGLE, true);
        AtlasRelationshipEndDef epSet              = new AtlasRelationshipEndDef("typeD", "attr4", Cardinality.SET, false);
        AtlasRelationshipEndDef epList             = new AtlasRelationshipEndDef("typeE", "attr5", Cardinality.LIST, true);
        AtlasRelationshipEndDef epSetContainer     = new AtlasRelationshipEndDef("typeF", "attr6", Cardinality.SET, true);

        AtlasRelationshipDef relationshipDef1 = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1", RelationshipCategory.ASSOCIATION, PropagateTags.ONE_TO_TWO, epSingle, epSet);
        AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef1);

        AtlasRelationshipDef relationshipDef2 = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1", RelationshipCategory.COMPOSITION, PropagateTags.ONE_TO_TWO, epSetContainer, epSingle);
        AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef2);

        AtlasRelationshipDef relationshipDef3 = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1", RelationshipCategory.AGGREGATION, PropagateTags.ONE_TO_TWO, epSetContainer, epSingle);
        AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef3);

        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1", RelationshipCategory.ASSOCIATION, PropagateTags.ONE_TO_TWO, epSingleContainer2, epSingleContainer);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);

            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_ASSOCIATION_AND_CONTAINER)) {
                fail("This call expected a different error");
            }
        }
        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1", RelationshipCategory.COMPOSITION, PropagateTags.ONE_TO_TWO, epSingle, epSingleContainer);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_COMPOSITION_NO_CONTAINER)) {
                fail("This call expected a different error");
            }
        }
        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1", RelationshipCategory.AGGREGATION, PropagateTags.ONE_TO_TWO, epSingle, epSingleContainer);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_AGGREGATION_NO_CONTAINER)) {
                fail("This call expected a different error");
            }
        }

        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                    RelationshipCategory.COMPOSITION, PropagateTags.ONE_TO_TWO, epSetContainer, epSet);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_COMPOSITION_MULTIPLE_PARENTS)) {
                fail("This call expected a different error");
            }
        }
        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1", RelationshipCategory.COMPOSITION, PropagateTags.ONE_TO_TWO, epSingle, epList);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_LIST_ON_END)) {
                fail("This call expected a different error");
            }
        }
        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1", RelationshipCategory.COMPOSITION, PropagateTags.ONE_TO_TWO, epList, epSingle);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_LIST_ON_END)) {
                fail("This call expected a different error");
            }
        }
    }

    @Test
    public void testRelationshipAttributes() {
        Map<String, Map<String, AtlasAttribute>> employeeRelationAttrs = getRelationAttrsForType(EMPLOYEE_TYPE);

        assertNotNull(employeeRelationAttrs);
        assertEquals(employeeRelationAttrs.size(), 2);

        assertTrue(employeeRelationAttrs.containsKey("department"));
        assertTrue(employeeRelationAttrs.containsKey("address"));

        AtlasAttribute deptAttr = employeeRelationAttrs.get("department").values().iterator().next();
        assertEquals(deptAttr.getTypeName(), DEPARTMENT_TYPE);

        AtlasAttribute addrAttr = employeeRelationAttrs.get("address").values().iterator().next();
        assertEquals(addrAttr.getTypeName(), ADDRESS_TYPE);

        Map<String, Map<String, AtlasAttribute>> deptRelationAttrs = getRelationAttrsForType(DEPARTMENT_TYPE);

        assertNotNull(deptRelationAttrs);
        assertEquals(deptRelationAttrs.size(), 1);
        assertTrue(deptRelationAttrs.containsKey("employees"));

        AtlasAttribute employeesAttr = deptRelationAttrs.get("employees").values().iterator().next();
        assertEquals(employeesAttr.getTypeName(), AtlasBaseTypeDef.getArrayTypeName(EMPLOYEE_TYPE));

        Map<String, Map<String, AtlasAttribute>> addressRelationAttrs = getRelationAttrsForType(ADDRESS_TYPE);

        assertNotNull(addressRelationAttrs);
        assertEquals(addressRelationAttrs.size(), 1);
        assertTrue(addressRelationAttrs.containsKey("employees"));

        AtlasAttribute employeesAttr1 = addressRelationAttrs.get("employees").values().iterator().next();
        assertEquals(employeesAttr1.getTypeName(), AtlasBaseTypeDef.getArrayTypeName(EMPLOYEE_TYPE));
    }

    @Test(dependsOnMethods = "testRelationshipAttributes")
    public void testRelationshipAttributesOnExistingAttributes() throws Exception {
        AtlasRelationshipDef employeePhoneRelationDef = new AtlasRelationshipDef(EMPLOYEE_PHONE_RELATION_TYPE, getDescription(EMPLOYEE_PHONE_RELATION_TYPE), "1.0",
                RelationshipCategory.ASSOCIATION, PropagateTags.ONE_TO_TWO, new AtlasRelationshipEndDef(EMPLOYEE_TYPE, "phone_no", Cardinality.SINGLE),
                new AtlasRelationshipEndDef(PHONE_TYPE, "owner", Cardinality.SINGLE));

        createType(employeePhoneRelationDef);

        Map<String, Map<String, AtlasAttribute>> employeeRelationshipAttrs = getRelationAttrsForType(EMPLOYEE_TYPE);
        Map<String, AtlasAttribute>              employeeAttrs             = getAttrsForType(EMPLOYEE_TYPE);

        // validate if phone_no exists in both relationAttributes and attributes
        assertTrue(employeeRelationshipAttrs.containsKey("phone_no"));
        assertTrue(employeeAttrs.containsKey("phone_no"));
    }

    private void createEmployeeTypes() throws AtlasBaseException {
        AtlasEntityDef phoneDef = AtlasTypeUtil.createClassTypeDef(PHONE_TYPE, getDescription(PHONE_TYPE), Collections.emptySet(),
                AtlasTypeUtil.createRequiredAttrDef("phone_number", "int"), AtlasTypeUtil.createOptionalAttrDef("area_code", "int"),
                AtlasTypeUtil.createOptionalAttrDef("owner", EMPLOYEE_TYPE));

        AtlasEntityDef employeeDef = AtlasTypeUtil.createClassTypeDef(EMPLOYEE_TYPE, getDescription(EMPLOYEE_TYPE), Collections.emptySet(),
                AtlasTypeUtil.createRequiredAttrDef("name", "string"), AtlasTypeUtil.createOptionalAttrDef("dob", "date"),
                AtlasTypeUtil.createOptionalAttrDef("age", "int"), AtlasTypeUtil.createRequiredAttrDef("phone_no", PHONE_TYPE));

        AtlasEntityDef departmentDef = AtlasTypeUtil.createClassTypeDef(DEPARTMENT_TYPE, getDescription(DEPARTMENT_TYPE), Collections.emptySet(),
                AtlasTypeUtil.createRequiredAttrDef("name", "string"), AtlasTypeUtil.createOptionalAttrDef("count", "int"));

        AtlasEntityDef addressDef = AtlasTypeUtil.createClassTypeDef(ADDRESS_TYPE, getDescription(ADDRESS_TYPE), Collections.emptySet(),
                AtlasTypeUtil.createOptionalAttrDef("street", "string"), AtlasTypeUtil.createRequiredAttrDef("city", "string"),
                AtlasTypeUtil.createRequiredAttrDef("state", "string"), AtlasTypeUtil.createOptionalAttrDef("zip", "int"));

        createTypes(new ArrayList<>(Arrays.asList(phoneDef, employeeDef, departmentDef, addressDef)));
    }

    private void createRelationshipTypes() throws AtlasBaseException {
        AtlasRelationshipDef deptEmployeeRelationDef = new AtlasRelationshipDef(DEPT_EMPLOYEE_RELATION_TYPE, getDescription(DEPT_EMPLOYEE_RELATION_TYPE), "1.0",
                RelationshipCategory.ASSOCIATION, PropagateTags.ONE_TO_TWO, new AtlasRelationshipEndDef(EMPLOYEE_TYPE, "department", Cardinality.SINGLE),
                new AtlasRelationshipEndDef(DEPARTMENT_TYPE, "employees", Cardinality.SET));

        AtlasRelationshipDef employeeAddrRelationDef = new AtlasRelationshipDef(EMPLOYEE_ADDRESS_RELATION_TYPE, getDescription(EMPLOYEE_ADDRESS_RELATION_TYPE), "1.0",
                RelationshipCategory.ASSOCIATION, PropagateTags.ONE_TO_TWO, new AtlasRelationshipEndDef(EMPLOYEE_TYPE, "address", Cardinality.SINGLE),
                new AtlasRelationshipEndDef(ADDRESS_TYPE, "employees", Cardinality.SET));

        createTypes(new ArrayList<>(Arrays.asList(deptEmployeeRelationDef, employeeAddrRelationDef)));
    }

    private void createType(AtlasBaseTypeDef typeDef) throws AtlasBaseException {
        createTypes(new ArrayList<>(Collections.singletonList(typeDef)));
    }

    private void createTypes(List<? extends AtlasBaseTypeDef> typeDefs) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.lockTypeRegistryForUpdate();

        ttr.addTypes(typeDefs);

        typeRegistry.releaseTypeRegistryForUpdate(ttr, true);
    }

    private String getDescription(String typeName) {
        return typeName + " description";
    }

    private Map<String, Map<String, AtlasAttribute>> getRelationAttrsForType(String typeName) {
        return typeRegistry.getEntityTypeByName(typeName).getRelationshipAttributes();
    }

    private Map<String, AtlasAttribute> getAttrsForType(String typeName) {
        return typeRegistry.getEntityTypeByName(typeName).getAllAttributes();
    }
}
