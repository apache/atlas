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
package org.apache.atlas.web.filters;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.TestModules;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRule;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.javatuples.Triplet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_ADD;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_DELETE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;

@Guice(modules = TestModules.TestOnlyModule.class)
public class CAFTests {
    public static final String                                                     ATLAS_REST_ADDRESS              = "atlas.rest.address";
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_NAME_CONTAINS_TEMP    = Triplet.with("name", AtlasRule.RuleExprObject.Operator.CONTAINS_IGNORECASE, "temp");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_NAME_STARTS_WITH_TEST = Triplet.with("name", AtlasRule.RuleExprObject.Operator.STARTS_WITH, "test");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_NAME_STARTS_WITH_DEMO = Triplet.with("name", AtlasRule.RuleExprObject.Operator.CONTAINS, "demo");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_NAME_ENDS_WITH_TEST   = Triplet.with("name", AtlasRule.RuleExprObject.Operator.ENDS_WITH, "test");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_DELETE_CLASSIFICATION = Triplet.with("operationType", AtlasRule.RuleExprObject.Operator.EQ, "CLASSIFICATION_DELETE");
    static final        Triplet<String, AtlasRule.RuleExprObject.Operator, String> CRITERION_ENTITY_DELETED        = Triplet.with("operationType", AtlasRule.RuleExprObject.Operator.EQ, "ENTITY_DELETE");

    protected           AtlasClientV2                                              atlasClientV2;
    final               List<String>                                               allRuleGuids                    = new ArrayList<>();
    final               List<String>                                               allEntityGuids                  = new ArrayList<>();

    @BeforeClass
    public void setUp() throws Exception {
        String[] atlasUrls = ApplicationProperties.get().getStringArray(ATLAS_REST_ADDRESS);
        if (atlasUrls == null || atlasUrls.length == 0) {
            atlasUrls = new String[] {"http://localhost:21000/"};
        }

        atlasClientV2 = AuthenticationUtil.isKerberosAuthenticationEnabled()
                ? new AtlasClientV2(atlasUrls)
                : new AtlasClientV2(atlasUrls, new String[] {"admin", "admin"});

        setupRules();
    }

    private void setupRules() {
        try {
            // Rule 1: Discard all audits for hive_table,hive_db if name contains "temp"(Note support for CSV of type-names)
            createRule("rule_1", "hive_table,hive_db", false, CRITERION_NAME_CONTAINS_TEMP);

            // Rule 2: Discard audits for spark_table if name starts or ends with "test"
            createRule("rule_2", "spark_table", false, AtlasRule.Condition.OR, CRITERION_NAME_STARTS_WITH_TEST, CRITERION_NAME_ENDS_WITH_TEST);

            // Rule 3: Discard audits for hive* (wildcard support) on classification delete or entity delete
            createRule("rule_3", "hive*", false, AtlasRule.Condition.OR, CRITERION_ENTITY_DELETED, CRITERION_DELETE_CLASSIFICATION);

            // Rule 4: Discard audits for Asset and subtypes where name starts with "demo"
            createRule("rule_4", "Asset", true, CRITERION_NAME_STARTS_WITH_DEMO);

            // Rule 5: Discard all audits by default for spark_process - all operations/events
            createRule("rule_5", "spark_process", false);
        } catch (AtlasServiceException e) {
            fail("Failed to set up rules: " + e.getMessage(), e);
        }
    }

    @SafeVarargs
    private final void createRule(String ruleName,
            String typeName,
            boolean includeSubtypes,
            Triplet<String, AtlasRule.RuleExprObject.Operator, String>... criteria) throws AtlasServiceException {
        createRule(ruleName, typeName, includeSubtypes, null, criteria);
    }

    @SafeVarargs
    private final void createRule(String ruleName,
            String typeName,
            boolean includeSubtypes,
            AtlasRule.Condition condition,
            Triplet<String, AtlasRule.RuleExprObject.Operator, String>... criteria) throws AtlasServiceException {
        if (condition != null && (criteria == null || criteria.length == 0)) {
            throw new IllegalArgumentException("Criteria must not be null or empty");
        }

        List<AtlasRule.RuleExprObject> ruleExprObjects = new ArrayList<>();

        if (condition != null && criteria.length > 1) {
            ruleExprObjects.add(getNestedRuleExprObject(typeName, includeSubtypes, condition, Arrays.asList(criteria)));
        } else {
            for (Triplet<String, AtlasRule.RuleExprObject.Operator, String> criterion : criteria) {
                ruleExprObjects.add(getSimpleRuleExprObject(typeName, includeSubtypes, criterion));
            }
        }

        AtlasRule.RuleExpr ruleExpr = new AtlasRule.RuleExpr(ruleExprObjects);

        AtlasRule atlasRule = new AtlasRule();
        atlasRule.setAction("DISCARD");
        atlasRule.setRuleName(ruleName);
        atlasRule.setRuleExpr(ruleExpr);

        AtlasRule createdRule = atlasClientV2.createRule(atlasRule);
        saveRuleGuid(createdRule.getRuleName());
    }

    private void createRule(String ruleName, String typeName, boolean includeSubtypes) throws AtlasServiceException {
        // Creates a basic rule with no filtering logic (no conditions and no criteria)
        AtlasRule.RuleExprObject ruleExprObject = getSimpleRuleExprObject(typeName, includeSubtypes);
        AtlasRule.RuleExpr ruleExpr = new AtlasRule.RuleExpr(Collections.singletonList(ruleExprObject));

        AtlasRule atlasRule = new AtlasRule();
        atlasRule.setAction("DISCARD");
        atlasRule.setRuleName(ruleName);
        atlasRule.setRuleExpr(ruleExpr);

        AtlasRule rule = atlasClientV2.createRule(atlasRule);
        saveRuleGuid(rule.getRuleName());
    }

    private AtlasRule.RuleExprObject getSimpleRuleExprObject(String typeName, boolean includeSubTypes) {
        return new AtlasRule.RuleExprObject(typeName, includeSubTypes);
    }

    private AtlasRule.RuleExprObject getSimpleRuleExprObject(String typeName, boolean includeSubTypes, String attributeName, AtlasRule.RuleExprObject.Operator operator, String attributeValue) {
        return new AtlasRule.RuleExprObject(typeName, attributeName, operator, attributeValue, includeSubTypes);
    }

    private AtlasRule.RuleExprObject getSimpleRuleExprObject(String typeName, boolean includeSubTypes, Triplet<String, AtlasRule.RuleExprObject.Operator, String> criterionTriplet) {
        return getSimpleRuleExprObject(typeName, includeSubTypes, criterionTriplet.getValue0(), criterionTriplet.getValue1(), criterionTriplet.getValue2());
    }

    private AtlasRule.RuleExprObject getNestedRuleExprObject(String typeName, boolean includeSubTypes, AtlasRule.Condition condition, List<Triplet<String, AtlasRule.RuleExprObject.Operator, String>> criterion) {
        return new AtlasRule.RuleExprObject(typeName, condition, transform(criterion), includeSubTypes);
    }

    private List<AtlasRule.RuleExprObject.Criterion> transform(List<Triplet<String, AtlasRule.RuleExprObject.Operator, String>> criterion) {
        return criterion.stream().map(criteriatriplet -> new AtlasRule.RuleExprObject.Criterion(criteriatriplet.getValue1(), criteriatriplet.getValue0(), criteriatriplet.getValue2())).collect(Collectors.toList());
    }

    private void saveRuleGuid(String ruleName) throws AtlasServiceException {
        Map<String, String> attributes = Collections.singletonMap("ruleName", ruleName);
        AtlasEntity         ruleEntity = atlasClientV2.getEntityByAttribute("__AtlasRule", attributes).getEntity();
        allRuleGuids.add(ruleEntity.getGuid());
    }

    private AtlasEntity createEntity(String typeName, String entityName) throws AtlasServiceException {
        Map<String, Object> attrMap = new HashMap<>();
        if (typeName.equals("hdfs_path")) {
            attrMap.put("path", entityName + "_path");
        } else if (typeName.equals("hive_db")) {
            attrMap.put("clusterName", "cl1");
        }
        return createEntity(typeName, entityName, attrMap);
    }

    private AtlasEntity createEntity(String typeName, String name, Map<String, Object> additionalAttrs) throws AtlasServiceException {
        AtlasEntity entity = new AtlasEntity(typeName);
        entity.setAttribute("name", name);
        entity.setAttribute("qualifiedName", "q" + name + randomString());
        additionalAttrs.forEach(entity::setAttribute);

        EntityMutationResponse response = atlasClientV2.createEntity(new AtlasEntity.AtlasEntityWithExtInfo(entity));
        String                 guid     = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).get(0).getGuid();
        entity.setGuid(guid);
        allEntityGuids.add(guid);
        return entity;
    }

    protected String randomString() {
        return RandomStringUtils.randomAlphabetic(1) + RandomStringUtils.randomAlphanumeric(9);
    }

    private void updateEntityName(AtlasEntity entity, String newName) throws AtlasServiceException {
        entity.setAttribute("name", newName);
        atlasClientV2.updateEntity(new AtlasEntity.AtlasEntityWithExtInfo(entity));
    }

    private String createClassification(String guid, String classificationName) throws AtlasServiceException {
        String                 clName = classificationName + randomString();
        AtlasClassificationDef clDef  = AtlasTypeUtil.createTraitTypeDef(clName, Collections.emptySet());
        atlasClientV2.createAtlasTypeDefs(new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.singletonList(clDef), Collections.emptyList()));
        atlasClientV2.addClassifications(guid, Collections.singletonList(new AtlasClassification(clDef.getName())));
        return clName;
    }

    public void removeEntityClassification(AtlasEntity entity, String classificationName) throws AtlasServiceException {
        Map<String, String> attrMap = new HashMap<String, String>() {
            {
                put("qualifiedName", (String) entity.getAttribute("qualifiedName"));
            }
        };
        atlasClientV2.removeClassification(entity.getTypeName(), attrMap, classificationName);
    }

    private void assertEntityAudits(String guid, int expectedCount) throws AtlasServiceException {
        assertEntityAudits(guid, null, expectedCount);
    }

    private void assertEntityAudits(String guid, EntityAuditEventV2.EntityAuditActionV2 action, int expectedCount) throws AtlasServiceException {
        List<EntityAuditEventV2> events = atlasClientV2.getAuditEvents(guid, "", action, (short) 100);
        assertEquals("Unexpected audit count for: " + guid, expectedCount, events.size());
    }

    @DataProvider(name = "nameContainsTempEntitiesProvider")
    public Object[][] nameContainsTempEntitiesProvider() {
        return new Object[][] {
                // {typeName, name, attributes, expectedAuditCount}
                {"hive_table", "temp_table", 0},
                {"hive_db", "db01_temp", 0},
                {"hdfs_path", "temp_path", 1}
        };
    }

    @Test(dataProvider = "nameContainsTempEntitiesProvider")
    public void test_DiscardAuditsIfNameContainsTemp(String typeName, String name, int expectedAuditCount) throws AtlasServiceException {
        AtlasEntity entity = createEntity(typeName, name);
        assertEntityAudits(entity.getGuid(), expectedAuditCount);

        // Only check update audit for types that had ENTITY_CREATE discarded (to ensure rule still applies)
        if (expectedAuditCount == 0) {
            updateEntityName(entity, entity.getAttribute("name") + randomString());
            assertEntityAudits(entity.getGuid(), ENTITY_UPDATE, 0);
        }
    }

    @DataProvider(name = "nameUpdateScenariosForAuditFiltering")
    public Object[][] nameUpdateScenariosForAuditFiltering() {
        return new Object[][] {
                {"test_", "",  0},      // starts with test; Should be discarded
                {"prefix_notest_", "_notest_suffix", 1},    // neither starts nor ends with test; Should be accepted
                {"", "_midname_test", 0}    // ends with test; Should be discarded
        };
    }

    @Test(dataProvider = "nameUpdateScenariosForAuditFiltering")
    public void test_DiscardUpdateIfNameStartsOrEndsWithTest(String newNamePrefix, String newNameSuffix, int expectedUpdateAudits) throws Exception {
        AtlasEntity entity = createEntity("spark_table", "sptable");
        assertEntityAudits(entity.getGuid(), 1); // One CREATE audit

        updateEntityName(entity, newNamePrefix + entity.getAttribute("name") + newNameSuffix);
        assertEntityAudits(entity.getGuid(), ENTITY_UPDATE, expectedUpdateAudits);

        // Check total audit events (CREATE + expected updates)
        assertEntityAudits(entity.getGuid(), 1 + expectedUpdateAudits);
    }

    @Test
    public void test_DiscardSpecificAuditsForAllTypesUnderHiveHook() throws AtlasServiceException {
        //Discard audits when entity is updated
        AtlasEntity dbEntity = createEntity("hive_db", "db01");
        assertEntityAudits(dbEntity.getGuid(), 1);

        EntityMutationResponse deleteResponse = atlasClientV2.deleteEntitiesByGuids(Collections.singletonList(dbEntity.getGuid()));
        allEntityGuids.remove(dbEntity.getGuid());
        assertNotNull(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE));
        assertTrue(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size() > 0);
        assertEntityAudits(dbEntity.getGuid(), ENTITY_DELETE, 0);

        //Discard audits when classification is removed
        AtlasEntity tblEntity = createEntity("hive_table", "tbl01");
        assertEntityAudits(tblEntity.getGuid(), 1);

        String clName = createClassification(tblEntity.getGuid(), "class01");
        assertEntityAudits(tblEntity.getGuid(), CLASSIFICATION_ADD, 1);

        try {
            Thread.sleep(1000); //hits error "ATLAS-406-00-001" otherwise
        } catch (InterruptedException ignored) {
        }

        removeEntityClassification(tblEntity, clName);
        assertEntityAudits(tblEntity.getGuid(), CLASSIFICATION_DELETE, 0);
    }

    @Test
    public void test_DiscardAuditsForTypeAndSubtypes() throws AtlasServiceException {
        //Discard all entity audits for type Asset and its subtypes
        AtlasEntity tblEntity = createEntity("hive_table", "DEMO_table");
        assertEntityAudits(tblEntity.getGuid(), ENTITY_CREATE, 1);

        updateEntityName(tblEntity, "demo_table");
        assertEntityAudits(tblEntity.getGuid(), ENTITY_UPDATE, 0);

        AtlasEntity processEntity = createEntity("Process", "demo_process");
        assertEntityAudits(processEntity.getGuid(), ENTITY_CREATE, 0);
    }

    @Test
    public void test_DefaultDiscardRuleApplied() throws AtlasServiceException {
        //Discards all audits by default for spark_process
        AtlasEntity entity = createEntity("spark_process", "sp_name");
        assertEntityAudits(entity.getGuid(), ENTITY_CREATE, 0);

        updateEntityName(entity, "sp_name_new");
        assertEntityAudits(entity.getGuid(), ENTITY_UPDATE, 0);

        createClassification(entity.getGuid(), "class02");
        assertEntityAudits(entity.getGuid(), CLASSIFICATION_ADD, 0);
    }

    @DataProvider(name = "unmatchedRuleEntitiesProvider")
    public Object[][] unmatchedRuleEntitiesProvider() {
        return new Object[][] {
                {"hive_table", "unmatched_table"},
                {"hive_db", "unmatched_db"},
                {"hdfs_path", "unmatched_hdfs"},
                {"spark_table", "uniqueSparkTable"}
        };
    }

    @Test(dataProvider = "unmatchedRuleEntitiesProvider")
    public void test_UnmatchedRuleAuditNotDiscarded(String typeName, String namePrefix) throws AtlasServiceException {
       //verifies the default behavior of the audit filter logic.
        String entityName = namePrefix + "_" + randomString();
        Map<String, Object> attrMap = new HashMap<>();
        if (typeName.equals("hdfs_path")) {
            attrMap.put("path", entityName + "_path");
        } else if (typeName.equals("hive_db")) {
            attrMap.put("clusterName", "cl1");
        }

        AtlasEntity entity = createEntity(typeName, entityName, attrMap);
        assertNotNull(entity);

        // ENTITY_CREATE audit is expected to be generated
        assertEntityAudits(entity.getGuid(), ENTITY_CREATE, 1);
    }

    @AfterClass
    public void teardown() throws Exception {
        for (String guid : allRuleGuids) {
            EntityMutationResponse resp = atlasClientV2.deleteRuleByGuid(guid);
            assertEquals(1, resp.getDeletedEntities().size());
        }

        if (!allEntityGuids.isEmpty()) {
            EntityMutationResponse deleteResponse = atlasClientV2.deleteEntitiesByGuids(allEntityGuids);
            assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), allEntityGuids.size());
        }

        AtlasGraphProvider.cleanup();
    }
}
