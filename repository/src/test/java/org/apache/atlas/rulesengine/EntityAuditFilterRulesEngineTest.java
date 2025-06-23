/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.rulesengine;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_ADD;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_DELETE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit-test mirror of {@link org.apache.atlas.web.filters.EntityAuditFilterIT}.
 *
 * <p>IT tests assert persisted audit counts via REST; these tests assert the same outcomes by
 * evaluating {@link AtlasRulesEngine#accept(Map)} on synthetic audit payloads (accept = store audit).
 */
public class EntityAuditFilterRulesEngineTest {
    private AtlasRulesEngine rulesEngine;

    @BeforeClass
    public void setUp() throws Exception {
        rulesEngine = EntityAuditFilterTestSupport.createRulesEngineWithStandardRules();
    }

    @DataProvider(name = "nameContainsTempEntitiesProvider")
    public Object[][] nameContainsTempEntitiesProvider() {
        return new Object[][] {
                {"hive_table", "temp_table", false},
                {"hive_db", "db01_temp", false},
                {"hdfs_path", "temp_path", true}
        };
    }

    /**
     * IT: {@code test_DiscardAuditsIfNameContainsTemp} — rule_1 CSV types + name containsIgnoreCase temp.
     */
    @Test(dataProvider = "nameContainsTempEntitiesProvider")
    public void testDiscardAuditsIfNameContainsTemp(String typeName, String name, boolean expectAuditAccepted)
            throws Exception {
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent(typeName, name, ENTITY_CREATE),
                expectAuditAccepted);

        if (!expectAuditAccepted) {
            assertAuditAccepted(
                    EntityAuditFilterTestSupport.auditEvent(typeName, name + "_updated", ENTITY_UPDATE),
                    false);
        }
    }

    @DataProvider(name = "nameUpdateScenariosForAuditFiltering")
    public Object[][] nameUpdateScenariosForAuditFiltering() {
        return new Object[][] {
                {"test_", "", false},
                {"prefix_notest_", "_notest_suffix", true},
                {"", "_midname_test", false}
        };
    }

    /**
     * IT: {@code test_DiscardUpdateIfNameStartsOrEndsWithTest} — rule_2 on spark_table updates.
     */
    @Test(dataProvider = "nameUpdateScenariosForAuditFiltering")
    public void testDiscardUpdateIfNameStartsOrEndsWithTest(String newNamePrefix, String newNameSuffix,
            boolean expectUpdateAuditAccepted) throws Exception {
        String baseName = "sptable";
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("spark_table", baseName, ENTITY_CREATE),
                true);

        String updatedName = newNamePrefix + baseName + newNameSuffix;
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("spark_table", updatedName, ENTITY_UPDATE),
                expectUpdateAuditAccepted);
    }

    /**
     * IT: {@code test_DiscardSpecificAuditsForAllTypesUnderHiveHook} — rule_3 hive* + operationType.
     */
    @Test
    public void testDiscardSpecificAuditsForHiveTypesOnDeleteOperations() throws Exception {
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("hive_db", "db01", ENTITY_CREATE),
                true);
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("hive_db", "db01", ENTITY_DELETE),
                false);

        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("hive_table", "tbl01", ENTITY_CREATE),
                true);
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("hive_table", "tbl01", CLASSIFICATION_ADD),
                true);
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("hive_table", "tbl01", CLASSIFICATION_DELETE),
                false);
    }

    /**
     * IT: {@code test_DiscardAuditsForTypeAndSubtypes} — rule_4 Asset + subtypes, name contains demo.
     */
    @Test
    public void testDiscardAuditsForTypeAndSubtypes() throws Exception {
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("hive_table", "DEMO_table", ENTITY_CREATE),
                true);
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("hive_table", "demo_table", ENTITY_UPDATE),
                false);
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("Process", "demo_process", ENTITY_CREATE),
                false);
    }

    /**
     * IT: {@code test_DefaultDiscardRuleApplied} — rule_5 type-only spark_process.
     */
    @Test
    public void testDefaultDiscardRuleAppliedForSparkProcess() throws Exception {
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("spark_process", "sp_name", ENTITY_CREATE),
                false);
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("spark_process", "sp_name_new", ENTITY_UPDATE),
                false);
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent("spark_process", "sp_name", CLASSIFICATION_ADD),
                false);
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

    /**
     * IT: {@code test_UnmatchedRuleAuditNotDiscarded} — names/types that should not match discard rules.
     */
    @Test(dataProvider = "unmatchedRuleEntitiesProvider")
    public void testUnmatchedRuleAuditNotDiscarded(String typeName, String entityName) throws Exception {
        assertAuditAccepted(
                EntityAuditFilterTestSupport.auditEvent(typeName, entityName, ENTITY_CREATE),
                true);
    }

    private void assertAuditAccepted(Map<String, Object> event, boolean expectAccepted) throws Exception {
        if (expectAccepted) {
            assertTrue(rulesEngine.accept(event), "Expected audit to be accepted (stored): " + event);
        } else {
            assertFalse(rulesEngine.accept(event), "Expected audit to be discarded (not stored): " + event);
        }
    }
}
