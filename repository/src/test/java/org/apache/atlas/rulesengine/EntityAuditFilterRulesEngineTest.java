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

import com.google.gson.JsonParser;
import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_DELETE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for custom entity audit filter rule evaluation and JsonLogic conversion.
 */
public class EntityAuditFilterRulesEngineTest {
    private AtlasRulesEngine rulesEngine;

    @BeforeClass
    public void setUp() throws Exception {
        rulesEngine = EntityAuditFilterTestSupport.createRulesEngineWithStandardRules();
    }

    @Test
    public void testStandardRulesEvaluation() throws Exception {
        // rule_1: hive_table,hive_db + name containsIgnoreCase temp
        assertAccepted("hive_table", "temp_table", ENTITY_CREATE, false);
        assertAccepted("hdfs_path", "temp_path", ENTITY_CREATE, true);

        // rule_2: spark_table name starts or ends with test (UPDATE)
        assertAccepted("spark_table", "sptable", ENTITY_CREATE, true);
        assertAccepted("spark_table", "test_sptable", ENTITY_UPDATE, false);

        // rule_3: hive* ENTITY_DELETE / CLASSIFICATION_DELETE
        assertAccepted("hive_db", "db01", ENTITY_DELETE, false);
        assertAccepted("hive_table", "tbl01", CLASSIFICATION_DELETE, false);

        // rule_4: Asset + subtypes, name contains demo
        assertAccepted("hive_table", "demo_table", ENTITY_UPDATE, false);

        // rule_5: spark_process type-only discard
        assertAccepted("spark_process", "sp_name", ENTITY_CREATE, false);

        // default ACCEPT when no rule matches
        assertAccepted("spark_table", "unmatched", ENTITY_CREATE, true);
    }

    @Test
    public void testJsonLogicEncodesStringAttributeValues() throws Exception {
        String jsonLogicTemp = EntityAuditFilterTestSupport.convertRuleToJsonLogic(
                EntityAuditFilterTestSupport.nameContainsIgnoreCaseRule("temp"));
        assertTrue(jsonLogicTemp.contains("\"temp\""), "Expected quoted temp; got: " + jsonLogicTemp);
        new JsonParser().parse(jsonLogicTemp);

        String jsonLogicSpaced = EntityAuditFilterTestSupport.convertRuleToJsonLogic(
                EntityAuditFilterTestSupport.nameContainsIgnoreCaseRule("my temp"));
        assertTrue(jsonLogicSpaced.contains("\"my temp\""), "Expected quoted spaced value; got: " + jsonLogicSpaced);
        new JsonParser().parse(jsonLogicSpaced);
    }

    private void assertAccepted(String typeName, String name, EntityAuditActionV2 action,
            boolean expectAccepted) throws Exception {
        Map<String, Object> event = EntityAuditFilterTestSupport.auditEvent(typeName, name, action);
        if (expectAccepted) {
            assertTrue(rulesEngine.accept(event), "Expected accept: " + event);
        } else {
            assertFalse(rulesEngine.accept(event), "Expected discard: " + event);
        }
    }
}
