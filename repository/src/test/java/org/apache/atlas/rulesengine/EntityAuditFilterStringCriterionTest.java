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
import io.github.jamsesso.jsonlogic.JsonLogic;
import org.apache.atlas.model.instance.AtlasRule;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Regression tests for string {@code attributeValue} in rule criteria (JsonLogic must JSON-encode values).
 *
 * <p>Broken conversion emits {@code ..., temp]} instead of {@code ..., "temp"]}, so rules fail silently
 * and audits are stored when they should be discarded.
 */
public class EntityAuditFilterStringCriterionTest {
    /**
     * rule_1-style criterion: {@code name} {@code containsIgnoreCase} {@code "temp"} on hive types.
     */
    @Test
    public void testJsonLogicQuotesTempInContainsIgnoreCaseCriterion() throws Exception {
        AtlasRule rule      = EntityAuditFilterTestSupport.nameContainsIgnoreCaseTempRule();
        String    jsonLogic = EntityAuditFilterTestSupport.convertRuleToJsonLogic(rule);

        assertTrue(jsonLogic.contains("\"temp\""),
                "JsonLogic must JSON-quote string attributeValue; got: " + jsonLogic);

        JsonLogic jsonLogicEngine = registerCustomOperations(new JsonLogic());
        Map<String, Object> event = EntityAuditFilterTestSupport.auditEvent("hive_table", "temp_orders", ENTITY_CREATE);
        assertTrue((boolean) jsonLogicEngine.apply(jsonLogic, event));
    }

    /**
     * rule_3-style criterion: {@code operationType} {@code ==} {@code "ENTITY_DELETE"} on {@code hive*}.
     */
    @Test
    public void testJsonLogicQuotesEntityDeleteInOperationTypeCriterion() throws Exception {
        AtlasRule rule      = EntityAuditFilterTestSupport.hiveEntityDeleteOperationTypeRule();
        String    jsonLogic = EntityAuditFilterTestSupport.convertRuleToJsonLogic(rule);

        assertTrue(jsonLogic.contains("\"ENTITY_DELETE\""),
                "JsonLogic must JSON-quote operationType value; got: " + jsonLogic);

        JsonLogic jsonLogicEngine = registerCustomOperations(new JsonLogic());
        Map<String, Object> event = EntityAuditFilterTestSupport.auditEvent("hive_db", "warehouse", ENTITY_DELETE);
        assertTrue((boolean) jsonLogicEngine.apply(jsonLogic, event));
    }

    @Test
    public void testNameContainsIgnoreCaseTempRuleDiscardsMatchingAudit() throws Exception {
        AtlasRulesEngine engine = EntityAuditFilterTestSupport.createRulesEngineWithRules(
                Collections.singletonList(EntityAuditFilterTestSupport.nameContainsIgnoreCaseTempRule()));

        Map<String, Object> event = EntityAuditFilterTestSupport.auditEvent("hive_table", "temp_orders", ENTITY_CREATE);
        assertFalse(engine.accept(event), "Audit should be discarded when name contains temp");
    }

    @Test
    public void testOperationTypeEntityDeleteRuleDiscardsMatchingAudit() throws Exception {
        AtlasRulesEngine engine = EntityAuditFilterTestSupport.createRulesEngineWithRules(
                Collections.singletonList(EntityAuditFilterTestSupport.hiveEntityDeleteOperationTypeRule()));

        Map<String, Object> event = EntityAuditFilterTestSupport.auditEvent("hive_db", "warehouse", ENTITY_DELETE);
        assertFalse(engine.accept(event), "Audit should be discarded for hive_db ENTITY_DELETE");
    }

    /** Unquoted values with spaces break JsonLogic parse (pre-P0-2); must be JSON-encoded. */
    @Test
    public void testJsonLogicEncodesAttributeValueWithSpaces() throws Exception {
        String              needle    = "my temp";
        AtlasRule           rule      = EntityAuditFilterTestSupport.nameContainsIgnoreCaseNeedleRule(needle);
        String              jsonLogic = EntityAuditFilterTestSupport.convertRuleToJsonLogic(rule);
        JsonLogic           engine    = registerCustomOperations(new JsonLogic());

        assertTrue(jsonLogic.contains("\"my temp\""), "JsonLogic must quote spaced value; got: " + jsonLogic);
        new JsonParser().parse(jsonLogic);

        Map<String, Object> event = EntityAuditFilterTestSupport.auditEvent("hive_table", "orders_my temp_x", ENTITY_CREATE);
        assertTrue((boolean) engine.apply(jsonLogic, event));
    }

    @Test
    public void testNameContainsNeedleWithSpacesDiscardsMatchingAudit() throws Exception {
        AtlasRulesEngine engine = EntityAuditFilterTestSupport.createRulesEngineWithRules(
                Collections.singletonList(EntityAuditFilterTestSupport.nameContainsIgnoreCaseNeedleRule("my temp")));

        Map<String, Object> event = EntityAuditFilterTestSupport.auditEvent("hive_table", "orders_my temp_x", ENTITY_CREATE);
        assertFalse(engine.accept(event), "Audit should be discarded when name contains spaced needle");
    }

    @Test
    public void testJsonLogicEncodesAttributeValueWithEmbeddedQuote() throws Exception {
        String    needle    = "say\"hi";
        AtlasRule rule      = EntityAuditFilterTestSupport.nameContainsIgnoreCaseNeedleRule(needle);
        // Jackson-shaped ruleExpr JSON (as stored on entity); json-simple cannot round-trip embedded quotes.
        String    jsonLogic = EntityAuditFilterTestSupport.convertRuleToJsonLogic(rule, true);

        assertTrue(jsonLogic.contains("\\\"hi"), "JsonLogic must escape embedded quote; got: " + jsonLogic);
        new JsonParser().parse(jsonLogic);

        JsonLogic engine = registerCustomOperations(new JsonLogic());
        Map<String, Object> event = EntityAuditFilterTestSupport.auditEvent("hive_table", "prefix_say\"hi_suffix", ENTITY_CREATE);
        assertTrue((boolean) engine.apply(jsonLogic, event));
    }

    private static JsonLogic registerCustomOperations(JsonLogic jsonLogic) {
        jsonLogic.addOperation("isNull", AtlasRuleUtils.isNullFunc);
        jsonLogic.addOperation("notNull", AtlasRuleUtils.notNullFunc);
        jsonLogic.addOperation("startsWith", AtlasRuleUtils.startsWithFunc);
        jsonLogic.addOperation("endsWith", AtlasRuleUtils.endsWithFunc);
        jsonLogic.addOperation("contains", AtlasRuleUtils.containsFunc);
        jsonLogic.addOperation("notContains", AtlasRuleUtils.notContainsFunc);
        jsonLogic.addOperation("containsIgnoreCase", AtlasRuleUtils.containsIgnoreCaseFunc);
        jsonLogic.addOperation("notContainsIgnoreCase", AtlasRuleUtils.notContainsIgnoreCaseFunc);
        return jsonLogic;
    }
}
