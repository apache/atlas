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

import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;
import org.apache.atlas.model.instance.AtlasRule;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.atlas.rulesengine.AtlasEntityAuditFilterService.ATTR_OPERATION_TYPE;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Builds the same five audit filter rules as {@code EntityAuditFilterIT} and provides
 * a rules engine backed by an in-memory type registry (no Atlas server).
 */
final class EntityAuditFilterTestSupport {
    static final String CRITERION_ATTR_NAME                 = "name";
    static final String CRITERION_ATTR_OPERATION            = "operationType";
    static final String CRITERION_VAL_TEMP                  = "temp";
    static final String CRITERION_VAL_TEST                  = "test";
    static final String CRITERION_VAL_DEMO                  = "demo";
    static final String CRITERION_VAL_ENTITY_DELETE         = "ENTITY_DELETE";
    static final String CRITERION_VAL_CLASSIFICATION_DELETE = "CLASSIFICATION_DELETE";

    private static final AtomicLong RULE_CREATED_TIME = new AtomicLong(1_000L);

    private EntityAuditFilterTestSupport() {
    }

    static AtlasRulesEngine createRulesEngineWithStandardRules() throws Exception {
        AtlasTypeRegistry                   registry       = createTypeRegistry();
        AtlasEntityAuditFilterService       delegate       = new AtlasEntityAuditFilterService(null, registry, null);
        AtlasEntityAuditFilterService.JsonLogicConverter jsonLogicConverter = newJsonLogicConverter(delegate);

        AtlasEntityAuditFilterService auditFilterService = mock(AtlasEntityAuditFilterService.class);
        when(auditFilterService.fetchRules()).thenReturn(buildStandardRules());
        when(auditFilterService.getDefaultAction()).thenReturn(AtlasRuleUtils.getRuleActionFromString("ACCEPT"));
        when(auditFilterService.getJsonLogicConverter()).thenReturn(jsonLogicConverter);

        return new AtlasRulesEngine(auditFilterService);
    }

    static AtlasRulesEngine createRulesEngineWithRules(List<AtlasRule> rules) throws Exception {
        AtlasTypeRegistry registry = createTypeRegistry();
        AtlasEntityAuditFilterService delegate = new AtlasEntityAuditFilterService(null, registry, null);
        AtlasEntityAuditFilterService.JsonLogicConverter jsonLogicConverter = newJsonLogicConverter(delegate);

        AtlasEntityAuditFilterService auditFilterService = mock(AtlasEntityAuditFilterService.class);
        when(auditFilterService.fetchRules()).thenReturn(AtlasRuleUtils.getSortedRules(rules));
        when(auditFilterService.getDefaultAction()).thenReturn(AtlasRuleUtils.getRuleActionFromString("ACCEPT"));
        when(auditFilterService.getJsonLogicConverter()).thenReturn(jsonLogicConverter);

        return new AtlasRulesEngine(auditFilterService);
    }

    static String convertRuleToJsonLogic(AtlasRule rule) throws Exception {
        return convertRuleToJsonLogic(rule, false);
    }

    /** Uses {@link AtlasRuleUtils#getRuleExprJsonString} (json-simple), same as {@link AtlasRulesEngine} at runtime. */
    static String convertRuleToJsonLogic(AtlasRule rule, boolean useAtlasJsonForRuleExpr) throws Exception {
        AtlasEntityAuditFilterService delegate = new AtlasEntityAuditFilterService(null, createTypeRegistry(), null);
        String ruleExprJson = useAtlasJsonForRuleExpr
                ? AtlasType.toJson(rule.getRuleExpr().getRuleExprObjList())
                : AtlasRuleUtils.getRuleExprJsonString(rule.getRuleExpr());
        return newJsonLogicConverter(delegate).convertToJsonLogic(ruleExprJson);
    }

    /** P0-2 example: string {@code attributeValue} {@code temp} with {@code containsIgnoreCase}. */
    static AtlasRule nameContainsIgnoreCaseTempRule() {
        return nameContainsIgnoreCaseNeedleRule(CRITERION_VAL_TEMP);
    }

    static AtlasRule nameContainsIgnoreCaseNeedleRule(String needle) {
        return rule("p0_2_name_contains_" + needle.hashCode(), "DISCARD",
                criterion("hive_table,hive_db", CRITERION_ATTR_NAME,
                        AtlasRule.RuleExprObject.Operator.CONTAINS_IGNORECASE, needle, false));
    }

    /** P0-2 example: string {@code attributeValue} {@code ENTITY_DELETE} with {@code operationType} EQ. */
    static AtlasRule hiveEntityDeleteOperationTypeRule() {
        return rule("p0_2_entity_delete_op", "DISCARD",
                nestedCriterion("hive*", AtlasRule.Condition.OR, false,
                        criterionLeaf(CRITERION_ATTR_OPERATION, AtlasRule.RuleExprObject.Operator.EQ,
                                CRITERION_VAL_ENTITY_DELETE)));
    }

    static Map<String, Object> auditEvent(String typeName, String name, EntityAuditActionV2 action) {
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("name", name);
        attributes.put("qualifiedName", "unit.test." + name + "@cl1");

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("typeName", typeName);
        event.put("guid", "unit-test-" + typeName + "-" + name);
        event.put("attributes", attributes);
        event.put(ATTR_OPERATION_TYPE, action.name());
        return event;
    }

    static List<AtlasRule> buildStandardRules() {
        List<AtlasRule> rules = new ArrayList<>();

        rules.add(rule("rule_1", "DISCARD",
                criterion("hive_table,hive_db", CRITERION_ATTR_NAME,
                        AtlasRule.RuleExprObject.Operator.CONTAINS_IGNORECASE, CRITERION_VAL_TEMP, false)));

        rules.add(rule("rule_2", "DISCARD",
                nestedCriterion("spark_table", AtlasRule.Condition.OR, false,
                        criterionLeaf(CRITERION_ATTR_NAME, AtlasRule.RuleExprObject.Operator.STARTS_WITH, CRITERION_VAL_TEST),
                        criterionLeaf(CRITERION_ATTR_NAME, AtlasRule.RuleExprObject.Operator.ENDS_WITH, CRITERION_VAL_TEST))));

        rules.add(rule("rule_3", "DISCARD",
                nestedCriterion("hive*", AtlasRule.Condition.OR, false,
                        criterionLeaf(CRITERION_ATTR_OPERATION, AtlasRule.RuleExprObject.Operator.EQ, CRITERION_VAL_ENTITY_DELETE),
                        criterionLeaf(CRITERION_ATTR_OPERATION, AtlasRule.RuleExprObject.Operator.EQ, CRITERION_VAL_CLASSIFICATION_DELETE))));

        rules.add(rule("rule_4", "DISCARD",
                criterion("Asset", CRITERION_ATTR_NAME,
                        AtlasRule.RuleExprObject.Operator.CONTAINS, CRITERION_VAL_DEMO, true)));

        rules.add(rule("rule_5", "DISCARD", typeOnly("spark_process", false)));

        return AtlasRuleUtils.getSortedRules(rules);
    }

    private static AtlasEntityAuditFilterService.JsonLogicConverter newJsonLogicConverter(
            AtlasEntityAuditFilterService delegate) throws Exception {
        Class<?> converterClass = Class.forName(
                "org.apache.atlas.rulesengine.AtlasEntityAuditFilterService$JsonLogicConverter");
        Constructor<?> ctor = converterClass.getDeclaredConstructor(AtlasEntityAuditFilterService.class);
        ctor.setAccessible(true);
        return (AtlasEntityAuditFilterService.JsonLogicConverter) ctor.newInstance(delegate);
    }

    private static AtlasRule rule(String ruleName, String action, AtlasRule.RuleExprObject... exprObjects) {
        AtlasRule rule = new AtlasRule();
        rule.setRuleName(ruleName);
        rule.setAction(action);
        rule.setCreatedTime(new Date(RULE_CREATED_TIME.getAndIncrement()));
        rule.setRuleExpr(new AtlasRule.RuleExpr(Arrays.asList(exprObjects)));
        return rule;
    }

    private static AtlasRule.RuleExprObject typeOnly(String typeName, boolean includeSubTypes) {
        return new AtlasRule.RuleExprObject(typeName, includeSubTypes);
    }

    private static AtlasRule.RuleExprObject criterion(String typeName, String attributeName,
            AtlasRule.RuleExprObject.Operator operator, String attributeValue, boolean includeSubTypes) {
        return new AtlasRule.RuleExprObject(typeName, attributeName, operator, attributeValue, includeSubTypes);
    }

    private static AtlasRule.RuleExprObject nestedCriterion(String typeName, AtlasRule.Condition condition,
            boolean includeSubTypes, AtlasRule.RuleExprObject.Criterion... criteria) {
        return new AtlasRule.RuleExprObject(typeName, condition, Arrays.asList(criteria), includeSubTypes);
    }

    private static AtlasRule.RuleExprObject.Criterion criterionLeaf(String attributeName,
            AtlasRule.RuleExprObject.Operator operator, String attributeValue) {
        return new AtlasRule.RuleExprObject.Criterion(operator, attributeName, attributeValue);
    }

    private static AtlasTypeRegistry createTypeRegistry() {
        Map<String, Set<String>> hierarchy = new LinkedHashMap<>();
        hierarchy.put("Asset", set("Asset", "hive_table", "hive_db", "hdfs_path", "Process", "spark_table", "spark_process"));
        hierarchy.put("hive_table", set("hive_table"));
        hierarchy.put("hive_db", set("hive_db"));
        hierarchy.put("hdfs_path", set("hdfs_path"));
        hierarchy.put("spark_table", set("spark_table"));
        hierarchy.put("spark_process", set("spark_process"));
        hierarchy.put("Process", set("Process", "spark_process"));

        AtlasTypeRegistry registry = mock(AtlasTypeRegistry.class);
        when(registry.getAllEntityDefNames()).thenReturn(hierarchy.keySet());

        for (Map.Entry<String, Set<String>> entry : hierarchy.entrySet()) {
            AtlasEntityType entityType = mock(AtlasEntityType.class);
            when(entityType.getTypeName()).thenReturn(entry.getKey());
            when(entityType.getTypeAndAllSubTypes()).thenReturn(entry.getValue());
            when(registry.getEntityTypeByName(entry.getKey())).thenReturn(entityType);
        }

        when(registry.getEntityTypeByName(argThat(name -> name == null || !hierarchy.containsKey(name)))).thenReturn(null);
        return registry;
    }

    private static Set<String> set(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }
}
