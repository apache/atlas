/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import io.github.jamsesso.jsonlogic.JsonLogic;
import io.github.jamsesso.jsonlogic.JsonLogicException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasRule;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.atlas.rulesengine.RuleAction.Result.ACCEPT;
import static org.apache.atlas.rulesengine.RuleAction.Result.DISCARD;

public class AtlasRulesEngine {
    private static final Logger                        LOG       = LoggerFactory.getLogger(AtlasRulesEngine.class);
    final                JsonLogic                     jsonLogic = new JsonLogic();
    private final        RuleAction                    defaultAction;
    private final        AtlasEntityAuditFilterService auditFilterService;

    public AtlasRulesEngine(AtlasEntityAuditFilterService auditFilterService) {
        this.auditFilterService = auditFilterService;
        this.defaultAction      = auditFilterService.getDefaultAction();
        registerOperations();
    }

    public boolean accept(Map<String, Object> entityObj) throws AtlasBaseException {
        RuleAction.Result result = ACCEPT;
        if (defaultAction != null) {
            result = defaultAction.getResult();
        }
        LOG.debug("DEFAULT rule action : {}", result);
        final List<AtlasRule> rules = auditFilterService.fetchRules();
        if (CollectionUtils.isEmpty(rules)) {
            LOG.debug("No rules defined, DEFAULT action to {} will be taken", result);
        }

        for (AtlasRule rule : rules) {
            try {
                String  jsonLogicExpr = auditFilterService.getJsonLogicConverter().convertToJsonLogic(AtlasRuleUtils.getRuleExprJsonString(rule.getRuleExpr()));
                boolean isMatched     = evaluate(jsonLogicExpr, entityObj);

                LOG.debug("entityObj : {}", entityObj);
                LOG.debug("JLexpr : {}", jsonLogicExpr);
                LOG.debug("isMatched : {}", isMatched);

                if (isMatched && rule.getAction() != null) {
                    result = AtlasRuleUtils.getRuleActionFromString(rule.getAction()).performAction(entityObj);
                    LOG.debug("Matching rule found {} for entityObj{} with action {}", rule.getRuleExpr(), entityObj, rule.getAction());

                    if (result.equals(ACCEPT)) {
                        continue;
                    }
                    if (result.equals(DISCARD)) {
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.error("Error applying rule {} to event. Proceeding to evaluate other rules", rule, e);
            }
        }

        return result == ACCEPT;
    }

    private void registerOperations() {
        // Register custom operations
        jsonLogic.addOperation("isNull", AtlasRuleUtils.isNullFunc);
        jsonLogic.addOperation("notNull", AtlasRuleUtils.notNullFunc);
        jsonLogic.addOperation("startsWith", AtlasRuleUtils.startsWithFunc);
        jsonLogic.addOperation("endsWith", AtlasRuleUtils.endsWithFunc);

        //checks if two given strings match. The chkStr(first parameter) may contain wildcard characters
        jsonLogic.addOperation("contains", AtlasRuleUtils.containsFunc);

        // Negating the containsBiPredicate to check if both strings do not match
        jsonLogic.addOperation("notContains", AtlasRuleUtils.notContainsFunc);

        jsonLogic.addOperation("containsIgnoreCase", AtlasRuleUtils.containsIgnoreCaseFunc);
        jsonLogic.addOperation("notContainsIgnoreCase", AtlasRuleUtils.notContainsIgnoreCaseFunc);
    }

    private boolean evaluate(String jsonLogicRuleExpr, Object dataObj) throws JsonLogicException {
        return (boolean) jsonLogic.apply(jsonLogicRuleExpr, dataObj);
    }
}
