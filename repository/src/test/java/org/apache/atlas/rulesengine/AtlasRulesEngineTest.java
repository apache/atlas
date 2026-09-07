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
package org.apache.atlas.rulesengine;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasRule;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AtlasRulesEngineTest {
    @Mock
    private AtlasEntityAuditFilterService auditFilterService;

    @Mock
    private RuleAction defaultAction;

    private AtlasRulesEngine rulesEngine;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(auditFilterService.getDefaultAction()).thenReturn(defaultAction);
        rulesEngine = new AtlasRulesEngine(auditFilterService);
    }

    @Test
    public void testAcceptUsesDefaultAcceptWhenFetchRulesFails() throws Exception {
        when(defaultAction.getResult()).thenReturn(RuleAction.Result.ACCEPT);
        when(auditFilterService.fetchRules()).thenThrow(new AtlasBaseException("graph unavailable"));

        assertTrue(rulesEngine.accept(eventPayload()));
    }

    @Test
    public void testAcceptUsesDefaultDiscardWhenFetchRulesFails() throws Exception {
        when(defaultAction.getResult()).thenReturn(RuleAction.Result.DISCARD);
        when(auditFilterService.fetchRules()).thenThrow(new RuntimeException("graph unavailable"));

        assertFalse(rulesEngine.accept(eventPayload()));
    }

    @Test
    public void testAcceptWithNoRulesUsesDefaultAccept() throws Exception {
        when(defaultAction.getResult()).thenReturn(RuleAction.Result.ACCEPT);
        when(auditFilterService.fetchRules()).thenReturn(Collections.<AtlasRule>emptyList());

        assertTrue(rulesEngine.accept(eventPayload()));
    }

    private Map<String, Object> eventPayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("typeName", "hive_table");
        return payload;
    }
}
