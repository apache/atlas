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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

    @DataProvider
    public Object[][] defaultActionWhenFetchRulesFails() {
        return new Object[][] {
                {RuleAction.Result.ACCEPT, true},
                {RuleAction.Result.DISCARD, false}
        };
    }

    @Test(dataProvider = "defaultActionWhenFetchRulesFails")
    public void testAcceptUsesDefaultWhenFetchRulesFails(RuleAction.Result defaultResult, boolean expectAccept)
            throws Exception {
        when(defaultAction.getResult()).thenReturn(defaultResult);
        when(auditFilterService.fetchRules()).thenThrow(new AtlasBaseException("graph unavailable"));

        if (expectAccept) {
            assertTrue(rulesEngine.accept(eventPayload()));
        } else {
            assertFalse(rulesEngine.accept(eventPayload()));
        }
    }

    private Map<String, Object> eventPayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("typeName", "hive_table");
        return payload;
    }
}
