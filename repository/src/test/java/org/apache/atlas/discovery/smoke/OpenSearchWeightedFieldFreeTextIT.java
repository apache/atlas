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
package org.apache.atlas.discovery.smoke;

import org.apache.atlas.runner.OpenSearchITBase;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * Integration test: reproduces the original {@code too_many_nested_clauses} (OpenSearch/Lucene default
 * {@code maxClauseCount=1024}) failure with 3000 system-wide weighted fields applied, and the case-sensitive
 * trailing-wildcard ("quick search") failure, against a real OpenSearch instance -- and proves both are fixed by
 * the {@code multi_match}-based free-text query architecture (not by capping/truncating the weighted-field set,
 * and not by using {@code match_phrase_prefix}). See {@link OpenSearchWeightedFieldFreeTextValidationDriver} for
 * the full scenario.
 */
public class OpenSearchWeightedFieldFreeTextIT extends OpenSearchITBase {
    @Test
    public void weightedFieldFreeTextValidation() throws Exception {
        assertTrue(OpenSearchWeightedFieldFreeTextValidationDriver.execute(),
                "OpenSearch multi_match free-text architecture validation failed");
    }
}
