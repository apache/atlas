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
package org.apache.atlas.trino.cli;

import org.quartz.CronExpression;
import org.quartz.DisallowConcurrentExecution;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for Trino extractor CLI behaviour that do not require a live Atlas/Trino stack.
 */
public class TrinoExtractorTest {
    @Test
    public void testInvalidCronExpressionRejected() {
        assertFalse(CronExpression.isValidExpression("not-a-valid-cron"));
    }

    @Test
    public void testValidCronExpressionAccepted() {
        assertTrue(CronExpression.isValidExpression("0 0 2 * * ?"));
    }

    @Test
    public void testMetadataJobDisallowConcurrentExecution() throws Exception {
        DisallowConcurrentExecution annotation = TrinoExtractor.MetadataJob.class.getAnnotation(DisallowConcurrentExecution.class);
        assertNotNull(annotation, "MetadataJob must disallow overlapping cron executions");
    }
}
