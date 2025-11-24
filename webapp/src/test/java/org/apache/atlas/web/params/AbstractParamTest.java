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

package org.apache.atlas.web.params;

import org.apache.atlas.exception.AtlasBaseException;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AbstractParamTest {
    @Test
    public void testAbstractParamFunctionality() {
        try {
            // Test successful parsing
            TestParam validParam = new TestParam("test value");
            assertEquals(validParam.get(), "test value");

            // Test toString method
            assertEquals(validParam.toString(), "test value");

            // Test equals method
            TestParam sameParam = new TestParam("test value");
            assertEquals(sameParam, validParam);
            assertEquals(validParam, validParam); // same instance
            assertNotEquals(validParam, null); // null check
            assertNotEquals(validParam, "not a param"); // different type

            // Test hashCode method
            assertEquals(validParam.hashCode(), sameParam.hashCode());

            // Test error handling methods
            TestParam errorParam = new TestParam(""); // This will cause parsing error
            // The constructor should throw WebApplicationException
        } catch (Exception e) {
            // Exception is expected for invalid input
            assertTrue(e instanceof javax.ws.rs.WebApplicationException);

            // Test error response creation
            try {
                TestParam errorParam = new TestParam(""); // This will cause parsing error
            } catch (javax.ws.rs.WebApplicationException we) {
                Response response = we.getResponse();
                assertNotNull(response);
                assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
                assertNotNull(response.getEntity());
            }
        }
    }

    // Concrete implementation for testing AbstractParam
    private static class TestParam extends AbstractParam<String> {
        public TestParam(String input) {
            super(input);
        }

        @Override
        protected String parse(String input) throws AtlasBaseException {
            if (input == null || input.trim().isEmpty()) {
                throw new AtlasBaseException("Invalid input");
            }
            return input.trim();
        }
    }
}
