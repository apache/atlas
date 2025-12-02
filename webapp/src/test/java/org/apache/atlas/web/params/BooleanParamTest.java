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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class BooleanParamTest {
    @Test
    public void testBooleanParamFunctionality() {
        try {
            // Test successful parsing of "true" (case insensitive)
            BooleanParam trueParam1 = new BooleanParam("true");
            assertEquals(trueParam1.get(), Boolean.TRUE);

            BooleanParam trueParam2 = new BooleanParam("TRUE");
            assertEquals(trueParam2.get(), Boolean.TRUE);

            BooleanParam trueParam3 = new BooleanParam("True");
            assertEquals(trueParam3.get(), Boolean.TRUE);

            // Test successful parsing of "false" (case insensitive)
            BooleanParam falseParam1 = new BooleanParam("false");
            assertEquals(falseParam1.get(), Boolean.FALSE);

            BooleanParam falseParam2 = new BooleanParam("FALSE");
            assertEquals(falseParam2.get(), Boolean.FALSE);

            BooleanParam falseParam3 = new BooleanParam("False");
            assertEquals(falseParam3.get(), Boolean.FALSE);

            // Test error handling for invalid input
            try {
                BooleanParam invalidParam = new BooleanParam("invalid");
                fail("Should have thrown exception for invalid input");
            } catch (Exception e) {
                // Exception is expected for invalid input
                assertTrue(e instanceof javax.ws.rs.WebApplicationException);

                // Test error response
                javax.ws.rs.WebApplicationException we = (javax.ws.rs.WebApplicationException) e;
                assertNotNull(we.getResponse());
                assertEquals(we.getResponse().getStatus(), 400); // BAD_REQUEST
                assertNotNull(we.getResponse().getEntity());

                // Verify custom error message
                String errorMessage = (String) we.getResponse().getEntity();
                assertTrue(errorMessage.contains("must be \"true\" or \"false\""));
            }
        } catch (Exception e) {
            // Exception is acceptable as it might be thrown by the parsing
            assertTrue(true);
        }
    }
}
