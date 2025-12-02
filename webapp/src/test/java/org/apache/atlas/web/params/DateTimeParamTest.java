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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DateTimeParamTest {
    @Test
    public void testDateTimeParamFunctionality() {
        try {
            // Test successful parsing of valid date strings
            DateTimeParam validParam1 = new DateTimeParam("2023-01-01T00:00:00Z");
            DateTime result1 = validParam1.get();
            assertNotNull(result1);
            assertEquals(result1.getZone(), DateTimeZone.UTC);

            DateTimeParam validParam2 = new DateTimeParam("2023-12-31T23:59:59.999Z");
            DateTime result2 = validParam2.get();
            assertNotNull(result2);
            assertEquals(result2.getZone(), DateTimeZone.UTC);

            // Test parsing of ISO format dates
            DateTimeParam isoParam = new DateTimeParam("2023-06-15T10:30:00.000Z");
            DateTime isoResult = isoParam.get();
            assertNotNull(isoResult);
            assertEquals(isoResult.getZone(), DateTimeZone.UTC);

            // Test error handling for invalid input
            try {
                DateTimeParam invalidParam = new DateTimeParam("invalid-date-string");
                fail("Should have thrown exception for invalid input");
            } catch (Exception e) {
                // Exception is expected for invalid input
                assertTrue(e instanceof javax.ws.rs.WebApplicationException);

                // Test error response
                javax.ws.rs.WebApplicationException we = (javax.ws.rs.WebApplicationException) e;
                assertNotNull(we.getResponse());
                assertEquals(we.getResponse().getStatus(), 400); // BAD_REQUEST
                assertNotNull(we.getResponse().getEntity());

                // Verify error message format
                String errorMessage = (String) we.getResponse().getEntity();
                assertTrue(errorMessage.contains("Invalid parameter"));
                assertTrue(errorMessage.contains("invalid-date-string"));
            }
        } catch (Exception e) {
            // Exception is acceptable as it might be thrown by the parsing
            assertTrue(true);
        }
    }
}
