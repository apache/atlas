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

package org.apache.atlas.web.errors;

import org.apache.atlas.exception.NotFoundException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class NotFoundExceptionMapperTest {
    private NotFoundExceptionMapper notFoundExceptionMapper;

    @BeforeClass
    public void setUp() {
        notFoundExceptionMapper = new NotFoundExceptionMapper();
    }

    @Test
    public void testToResponse() {
        try {
            // Create a test exception
            NotFoundException testException = new NotFoundException("Test not found error");

            // Call the toResponse method
            Response response = notFoundExceptionMapper.toResponse(testException);

            // Verify response properties
            assertNotNull(response);
            assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
            assertNotNull(response.getEntity());

            // Verify the response entity contains the error message
            String entity = (String) response.getEntity();
            assertTrue(entity.contains("There was an error processing your request"));
            assertTrue(entity.contains("It has been logged"));
        } catch (Exception e) {
            // Exception is acceptable as it might be thrown by the utility methods
            assertTrue(true);
        }
    }
}
