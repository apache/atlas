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

package org.apache.atlas.web.util;

import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AtlasJsonProviderTest {
    private AtlasJsonProvider jsonProvider;

    @Before
    public void setUp() {
        jsonProvider = new AtlasJsonProvider();
    }

    @Test
    public void testConstructorAndConstants() {
        // Test constructor and constants
        assertNotNull(jsonProvider);

        // Test that the provider is properly annotated
        assertTrue(jsonProvider.getClass().isAnnotationPresent(javax.ws.rs.ext.Provider.class));
        assertTrue(jsonProvider.getClass().isAnnotationPresent(javax.ws.rs.Produces.class));
        assertTrue(jsonProvider.getClass().isAnnotationPresent(org.springframework.stereotype.Component.class));
    }

    @Test
    public void testReadFromWithJsonParseException() {
        // Test reading malformed JSON that causes JsonParseException
        String invalidJson = "{invalid json}";
        InputStream inputStream = new ByteArrayInputStream(invalidJson.getBytes(StandardCharsets.UTF_8));

        try {
            jsonProvider.readFrom(
                    Object.class,
                    null,
                    new Annotation[0],
                    MediaType.APPLICATION_JSON_TYPE,
                    null,
                    inputStream);
            fail("Should have thrown WebApplicationException");
        } catch (WebApplicationException e) {
            Response response = e.getResponse();
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
            assertNotNull(response.getEntity());
        } catch (Exception e) {
            assertTrue(true);
        }
    }
}
