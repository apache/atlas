/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.resources;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.atlas.AtlasClient.EntityResult;
import org.apache.atlas.services.MetadataService;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *   Unit test of {@link EntityResource}
 */
public class EntityResourceTest {

    private static final String DELETED_GUID = "deleted_guid";

    @Mock
    MetadataService mockService;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDeleteEntitiesDoesNotLookupDeletedEntity() throws Exception {
        List<String> guids = Collections.singletonList(DELETED_GUID);

        // Create EntityResult with a deleted guid and no other guids.
        EntityResult entityResult = new EntityResult(Collections.<String>emptyList(),
            Collections.<String>emptyList(), guids);
        when(mockService.deleteEntities(guids)).thenReturn(entityResult);

        // Create EntityResource with mock MetadataService.
        EntityResource entityResource = new EntityResource(mockService);

        Response response = entityResource.deleteEntities(guids, null, null, null);

        // Verify that if the EntityResult returned by MetadataService includes only deleted guids,
        // deleteEntities() does not perform any entity lookup.
        verify(mockService, never()).getEntityDefinition(Matchers.anyString());

        EntityResult resultFromEntityResource = EntityResult.fromString(response.getEntity().toString());
        Assert.assertTrue(resultFromEntityResource.getDeletedEntities().contains(DELETED_GUID));
    }
}
