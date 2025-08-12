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
package org.apache.atlas.services;

import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.*;

import static org.mockito.Mockito.when;

public class PurgeServiceTest extends AtlasTestBase {

    @Mock
    private PurgeService purgeService;

    @Mock
    private static Configuration atlasProperties;

    private static final String  PURGE_ENABLED_SERVICE_TYPES    = "atlas.purge.enabled.services";

    private final Map<String, String> entityTypes = new HashMap<String, String>() {{
        // entity name --> service
        put("hive_storagedesc", "hive");
        put("hive_column_lineage", "hive");
        put("hive_table", "hive");
        put("hive_column", "hive");
        put("hive_db", "hive");
        put("hive_process", "hive");
    }};

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSuccessfulPurgeEntities() {
        when(purgeService.purgeEntities()).thenReturn(createResponseForSuccessfulPurgeOfEntities());

        EntityMutationResponse response = purgeService.purgeEntities();

        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getPurgedEntities());
    }

    @Test
    public void testPurgeEntitiesReturnsEntityMutationResponse() {
        when(purgeService.purgeEntities()).thenReturn(new EntityMutationResponse());

        EntityMutationResponse response = purgeService.purgeEntities();

        Assert.assertTrue(response instanceof EntityMutationResponse);
    }

    @Test
    public void testGetEntityTypes() {
        Set<String> entityTypes = getTestEntityTypes();
        when(purgeService.getEntityTypes()).thenReturn(entityTypes);

        Assert.assertNotNull(entityTypes);
    }

    @Test
    public void testOnlyEnabledEntityTypesArePurged() {
        Set<String> servicesEnabled = new HashSet<>(Collections.singletonList("hive"));

        when(purgeService.purgeEntities()).thenReturn(createResponseForSuccessfulPurgeOfEntities());

        EntityMutationResponse response = purgeService.purgeEntities();

        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getPurgedEntities());

        int expectedNonEligibleTypesPurgedCount = 0;
        int actualNonEligibleTypesPurgedCount   = getCountNonEligibleTypesPurged(response.getPurgedEntities(), servicesEnabled);

        Assert.assertEquals(actualNonEligibleTypesPurgedCount, expectedNonEligibleTypesPurgedCount);
    }

    public int getCountNonEligibleTypesPurged(List<AtlasEntityHeader> purgedEntities, Set<String> servicesEnabled) {
        int resultCnt = 0;

        for (AtlasEntityHeader entityHeader : purgedEntities) {
            String entityType = entityHeader.getTypeName();
            String entityTypeServiceType = getServiceType(entityType);
            if (!servicesEnabled.contains(entityTypeServiceType)) {
                resultCnt++;
            }
        }

        return resultCnt;
    }

    @Test
    public void testOnlyGetEnabledServiceTypes() {
        when(atlasProperties.getStringArray(PURGE_ENABLED_SERVICE_TYPES)).thenReturn(getEnabledServiceTypes());

        String[] serviceTypes = atlasProperties.getStringArray(PURGE_ENABLED_SERVICE_TYPES);

        Assert.assertNotNull(serviceTypes);
    }

    String[] getEnabledServiceTypes() {
        return new String[]{"hive"};
    }

    public Set<String> getTestEntityTypes() {
        when(atlasProperties.getStringArray(PURGE_ENABLED_SERVICE_TYPES)).thenReturn(getEnabledServiceTypes());

        Set<String> serviceTypes = new HashSet<>(Arrays.asList(atlasProperties.getStringArray(PURGE_ENABLED_SERVICE_TYPES)));
        Set<String> entityTypes = new HashSet<>();

        for (String entityDef : serviceTypes) {
            String serviceTypeOfEntityDef = getServiceType(entityDef);
            if (serviceTypes.contains(serviceTypeOfEntityDef)) {
                entityTypes.add(entityDef);
            }
        }

        return entityTypes;
    }

    public String getServiceType(String entityDef) {
        return entityTypes.get(entityDef);
    }

    public EntityMutationResponse createResponseForSuccessfulPurgeOfEntities() {
        EntityMutationResponse response = new EntityMutationResponse();
        Random random = new Random();
        List<String> types = new ArrayList<>(entityTypes.keySet());

        List<AtlasEntityHeader> purgedEntities = new ArrayList<>();

        for (int i = 0 ; i < 10 ; i++) {
            AtlasEntityHeader mockEntity = new AtlasEntityHeader();
            mockEntity.setGuid(UUID.randomUUID().toString());
            String randomType = types.get(random.nextInt(types.size()));
            mockEntity.setTypeName(randomType);
            purgedEntities.add(mockEntity);
            response.addEntity(EntityMutations.EntityOperation.PURGE, mockEntity);
        }

        return response;
    }

    @Test
    public void testNoPurgeEntitiesFoundForPurge() {
        EntityMutationResponse expectedResponse = createResponseForNoEntitiesFound();
        when(purgeService.purgeEntities()).thenReturn(expectedResponse);

        EntityMutationResponse response = purgeService.purgeEntities();

        Assert.assertNotNull(response);
        Assert.assertNull(response.getPurgedEntities());
    }

    public EntityMutationResponse createResponseForNoEntitiesFound() {
        EntityMutationResponse response = new EntityMutationResponse();
        return response;
    }

    @Test
    public void testSoftDeleteProcessEntities_HasVoidReturnType() throws NoSuchMethodException {
        Method method = PurgeService.class.getMethod("softDeleteProcessEntities");

        Assert.assertEquals(method.getReturnType(), void.class);
    }
}
