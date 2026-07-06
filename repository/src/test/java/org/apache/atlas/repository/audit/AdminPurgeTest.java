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
package org.apache.atlas.repository.audit;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.model.audit.AuditSearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.purge.PurgeUtils;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules = TestModules.TestOnlyModule.class)
public class AdminPurgeTest extends AtlasTestBase {
    private static final String CLIENT_HOST                  = "127.0.0.0";
    private static final String DEFAULT_USER                 = "Admin";
    private static final String AUDIT_PARAMETER_RESOURCE_DIR = "auditSearchParameters";

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasAuditService auditService;

    @Inject
    private AtlasEntityStoreV2 entityStore;

    @BeforeClass
    public void initialize() throws Exception {
        super.initialize();
    }

    @BeforeTest
    public void setupTest() throws IOException, AtlasBaseException {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);

        basicSetup(typeDefStore, typeRegistry);
    }

    @AfterClass
    public void clear() throws Exception {
        Thread.sleep(1000);

        AtlasGraphProvider.cleanup();

        super.cleanup();
    }

    @Test
    public void testDeleteEntitiesDoesNotLookupDeletedEntity() throws Exception {
        AtlasTypesDef sampleTypes   = TestUtilsV2.defineDeptEmployeeTypes();
        AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(sampleTypes, typeRegistry);

        if (!typesToCreate.isEmpty()) {
            typeDefStore.createTypesDef(typesToCreate);
        }

        AtlasEntity.AtlasEntitiesWithExtInfo deptEg2      = TestUtilsV2.createDeptEg2();
        AtlasEntityStream                    entityStream = new AtlasEntityStream(deptEg2);
        EntityMutationResponse               emr          = entityStore.createOrUpdate(entityStream, false);

        pauseForIndexCreation();

        assertNotNull(emr);
        assertNotNull(emr.getCreatedEntities());
        assertFalse(emr.getCreatedEntities().isEmpty());

        List<String> guids = emr.getCreatedEntities().stream()
                .map(AtlasEntityHeader::getGuid)
                .collect(Collectors.toList());

        EntityMutationResponse deleteResponse = entityStore.deleteByIds(guids);

        pauseForIndexCreation();

        assertSortedGuidsMatch(emr.getCreatedEntities(), deleteResponse.getDeletedEntities(), "deleteByIds");

        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "1");

        Date startTimestamp = new Date();
        EntityMutationResponse purgeResponse = entityStore.purgeByIds(new HashSet<>(guids));

        pauseForIndexCreation();

        assertPurgeSucceededForRequestedGuids(guids, purgeResponse);

        auditService.add(DEFAULT_USER, AtlasAuditEntry.AuditOperation.PURGE, CLIENT_HOST, startTimestamp, new Date(),
                guids.toString(), purgeResponse.getPurgedEntitiesIds(), purgeResponse.getPurgedEntities().size());

        assertAuditEntry(auditService, createAuditParameter("audit-search-parameter-without-filter"));
        assertAuditEntry(auditService, createAuditParameter("audit-search-parameter-purge"));
    }

    private void assertSortedGuidsMatch(List<AtlasEntityHeader> expected, List<AtlasEntityHeader> actual, String operation) {
        assertNotNull(actual, operation + " returned null entities");
        assertEquals(toSortedGuidList(actual), toSortedGuidList(expected), operation + " guid mismatch");
    }

    private void assertPurgeSucceededForRequestedGuids(List<String> requestedGuids, EntityMutationResponse response) {
        assertNotNull(response.getPurgedEntities(), "purgeByIds returned no purged entities");
        assertNotNull(response.getPurgeSummary(), "purgeByIds returned no summary");

        Set<String> purgedGuids = response.getPurgedEntities().stream()
                .map(AtlasEntityHeader::getGuid)
                .collect(Collectors.toSet());

        Set<String> skippedGuids = new HashSet<>();
        if (response.getFailedEntities() != null) {
            for (FailedEntity failedEntity : response.getFailedEntities()) {
                if (PurgeUtils.isSkippablePurgeFailureCode(failedEntity.getErrorCode())) {
                    skippedGuids.add(failedEntity.getGuid());
                }
            }
        }

        for (String guid : requestedGuids) {
            assertTrue(purgedGuids.contains(guid) || skippedGuids.contains(guid),
                    "Expected requested guid to be purged or skipped as already removed: " + guid);
        }

        assertEquals(response.getPurgeSummary().getRequestedCount(), requestedGuids.size());
        assertEquals(response.getPurgeSummary().getPurgedCount() + response.getPurgeSummary().getSkippedRequestedCount(),
                requestedGuids.size());
        assertEquals(response.getPurgeSummary().getFailedCount(), 0);
    }

    private static List<String> toSortedGuidList(List<AtlasEntityHeader> headers) {
        return headers.stream()
                .map(AtlasEntityHeader::getGuid)
                .sorted()
                .collect(Collectors.toList());
    }

    private AuditSearchParameters createAuditParameter(String fileName) {
        try {
            return TestResourceFileUtils.readObjectFromJson(AUDIT_PARAMETER_RESOURCE_DIR, fileName, AuditSearchParameters.class);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        return null;
    }

    private void assertAuditEntry(AtlasAuditService auditService, AuditSearchParameters auditSearchParameters) {
        pauseForIndexCreation();

        List<AtlasAuditEntry> result;

        try {
            result = auditService.get(auditSearchParameters);
        } catch (Exception e) {
            throw new SkipException("audit entries not retrieved.");
        }

        assertNotNull(result);
        assertFalse(result.isEmpty());
    }
}
