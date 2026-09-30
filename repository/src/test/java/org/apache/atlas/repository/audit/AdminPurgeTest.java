/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.model.audit.AuditSearchParameters;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.purge.PurgeUtils;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.services.PurgeService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * End-to-end delete, purge, and audit-search integration test for admin purge flows.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class AdminPurgeTest extends AtlasTestBase {
    private static final String CLIENT_HOST                  = "127.0.0.0";
    private static final String DEFAULT_USER                 = "Admin";
    private static final String AUDIT_PARAMETER_RESOURCE_DIR = "auditSearchParameters";

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasEntityStoreV2 entityStore;

    @Inject
    private AtlasGraph atlasGraph;

    @Inject
    private AtlasAuditService auditService;

    @BeforeClass
    public void setupClass() throws Exception {
        RequestContext.clear();
        super.initialize();
        basicSetup(typeDefStore, typeRegistry);
        Thread.sleep(1000);
    }

    @BeforeMethod
    public void setupMethod() {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);
    }

    @AfterClass
    public void tearDownClass() throws Exception {
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

        AtlasEntitiesWithExtInfo deptEg2      = TestUtilsV2.createDeptEg2();
        AtlasEntityStream        entityStream = new AtlasEntityStream(deptEg2);
        EntityMutationResponse   emr          = entityStore.createOrUpdate(entityStream, false);

        pauseForIndexCreation();

        assertNotNull(emr);
        assertNotNull(emr.getCreatedEntities());
        assertFalse(emr.getCreatedEntities().isEmpty());

        List<String> guids = emr.getCreatedEntities().stream()
                .map(AtlasEntityHeader::getGuid)
                .collect(Collectors.toList());

        EntityMutationResponse deleteResponse = entityStore.deleteByIds(guids);
        pauseForIndexCreation();

        List<AtlasEntityHeader> responseDeletedEntities = deleteResponse.getDeletedEntities();
        assertNotNull(responseDeletedEntities);

        responseDeletedEntities.sort(Comparator.comparing(AtlasEntityHeader::getGuid));

        List<AtlasEntityHeader> toBeDeletedEntities = emr.getCreatedEntities();
        toBeDeletedEntities.sort(Comparator.comparing(AtlasEntityHeader::getGuid));

        assertEquals(responseDeletedEntities.size(), emr.getCreatedEntities().size());

        for (int index = 0; index < responseDeletedEntities.size(); index++) {
            assertEquals(responseDeletedEntities.get(index).getGuid(), emr.getCreatedEntities().get(index).getGuid());
        }

        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "1");

        Date startTimestamp = new Date();
        EntityMutationResponse purgeResponse = createPurgeService().purgeByIds(new HashSet<>(guids));

        pauseForIndexCreation();

        List<AtlasEntityHeader> responsePurgedEntities = purgeResponse.getPurgedEntities();
        assertNotNull(responsePurgedEntities);
        responsePurgedEntities.sort(Comparator.comparing(AtlasEntityHeader::getGuid));

        assertEquals(responsePurgedEntities.size(), responseDeletedEntities.size());

        for (int index = 0; index < responsePurgedEntities.size(); index++) {
            assertEquals(responsePurgedEntities.get(index).getGuid(), responseDeletedEntities.get(index).getGuid());
        }

        auditService.add(DEFAULT_USER, AuditOperation.PURGE, CLIENT_HOST, startTimestamp, new Date(),
                guids.toString(), purgeResponse.getPurgedEntitiesIds(), purgeResponse.getPurgedEntities().size());

        assertAuditEntry(auditService, createAuditParameter("audit-search-parameter-without-filter"));
        assertAuditEntry(auditService, createAuditParameter("audit-search-parameter-purge"));
        assertPurgeAuditRowsWrittenByPurgeService(createAuditParameter("audit-search-parameter-purge"));
    }

    private PurgeService createPurgeService() {
        return new PurgeService(atlasGraph, entityStore, typeRegistry, auditService);
    }

    private AuditSearchParameters createAuditParameter(String fileName) {
        try {
            return TestResourceFileUtils.readObjectFromJson(AUDIT_PARAMETER_RESOURCE_DIR, fileName, AuditSearchParameters.class);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        return null;
    }

    private void assertPurgeAuditRowsWrittenByPurgeService(AuditSearchParameters auditSearchParameters) {
        pauseForIndexCreation();

        List<AtlasAuditEntry> result;

        try {
            result = auditService.get(auditSearchParameters);
        } catch (Exception e) {
            throw new SkipException("purge audit entries not retrieved.");
        }

        assertNotNull(result);
        assertFalse(result.isEmpty());

        boolean hasSummaryRow = false;
        boolean hasBatchRow   = false;
        for (AtlasAuditEntry entry : result) {
            if (entry.getOperation() != AuditOperation.PURGE && entry.getOperation() != AuditOperation.AUTO_PURGE) {
                continue;
            }

            if (PurgeUtils.isPurgeSummaryAudit(entry)) {
                hasSummaryRow = true;
                assertNotNull(entry.getRunId(), "Purge summary audit should carry runId");
                assertNotNull(PurgeUtils.parsePurgeSummary(entry), "Summary row should contain parseable PurgeSummary JSON");
            }

            if (PurgeUtils.isPurgeBatchAudit(entry)) {
                hasBatchRow = true;
                assertNotNull(entry.getRunId(), "Purge batch audit should carry runId");
            }
        }

        assertTrue(hasSummaryRow, "Expected at least one purge summary audit row from purgeByIds");
        assertTrue(hasBatchRow, "Expected at least one purge batch audit row from purgeByIds");
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
