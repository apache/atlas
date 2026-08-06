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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.TestModules;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditRowKind;
import org.apache.atlas.model.audit.AuditSearchParameters;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.model.instance.PurgeSummary;
import org.apache.atlas.repository.ogm.AtlasAuditEntryDTO;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.purge.PurgeExecutionStats;
import org.apache.atlas.repository.purge.PurgeUtils;
import org.apache.atlas.services.PurgeAuditWriter;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.mockito.ArgumentCaptor;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.utils.TestLoadModelUtils.loadBaseModel;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasAuditServiceTest {
    private static final int    WAIT_TIME_FOR_INDEX_CREATION_IN_MILLI = 5000;
    private static final String AUDIT_PARAMETER_RESOURCE_DIR          = "auditSearchParameters";
    private static final String DEFAULT_USER                          = "admin";
    private static final String TEST_RUN_ID                           = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    AtlasAuditService auditService;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    private AtlasDiscoveryService mockDiscoveryService;
    private AtlasAuditService     purgeRunLookupAuditService;

    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        loadBaseModel(typeDefStore, typeRegistry);
    }

    @BeforeMethod
    public void setupPurgeRunLookupMocks() {
        mockDiscoveryService     = mock(AtlasDiscoveryService.class);
        purgeRunLookupAuditService = new AtlasAuditService(mock(DataAccess.class), mockDiscoveryService);
    }

    @Test
    public void checkTypeRegistered() throws AtlasBaseException {
        AtlasType auditEntryType = typeRegistry.getType("__" + AtlasAuditEntry.class.getSimpleName());

        assertNotNull(auditEntryType);
    }

    @Test
    public void checkStoringOfAuditEntry() throws AtlasBaseException {
        final String    clientId1        = "client1";
        AtlasAuditEntry entryTobeStored1 = saveEntry(AuditOperation.PURGE, clientId1);

        String          clientId2        = "client2";
        AtlasAuditEntry entryTobeStored2 = saveEntry(AuditOperation.PURGE, clientId2);

        waitForIndexCreation();

        AtlasAuditEntry storedEntry1 = retrieveEntry(entryTobeStored1);
        AtlasAuditEntry storedEntry2 = retrieveEntry(entryTobeStored2);

        assertNotEquals(storedEntry1.getGuid(), storedEntry2.getGuid());

        assertNotNull(storedEntry1.getGuid());
        assertNotNull(storedEntry2.getGuid());

        assertEquals(storedEntry1.getUserName(), DEFAULT_USER);
        assertEquals(storedEntry2.getUserName(), DEFAULT_USER);

        assertEquals(storedEntry1.getClientId(), entryTobeStored1.getClientId());
        assertEquals(storedEntry2.getClientId(), entryTobeStored2.getClientId());

        assertEquals(storedEntry1.getOperation(), entryTobeStored1.getOperation());
        assertEquals(storedEntry2.getOperation(), entryTobeStored2.getOperation());
    }

    @Test
    public void checkStoringMultipleAuditEntries() throws AtlasBaseException {
        final String clientId   = "client1";
        final int    maxEntries = 5;
        final int    limitParam = 3;

        for (int i = 0; i < maxEntries; i++) {
            saveEntry(AuditOperation.PURGE, clientId);
        }

        waitForIndexCreation();

        AuditSearchParameters auditSearchParameters = createAuditParameter("audit-search-parameter-purge");

        auditSearchParameters.setLimit(limitParam);
        auditSearchParameters.setOffset(0);

        List<AtlasAuditEntry> resultLimitedByParam = auditService.get(auditSearchParameters);

        assertEquals(resultLimitedByParam.size(), limitParam);

        auditSearchParameters.setLimit(maxEntries);
        auditSearchParameters.setOffset(limitParam);

        List<AtlasAuditEntry> results = auditService.get(auditSearchParameters);

        assertEquals(results.size(), (maxEntries - limitParam));
    }

    @Test
    public void purgeAuditWriter_writeBatchAndFinishRun_writesBatchAndSummaryAudits() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);

        Set<String> batchGuids = new LinkedHashSet<>(Arrays.asList("guid2", "guid1"));
        EntityMutationResponse response = new EntityMutationResponse();
        AtlasEntityHeader header1 = new AtlasEntityHeader();
        header1.setGuid("guid1");
        AtlasEntityHeader header2 = new AtlasEntityHeader();
        header2.setGuid("guid2");
        response.addEntity(EntityOperation.PURGE, header1);
        response.addEntity(EntityOperation.PURGE, header2);

        PurgeAuditWriter.writeBatch(mockAuditService, AuditOperation.PURGE, TEST_RUN_ID, batchGuids, response);

        ArgumentCaptor<String> batchParamsCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> batchResultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), batchParamsCaptor.capture(), batchResultCaptor.capture(),
                eq(2L), eq(TEST_RUN_ID), eq(AuditRowKind.BATCH));

        assertEquals(batchParamsCaptor.getValue(), PurgeUtils.buildGuidParams(batchGuids));
        assertEquals(batchResultCaptor.getValue(), "guid1,guid2");

        AtlasAuditEntry batchEntry = new AtlasAuditEntry();
        batchEntry.setAuditRowKind(AuditRowKind.BATCH);
        batchEntry.setResult(batchResultCaptor.getValue());
        assertFalse(PurgeUtils.isPurgeSummaryAudit(batchEntry));

        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Arrays.asList(
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222"));
        PurgeExecutionStats stats = new PurgeExecutionStats(originallyRequestedGuids, originallyRequestedGuids.size());

        AtlasEntityHeader purged = new AtlasEntityHeader();
        purged.setGuid("11111111-1111-1111-1111-111111111111");
        stats.recordBatchOutcome(Arrays.asList(purged), null, originallyRequestedGuids);

        PurgeAuditWriter.finishRun(mockAuditService, AuditOperation.PURGE, TEST_RUN_ID, originallyRequestedGuids, stats);

        ArgumentCaptor<String> summaryParamsCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> summaryResultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), summaryParamsCaptor.capture(), summaryResultCaptor.capture(),
                eq(1L), eq(TEST_RUN_ID), eq(AuditRowKind.SUMMARY));

        assertEquals(summaryParamsCaptor.getValue(), PurgeUtils.buildGuidParams(originallyRequestedGuids));

        PurgeSummary summary = AtlasJson.fromJson(summaryResultCaptor.getValue(), PurgeSummary.class);
        assertEquals(summary.getRunId(), TEST_RUN_ID);
        assertEquals(summary.getRequestedCount(), 2);
        assertEquals(summary.getPurgedCount(), 1);

        AtlasAuditEntry summaryEntry = new AtlasAuditEntry();
        summaryEntry.setAuditRowKind(AuditRowKind.SUMMARY);
        summaryEntry.setResult(summaryResultCaptor.getValue());
        assertTrue(PurgeUtils.isPurgeSummaryAudit(summaryEntry));

        Set<String> emptyBatchGuids = new LinkedHashSet<>(Arrays.asList("guid-b", "guid-a"));
        EntityMutationResponse emptyBatchResponse = new EntityMutationResponse();
        emptyBatchResponse.addFailedEntity(new FailedEntity("guid-a", "ATLAS-500-00-001", "batch failed"));
        emptyBatchResponse.addFailedEntity(new FailedEntity("guid-b", "ATLAS-500-00-001", "batch failed"));

        PurgeAuditWriter.writeBatch(mockAuditService, AuditOperation.PURGE, TEST_RUN_ID, emptyBatchGuids, emptyBatchResponse);

        ArgumentCaptor<String> emptyBatchParamsCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> emptyBatchResultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), emptyBatchParamsCaptor.capture(),
                emptyBatchResultCaptor.capture(), eq(0L), eq(TEST_RUN_ID), eq(AuditRowKind.BATCH));

        assertEquals(emptyBatchParamsCaptor.getValue(), PurgeUtils.buildGuidParams(emptyBatchGuids));
        assertEquals(emptyBatchResultCaptor.getValue(), "");

        AtlasAuditEntry emptyBatchEntry = new AtlasAuditEntry();
        emptyBatchEntry.setAuditRowKind(AuditRowKind.BATCH);
        emptyBatchEntry.setOperation(AuditOperation.PURGE);
        emptyBatchEntry.setResult(emptyBatchResultCaptor.getValue());
        assertTrue(PurgeUtils.isPurgeBatchAudit(emptyBatchEntry));
        assertFalse(PurgeUtils.isPurgeSummaryAudit(emptyBatchEntry));
    }

    @Test
    public void getPurgeBatchAuditGuidsForRun_pagesUntilEmpty() throws Exception {
        when(mockDiscoveryService.searchWithParameters(any(SearchParameters.class))).thenAnswer(invocation -> {
            SearchParameters params = invocation.getArgument(0);
            int limit  = params.getLimit();
            int offset = params.getOffset();

            AtlasSearchResult result = new AtlasSearchResult(params);
            if (offset == 0) {
                List<AtlasEntityHeader> fullPage = new ArrayList<>();
                for (int i = 0; i < limit; i++) {
                    fullPage.add(buildAuditHeader("batch-guid-" + i, "entity-" + i, AuditRowKind.BATCH));
                }
                result.setEntities(fullPage);
            } else if (offset == limit) {
                result.setEntities(Arrays.asList(buildAuditHeader("batch-guid-last", "entity-last", AuditRowKind.BATCH)));
            } else {
                result.setEntities(new ArrayList<>());
            }
            return result;
        });

        AtlasAuditEntry summaryEntry = new AtlasAuditEntry();
        summaryEntry.setRunId(TEST_RUN_ID);

        List<String> batchGuids = purgeRunLookupAuditService.getPurgeBatchAuditGuidsForRun(summaryEntry);

        assertEquals(batchGuids.size(), AtlasConfiguration.SEARCH_MAX_LIMIT.getInt() + 1);
    }

    @Test
    public void getPurgedEntityGuidsForRun_filtersSummaryAndMergesBatchRows() throws Exception {
        PurgeSummary summary = new PurgeSummary(3, 2, 1, 0, 0, 0);
        summary.setRunId(TEST_RUN_ID);

        AtlasAuditEntry summaryEntry = new AtlasAuditEntry();
        summaryEntry.setGuid("summary-guid");
        summaryEntry.setRunId(TEST_RUN_ID);
        summaryEntry.setAuditRowKind(AuditRowKind.SUMMARY);
        summaryEntry.setResult(AtlasJson.toJson(summary));

        Map<Integer, List<AtlasEntityHeader>> pagesByOffset = new HashMap<>();
        pagesByOffset.put(0, Arrays.asList(
                buildAuditHeader("batch-guid-1", "entity-1,entity-2", AuditRowKind.BATCH),
                buildAuditHeader("batch-guid-2", "entity-3", AuditRowKind.BATCH)));

        when(mockDiscoveryService.searchWithParameters(any(SearchParameters.class))).thenAnswer(invocation -> {
            SearchParameters params = invocation.getArgument(0);
            assertNotNull(params.getEntityFilters());
            assertTrue(hasBatchRowKindFilter(params.getEntityFilters()),
                    "Batch audit lookup should filter auditRowKind=BATCH at graph level");

            AtlasSearchResult result = new AtlasSearchResult(params);
            result.setEntities(pagesByOffset.getOrDefault(params.getOffset(), new ArrayList<>()));
            return result;
        });

        assertEquals(purgeRunLookupAuditService.getPurgeBatchAuditGuidsForRun(summaryEntry),
                Arrays.asList("batch-guid-1", "batch-guid-2"));
        assertEquals(purgeRunLookupAuditService.getPurgedEntityGuidsForRun(summaryEntry),
                Arrays.asList("entity-1", "entity-2", "entity-3"));
    }

    protected void waitForIndexCreation() {
        try {
            Thread.sleep(WAIT_TIME_FOR_INDEX_CREATION_IN_MILLI);
        } catch (InterruptedException ex) {
            throw new SkipException("Wait interrupted.");
        }
    }

    private AuditSearchParameters createAuditParameter(String fileName) {
        try {
            return TestResourceFileUtils.readObjectFromJson(AUDIT_PARAMETER_RESOURCE_DIR, fileName, AuditSearchParameters.class);
        } catch (IOException e) {
            fail(e.getMessage());
        }
        return null;
    }

    private AtlasAuditEntry retrieveEntry(AtlasAuditEntry entry) throws AtlasBaseException {
        AuditSearchParameters auditSearchParameters = createAuditParameter("audit-search-parameter-purge");
        AtlasAuditEntry       result                = auditService.get(entry);

        assertNotNull(result);

        entry.setGuid(result.getGuid());

        return auditService.get(entry);
    }

    private AtlasAuditEntry saveEntry(AuditOperation operation, String clientId) throws AtlasBaseException {
        AtlasAuditEntry entry = new AtlasAuditEntry(operation, DEFAULT_USER, clientId);

        entry.setStartTime(new Date());
        entry.setEndTime(new Date());

        auditService.save(entry);

        return entry;
    }

    private static AtlasEntityHeader buildAuditHeader(String guid, String result, AuditRowKind rowKind) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(AtlasAuditEntryDTO.ATTRIBUTE_OPERATION, AuditOperation.PURGE.name());
        attributes.put(AtlasAuditEntryDTO.ATTRIBUTE_RESULT, result);
        attributes.put(AtlasAuditEntryDTO.ATTRIBUTE_RESULT_COUNT, 0L);
        attributes.put(AtlasAuditEntryDTO.ATTRIBUTE_RUN_ID, TEST_RUN_ID);
        attributes.put(AtlasAuditEntryDTO.ATTRIBUTE_AUDIT_ROW_KIND, rowKind.name());

        AtlasEntityHeader header = new AtlasEntityHeader();
        header.setGuid(guid);
        header.setAttributes(attributes);
        return header;
    }

    private static boolean hasBatchRowKindFilter(SearchParameters.FilterCriteria filter) {
        if (filter == null) {
            return false;
        }

        if (AtlasAuditEntryDTO.ATTRIBUTE_AUDIT_ROW_KIND.equals(filter.getAttributeName())
                && SearchParameters.Operator.EQ.equals(filter.getOperator())
                && AuditRowKind.BATCH.name().equals(filter.getAttributeValue())) {
            return true;
        }

        if (filter.getCriterion() != null) {
            for (SearchParameters.FilterCriteria each : filter.getCriterion()) {
                if (hasBatchRowKindFilter(each)) {
                    return true;
                }
            }
        }

        return false;
    }
}
