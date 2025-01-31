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

import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.model.audit.AuditSearchParameters;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import static org.apache.atlas.utils.TestLoadModelUtils.loadBaseModel;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasAuditServiceTest {
    private static final int    WAIT_TIME_FOR_INDEX_CREATION_IN_MILLI = 5000;
    private static final String AUDIT_PARAMETER_RESOURCE_DIR          = "auditSearchParameters";
    private static final String DEFAULT_USER                          = "admin";

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    AtlasAuditService auditService;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        loadBaseModel(typeDefStore, typeRegistry);
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
}
