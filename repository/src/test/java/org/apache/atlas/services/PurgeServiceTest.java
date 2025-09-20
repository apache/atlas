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
package org.apache.atlas.services;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.IAtlasEntityChangeNotifier;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.TestLoadModelUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = org.apache.atlas.TestModules.TestOnlyModule.class)
public class PurgeServiceTest extends AtlasTestBase {
    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasEntityStoreV2 entityStore;

    @Inject
    private AtlasGraph atlasGraph;

    @BeforeClass
    public void setup() throws Exception {
        RequestContext.clear();
        super.initialize();
        TestLoadModelUtils.loadBaseModel(typeDefStore, typeRegistry);
        TestLoadModelUtils.loadHiveModel(typeDefStore, typeRegistry);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) { }
    }

    @Test
    public void testPurgeEntities() throws Exception {
        // Approach (pre-commit check on async flow):
        // - Spy store's IAtlasEntityChangeNotifier and capture onEntitiesMutated(response,false) to assert
        //   the purged GUID before commit/locking paths.
        // Create DB and table
        AtlasEntity db = newHiveDb(null);
        String dbGuid = persistAndGetGuid(db);
        AtlasEntity tbl = newHiveTable(db, null);
        String tblGuid = persistAndGetGuid(tbl);

        // Soft-delete table
        EntityMutationResponse del = entityStore.deleteByIds(Collections.singletonList(tblGuid));
        assertNotNull(del);

        // Backdate timestamp beyond default 30d retention and reindex to reflect in Solr
        backdateModificationTimestamp(tblGuid, 31);
        reindexVertices(tblGuid);
        pauseForIndexCreation();

        // Enable hive purge and single worker for determinism
        ApplicationProperties.get().setProperty("atlas.purge.enabled.services", "hive");
        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "1");
        ApplicationProperties.get().setProperty("atlas.purge.worker.batch.size", "1");

        // Clear context to avoid skipping due to prior delete tracking
        RequestContext.clear();

        // Inject notifier spy to capture pre-commit signal (pre-commit verification point)
        Object originalNotifier = injectNotifierSpy(entityStore);

        new PurgeService(atlasGraph, entityStore, typeRegistry).purgeEntities();

        // Verify notifier call and capture response before transaction commit (async-safe via timeout)
        IAtlasEntityChangeNotifier spy = (IAtlasEntityChangeNotifier) getNotifier(entityStore);
        ArgumentCaptor<EntityMutationResponse> cap = ArgumentCaptor.forClass(EntityMutationResponse.class);
        Mockito.verify(spy, Mockito.timeout(5000)).onEntitiesMutated(cap.capture(), Mockito.eq(false));

        EntityMutationResponse notified = cap.getValue();
        assertNotNull(notified);
        List<AtlasEntityHeader> purged = notified.getPurgedEntities();
        assertNotNull(purged);

        assertTrue(purged.stream().anyMatch(h -> tblGuid.equals(h.getGuid())));

        // Restore original notifier to leave the store in a clean state
        restoreNotifier(entityStore, originalNotifier);

        // Flag assertions
        assertTrue(RequestContext.get().isPurgeRequested());
        assertEquals(RequestContext.get().getDeleteType(), DeleteType.HARD);
    }

    private AtlasEntity newHiveDb(String nameOpt) {
        String name = nameOpt != null ? nameOpt : RandomStringUtils.randomAlphanumeric(10);
        AtlasEntity db = new AtlasEntity("hive_db");
        db.setAttribute("name", name);
        db.setAttribute("qualifiedName", name);
        db.setAttribute("clusterName", "cl1");
        db.setAttribute("location", "/tmp");
        db.setAttribute("description", "test db");
        return db;
    }

    private AtlasEntity newHiveTable(AtlasEntity db, String nameOpt) {
        String name = nameOpt != null ? nameOpt : RandomStringUtils.randomAlphanumeric(10);
        AtlasEntity tbl = new AtlasEntity("hive_table");
        tbl.setAttribute("name", name);
        tbl.setAttribute("qualifiedName", name);
        tbl.setAttribute("description", "random table");
        tbl.setAttribute("type", "type");
        tbl.setAttribute("tableType", "MANAGED");
        tbl.setAttribute("db", AtlasTypeUtil.getAtlasObjectId(db));
        return tbl;
    }

    private String persistAndGetGuid(AtlasEntity entity) throws AtlasBaseException {
        EntityMutationResponse resp = entityStore.createOrUpdate(new AtlasEntityStream(new AtlasEntityWithExtInfo(entity)), false);
        String typeName = entity.getTypeName();
        AtlasEntityHeader hdr = resp.getFirstCreatedEntityByTypeName(typeName);
        String guid = hdr != null ? hdr.getGuid() : null;
        return guid;
    }

    private void backdateModificationTimestamp(String guid, int days) {
        AtlasVertex v = AtlasGraphUtilsV2.findByGuid(atlasGraph, guid);
        if (v != null) {
            long delta = days * 24L * 60 * 60 * 1000;
            long ts = System.currentTimeMillis() - delta;
            AtlasGraphUtilsV2.setProperty(v, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, ts);
        }
    }

    private void reindexVertices(String... guids) {
        List<AtlasElement> elements = new ArrayList<>();
        for (String g : guids) {
            if (g == null) {
                continue;
            }
            AtlasVertex v = AtlasGraphUtilsV2.findByGuid(atlasGraph, g);
            if (v != null) {
                elements.add(v);
            }
        }
        if (!elements.isEmpty()) {
            try {
                atlasGraph.getManagementSystem().reindex(Constants.VERTEX_INDEX, elements);
                atlasGraph.getManagementSystem().reindex(Constants.FULLTEXT_INDEX, elements);
            } catch (Exception ignored) { }
        }
    }

    // Test helper: swap store's private notifier field with a Mockito spy so we can
    // capture and assert the pre-commit mutation response invoked by the store.
    private Object injectNotifierSpy(AtlasEntityStoreV2 storeV2) throws Exception {
        Field f = AtlasEntityStoreV2.class.getDeclaredField("entityChangeNotifier");
        f.setAccessible(true);
        Object original = f.get(storeV2);
        Object spy = Mockito.spy(original);
        f.set(storeV2, spy);
        return original;
    }

    // Test helper: fetch the (spied) notifier instance currently installed on the store.
    private Object getNotifier(AtlasEntityStoreV2 storeV2) throws Exception {
        Field f = AtlasEntityStoreV2.class.getDeclaredField("entityChangeNotifier");
        f.setAccessible(true);
        return f.get(storeV2);
    }

    // Test helper: restore the original notifier instance after verification.
    private void restoreNotifier(AtlasEntityStoreV2 storeV2, Object original) throws Exception {
        Field f = AtlasEntityStoreV2.class.getDeclaredField("entityChangeNotifier");
        f.setAccessible(true);
        f.set(storeV2, original);
    }
}
