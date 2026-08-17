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
package org.apache.atlas.integration;

import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 * Integration smoke test for classes changed in active-active branch.
 *
 * <p>This validates that all changed, non-deleted production classes are
 * resolvable in the integrated test classpath assembled by atlas-webapp.
 */
public class ActiveActiveChangedClassesLoadIT {
    private static final String[] CHANGED_NON_DELETED_CLASSES = new String[] {
            "org.apache.atlas.ha.HAConfiguration",
            "org.apache.atlas.repository.Constants",
            "org.apache.atlas.AtlasConfiguration",
            "org.apache.atlas.AtlasRunMode",
            "org.apache.atlas.model.patches.AtlasPatch",
            "org.apache.atlas.GraphTransactionInterceptor",
            "org.apache.atlas.repository.audit.AbstractStorageBasedAuditRepository",
            "org.apache.atlas.repository.audit.HBaseBasedAuditRepository",
            "org.apache.atlas.repository.graph.GraphBackedSearchIndexer",
            "org.apache.atlas.repository.graph.IndexRecoveryService",
            "org.apache.atlas.repository.impexp.AsyncImportService",
            "org.apache.atlas.repository.patches.AtlasPatchManager",
            "org.apache.atlas.repository.patches.AtlasPatchRegistry",
            "org.apache.atlas.repository.patches.AtlasPatchService",
            "org.apache.atlas.repository.patches.ReIndexPatch",
            "org.apache.atlas.repository.patches.UpdateCompositeIndexStatusPatch",
            "org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer",
            "org.apache.atlas.services.PurgeService",
            "org.apache.atlas.tasks.GraphClaimable",
            "org.apache.atlas.tasks.TaskExecutor",
            "org.apache.atlas.tasks.TaskManagement",
            "org.apache.atlas.tasks.TaskRegistry",
            "org.apache.atlas.listener.ActiveStateChangeHandler",
            "org.apache.atlas.Atlas",
            "org.apache.atlas.ha.TypeDefChangeNotifier",
            "org.apache.atlas.ha.TypeDefSyncConsumer",
            "org.apache.atlas.notification.ImportTaskListenerImpl",
            "org.apache.atlas.notification.NotificationHookConsumer",
            "org.apache.atlas.web.filters.ActiveServerFilter",
            "org.apache.atlas.web.security.AtlasSecurityConfig",
            "org.apache.atlas.web.service.AtlasActivationService",
            "org.apache.atlas.web.service.EmbeddedServer",
            "org.apache.atlas.web.service.ServiceState"
    };

    @Test
    public void changedClasses_areLoadableInIntegratedClasspath() throws Exception {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        for (String className : CHANGED_NON_DELETED_CLASSES) {
            Class<?> loaded = Class.forName(className, false, loader);

            assertNotNull(loaded, "Expected class to load: " + className);
        }
    }
}
