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
package org.apache.atlas;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.multibindings.Multibinder;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.discovery.EntityLineageService;
import org.apache.atlas.graph.GraphSandboxUtil;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.TypeDefChangeListener;
import org.apache.atlas.repository.audit.EntityAuditListener;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.impexp.ExportService;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.BulkImporter;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityChangeNotifier;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStoreV1;
import org.apache.atlas.repository.store.graph.v1.AtlasTypeDefGraphStoreV1;
import org.apache.atlas.repository.store.graph.v1.AtlasRelationshipStoreV1;
import org.apache.atlas.repository.store.graph.v1.BulkImporterImpl;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v1.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v1.HardDeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v1.SoftDeleteHandlerV1;
import org.apache.atlas.service.Service;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.atlas.util.SearchTracker;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestModules {

    static class MockNotifier implements Provider<AtlasEntityChangeNotifier> {
        @Override
        public AtlasEntityChangeNotifier get() {
            return Mockito.mock(AtlasEntityChangeNotifier.class);
        }
    }

    // Test only DI modules
    public static class TestOnlyModule extends AbstractModule {

        private static final Logger LOG = LoggerFactory.getLogger(TestOnlyModule.class);

        static class AtlasConfigurationProvider implements Provider<Configuration> {

            @Override
            public Configuration get() {
                try {
                    return ApplicationProperties.get();
                } catch (AtlasException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        static class AtlasGraphProvider implements Provider<AtlasGraph> {
            @Override
            public AtlasGraph get() {
                return org.apache.atlas.repository.graph.AtlasGraphProvider.getGraphInstance();
            }
        }

        @Override
        protected void configure() {

            GraphSandboxUtil.create();

            bindAuditRepository(binder());

            bindDeleteHandler(binder());

            bind(AtlasGraph.class).toProvider(AtlasGraphProvider.class);

            // allow for dynamic binding of graph service
            bind(Configuration.class).toProvider(AtlasConfigurationProvider.class).in(Singleton.class);

            // bind the AtlasTypeDefStore interface to an implementation
            bind(AtlasTypeDefStore.class).to(AtlasTypeDefGraphStoreV1.class).asEagerSingleton();

            bind(AtlasTypeRegistry.class).asEagerSingleton();
            bind(EntityGraphMapper.class).asEagerSingleton();
            bind(ExportService.class).asEagerSingleton();

            // New typesdef/instance change listener should also be bound to the corresponding implementation
            Multibinder<TypeDefChangeListener> typeDefChangeListenerMultibinder =
                    Multibinder.newSetBinder(binder(), TypeDefChangeListener.class);
            typeDefChangeListenerMultibinder.addBinding().to(GraphBackedSearchIndexer.class).asEagerSingleton();

            bind(SearchTracker.class).asEagerSingleton();

            bind(AtlasEntityStore.class).to(AtlasEntityStoreV1.class);
            bind(AtlasRelationshipStore.class).to(AtlasRelationshipStoreV1.class);

            // bind the DiscoveryService interface to an implementation
            bind(AtlasDiscoveryService.class).to(EntityDiscoveryService.class).asEagerSingleton();

            bind(AtlasLineageService.class).to(EntityLineageService.class).asEagerSingleton();
            bind(BulkImporter.class).to(BulkImporterImpl.class).asEagerSingleton();

            //Add EntityAuditListener as EntityChangeListener
            Multibinder<EntityChangeListener> entityChangeListenerBinder =
                    Multibinder.newSetBinder(binder(), EntityChangeListener.class);
            entityChangeListenerBinder.addBinding().to(EntityAuditListener.class);

            final GraphTransactionInterceptor graphTransactionInterceptor = new GraphTransactionInterceptor(new AtlasGraphProvider().get());
            requestInjection(graphTransactionInterceptor);
            bindInterceptor(Matchers.any(), Matchers.annotatedWith(GraphTransaction.class), graphTransactionInterceptor);
        }

        protected void bindDeleteHandler(Binder binder) {
            binder.bind(DeleteHandlerV1.class).to(AtlasRepositoryConfiguration.getDeleteHandlerV1Impl()).asEagerSingleton();
        }

        protected void bindAuditRepository(Binder binder) {

            Class<? extends EntityAuditRepository> auditRepoImpl = AtlasRepositoryConfiguration.getAuditRepositoryImpl();

            //Map EntityAuditRepository interface to configured implementation
            binder.bind(EntityAuditRepository.class).to(auditRepoImpl).asEagerSingleton();

            if(Service.class.isAssignableFrom(auditRepoImpl)) {
                Class<? extends Service> auditRepoService = (Class<? extends Service>)auditRepoImpl;
                //if it's a service, make sure that it gets properly closed at shutdown
                Multibinder<Service> serviceBinder = Multibinder.newSetBinder(binder, Service.class);
                serviceBinder.addBinding().to(auditRepoService);
            }
        }
    }

    public static class SoftDeleteModule extends TestOnlyModule {
        @Override
        protected void bindDeleteHandler(Binder binder) {
            bind(DeleteHandlerV1.class).to(SoftDeleteHandlerV1.class).asEagerSingleton();
            bind(AtlasEntityChangeNotifier.class).toProvider(MockNotifier.class);
        }
    }

    public static class HardDeleteModule extends TestOnlyModule {
        @Override
        protected void bindDeleteHandler(Binder binder) {
            bind(DeleteHandlerV1.class).to(HardDeleteHandlerV1.class).asEagerSingleton();
            bind(AtlasEntityChangeNotifier.class).toProvider(MockNotifier.class);
        }
    }
}
