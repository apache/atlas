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

package org.apache.atlas;

import com.google.inject.Binder;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.multibindings.Multibinder;

import org.aopalliance.intercept.MethodInterceptor;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.discovery.DataSetLineageService;
import org.apache.atlas.discovery.DiscoveryService;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.discovery.EntityLineageService;
import org.apache.atlas.discovery.LineageService;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.TypeDefChangeListener;
import org.apache.atlas.listener.TypesChangeListener;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.audit.EntityAuditListener;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graph.DeleteHandler;
import org.apache.atlas.repository.graph.GraphBackedMetadataRepository;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStoreV1;
import org.apache.atlas.repository.store.graph.v1.AtlasTypeDefGraphStoreV1;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v1.EntityGraphMapper;
import org.apache.atlas.repository.typestore.GraphBackedTypeStore;
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.service.Service;
import org.apache.atlas.services.DefaultMetadataService;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeSystemProvider;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * Guice module for Repository module.
 */
public class RepositoryMetadataModule extends com.google.inject.AbstractModule {

    @Override
    protected void configure() {

        // allow for dynamic binding of the metadata repo & graph service
        // bind the MetadataRepositoryService interface to an implementation
        bind(MetadataRepository.class).to(GraphBackedMetadataRepository.class).asEagerSingleton();

        bind(TypeSystem.class).toProvider(TypeSystemProvider.class).in(Singleton.class);

        // bind the ITypeStore interface to an implementation
        bind(ITypeStore.class).to(GraphBackedTypeStore.class).asEagerSingleton();
        bind(AtlasTypeDefStore.class).to(AtlasTypeDefGraphStoreV1.class).asEagerSingleton();
        bind(AtlasTypeRegistry.class).asEagerSingleton();

        //GraphBackedSearchIndexer must be an eager singleton to force the search index creation to happen before
        //we try to restore the type system (otherwise we'll end up running queries
        //before we have any indices during the initial graph setup)
        Multibinder<TypesChangeListener> typesChangeListenerBinder =
                Multibinder.newSetBinder(binder(), TypesChangeListener.class);
        typesChangeListenerBinder.addBinding().to(GraphBackedSearchIndexer.class).asEagerSingleton();

        // New typesdef/instance change listener should also be bound to the corresponding implementation
        Multibinder<TypeDefChangeListener> typeDefChangeListenerMultibinder =
                Multibinder.newSetBinder(binder(), TypeDefChangeListener.class);
        typeDefChangeListenerMultibinder.addBinding().to(DefaultMetadataService.class);
        typeDefChangeListenerMultibinder.addBinding().to(GraphBackedSearchIndexer.class).asEagerSingleton();

        bind(AtlasEntityStore.class).to(AtlasEntityStoreV1.class);

        // bind the MetadataService interface to an implementation
        bind(MetadataService.class).to(DefaultMetadataService.class).asEagerSingleton();

        // bind the DiscoveryService interface to an implementation
        bind(DiscoveryService.class).to(GraphBackedDiscoveryService.class).asEagerSingleton();
        bind(AtlasDiscoveryService.class).to(EntityDiscoveryService.class).asEagerSingleton();

        bind(LineageService.class).to(DataSetLineageService.class).asEagerSingleton();
        bind(AtlasLineageService.class).to(EntityLineageService.class).asEagerSingleton();

        Configuration configuration = getConfiguration();
        bindAuditRepository(binder(), configuration);

        bind(DeleteHandler.class).to(AtlasRepositoryConfiguration.getDeleteHandlerImpl()).asEagerSingleton();

        bind(DeleteHandlerV1.class).to(AtlasRepositoryConfiguration.getDeleteHandlerV1Impl()).asEagerSingleton();

        bind(TypeCache.class).to(AtlasRepositoryConfiguration.getTypeCache()).asEagerSingleton();

        bind(EntityGraphMapper.class);

        //Add EntityAuditListener as EntityChangeListener
        Multibinder<EntityChangeListener> entityChangeListenerBinder =
                Multibinder.newSetBinder(binder(), EntityChangeListener.class);
        entityChangeListenerBinder.addBinding().to(EntityAuditListener.class);

        MethodInterceptor interceptor = new GraphTransactionInterceptor();
        requestInjection(interceptor);
        bindInterceptor(Matchers.any(), Matchers.annotatedWith(GraphTransaction.class), interceptor);
    }

    protected Configuration getConfiguration() {
        try {
            return ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    protected void bindAuditRepository(Binder binder, Configuration configuration) {

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
