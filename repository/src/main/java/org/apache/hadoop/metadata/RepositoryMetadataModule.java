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

package org.apache.hadoop.metadata;

import com.google.inject.Scopes;
import com.google.inject.matcher.Matchers;
import com.google.inject.throwingproviders.ThrowingProviderBinder;
import com.thinkaurelius.titan.core.TitanGraph;
import org.aopalliance.intercept.MethodInterceptor;
import org.apache.hadoop.metadata.discovery.DiscoveryService;
import org.apache.hadoop.metadata.discovery.HiveLineageService;
import org.apache.hadoop.metadata.discovery.LineageService;
import org.apache.hadoop.metadata.discovery.SearchIndexer;
import org.apache.hadoop.metadata.discovery.graph.GraphBackedDiscoveryService;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.repository.graph.GraphBackedMetadataRepository;
import org.apache.hadoop.metadata.repository.graph.GraphBackedSearchIndexer;
import org.apache.hadoop.metadata.repository.graph.GraphProvider;
import org.apache.hadoop.metadata.repository.graph.TitanGraphProvider;
import org.apache.hadoop.metadata.repository.typestore.GraphBackedTypeStore;
import org.apache.hadoop.metadata.repository.typestore.ITypeStore;
import org.apache.hadoop.metadata.services.DefaultMetadataService;
import org.apache.hadoop.metadata.services.MetadataService;

/**
 * Guice module for Repository module.
 */
public class RepositoryMetadataModule extends com.google.inject.AbstractModule {
    @Override
    protected void configure() {
        // special wiring for Titan Graph
        ThrowingProviderBinder.create(binder())
                .bind(GraphProvider.class, TitanGraph.class)
                .to(TitanGraphProvider.class)
                .in(Scopes.SINGLETON);

        // allow for dynamic binding of the metadata repo & graph service

        // bind the MetadataRepositoryService interface to an implementation
        bind(MetadataRepository.class).to(GraphBackedMetadataRepository.class);

        // bind the ITypeStore interface to an implementation
        bind(ITypeStore.class).to(GraphBackedTypeStore.class);

        // bind the GraphService interface to an implementation
        // bind(GraphService.class).to(graphServiceClass);

        // bind the MetadataService interface to an implementation
        bind(MetadataService.class).to(DefaultMetadataService.class);

        // bind the DiscoveryService interface to an implementation
        bind(DiscoveryService.class).to(GraphBackedDiscoveryService.class);

        bind(SearchIndexer.class).to(GraphBackedSearchIndexer.class);

        bind(LineageService.class).to(HiveLineageService.class);

        MethodInterceptor interceptor = new GraphTransactionInterceptor();
        requestInjection(interceptor);
        bindInterceptor(Matchers.any(), Matchers.annotatedWith(GraphTransaction.class), interceptor);
    }
}
