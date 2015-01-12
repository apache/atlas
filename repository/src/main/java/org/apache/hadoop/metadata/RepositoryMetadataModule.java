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

/*
 * Created by IntelliJ IDEA.
 * User: seetharam
 * Date: 12/1/14
 * Time: 2:21 PM
 */
package org.apache.hadoop.metadata;

import org.apache.hadoop.metadata.services.GraphBackedMetadataRepository;
import org.apache.hadoop.metadata.services.GraphProvider;
import org.apache.hadoop.metadata.services.GraphService;
import org.apache.hadoop.metadata.services.GraphServiceConfigurator;
import org.apache.hadoop.metadata.services.MetadataRepository;
import org.apache.hadoop.metadata.services.TitanGraphProvider;
import org.apache.hadoop.metadata.storage.IRepository;
import org.apache.hadoop.metadata.storage.memory.MemRepository;

import com.google.inject.Scopes;
import com.google.inject.throwingproviders.ThrowingProviderBinder;
import com.thinkaurelius.titan.core.TitanGraph;

/**
 * Guice module for Repository module.
 */
public class RepositoryMetadataModule extends com.google.inject.AbstractModule {

	// Graph Service implementation class
	private Class<? extends GraphService> graphServiceClass;
	// MetadataRepositoryService implementation class
	private Class<? extends MetadataRepository> metadataRepoClass;

	public RepositoryMetadataModule() {
		GraphServiceConfigurator gsp = new GraphServiceConfigurator();

		// get the impl classes for the repo and the graph service
		this.graphServiceClass = gsp.getImplClass();
		this.metadataRepoClass = GraphBackedMetadataRepository.class;
	}

	protected void configure() {
		// special wiring for Titan Graph
		ThrowingProviderBinder.create(binder())
				.bind(GraphProvider.class, TitanGraph.class)
				.to(TitanGraphProvider.class)
				.in(Scopes.SINGLETON);

		// allow for dynamic binding of the metadata repo & graph service

		// bind the MetadataRepositoryService interface to an implementation
		bind(MetadataRepository.class).to(metadataRepoClass);
		// bind the GraphService interface to an implementation
		bind(GraphService.class).to(graphServiceClass);
	}
}
