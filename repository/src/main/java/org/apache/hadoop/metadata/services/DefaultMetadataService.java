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

package org.apache.hadoop.metadata.services;

import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.json.Serialization$;
import org.apache.hadoop.metadata.service.Services;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DefaultMetadataService implements MetadataService {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultMetadataService.class);
    public static final String NAME = DefaultMetadataService.class.getSimpleName();

    private TypeSystem typeSystem;
    private MetadataRepositoryService repositoryService;

    /**
     * Creates a new type based on the type system to enable adding
     * entities (instances for types).
     *
     * @param typeName       name for this type, must be unique
     * @param typeDefinition definition as json
     * @return a unique id for this type
     */
    @Override
    public String createType(String typeName, String typeDefinition) throws MetadataException {
        return null;
    }

    /**
     * Return the definition for the given type.
     *
     * @param typeName name for this type, must be unique
     * @return type definition as JSON
     */
    @Override
    public String getTypeDefinition(String typeName) throws MetadataException {
        return null;
    }

    /**
     * Return the list of types in the repository.
     *
     * @return list of type names in the repository
     */
    @Override
    public List<String> getTypeNamesList() throws MetadataException {
        return null;
    }

    /**
     * Creates an entity, instance of the type.
     *
     * @param entityType       type
     * @param entityDefinition definition
     * @return guid
     */
    @Override
    public String createEntity(String entityType,
                               String entityDefinition) throws MetadataException {
        ITypedReferenceableInstance entityInstance =
                Serialization$.MODULE$.fromJson(entityDefinition);
        return repositoryService.createEntity(entityInstance, entityType);
    }

    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    @Override
    public String getEntityDefinition(String guid) throws MetadataException {
        return null;
    }

    /**
     * Return the definition for the given entity name and type.
     *
     * @param entityName name
     * @param entityType type
     * @return entity definition as JSON
     */
    @Override
    public String getEntityDefinition(String entityName,
                                      String entityType) throws MetadataException {

        throw new UnsupportedOperationException();
    }

    /**
     * Return the list of entity names for the given type in the repository.
     *
     * @param entityType type
     * @return list of entity names for the given type in the repository
     */
    @Override
    public List<String> getEntityNamesList(String entityType) throws MetadataException {
        throw new UnsupportedOperationException();
    }

    /**
     * Name of the service.
     *
     * @return name of the service
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Starts the service. This method blocks until the service has completely started.
     *
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        LOG.info("Initializing the Metadata service");
        if (Services.get().isRegistered(TitanGraphService.NAME)) {
            DefaultTypesService typesService = Services.get().getService(DefaultTypesService.NAME);
            typeSystem = typesService.getTypeSystem();
        } else {
            throw new RuntimeException("Types service is not initialized");
        }

        if (Services.get().isRegistered(TitanGraphService.NAME)) {
            repositoryService = Services.get().getService(GraphBackedMetadataRepositoryService.NAME);
        } else {
            throw new RuntimeException("repository service is not initialized");
        }
    }

    /**
     * Stops the service. This method blocks until the service has completely shut down.
     */
    @Override
    public void stop() {
        // do nothing
        repositoryService = null;
    }

    /**
     * A version of stop() that is designed to be usable in Java7 closure
     * clauses.
     * Implementation classes MUST relay this directly to {@link #stop()}
     *
     * @throws java.io.IOException never
     * @throws RuntimeException    on any failure during the stop operation
     */
    @Override
    public void close() throws IOException {
        stop();
    }
}
