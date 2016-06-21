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

package org.apache.atlas.discovery;

import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.discovery.graph.DefaultGraphPersistenceStrategy;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.query.GremlinQueryResult;
import org.apache.atlas.query.InputLineageClosureQuery;
import org.apache.atlas.query.OutputLineageClosureQuery;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.collection.immutable.List;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Hive implementation of Lineage service interface.
 */
@Singleton
public class DataSetLineageService implements LineageService {

    private static final Logger LOG = LoggerFactory.getLogger(DataSetLineageService.class);

    private static final Option<List<String>> SELECT_ATTRIBUTES =
            Some.<List<String>>apply(List.<String>fromArray(new String[]{"name"}));
    public static final String SELECT_INSTANCE_GUID = "__guid";

    public static final String DATASET_SCHEMA_QUERY_PREFIX = "atlas.lineage.schema.query.";

    private static final String HIVE_PROCESS_TYPE_NAME = "Process";
    private static final String HIVE_PROCESS_INPUT_ATTRIBUTE_NAME = "inputs";
    private static final String HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME = "outputs";

    private static final String DATASET_EXISTS_QUERY = AtlasClient.DATA_SET_SUPER_TYPE + " where __guid = '%s'";
    private static final String DATASET_NAME_EXISTS_QUERY =
            AtlasClient.DATA_SET_SUPER_TYPE + " where " + AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME + "='%s' and __state = 'ACTIVE'";

    private static final Configuration propertiesConf;

    static {
        try {
            propertiesConf = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }


    private final TitanGraph titanGraph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;
    private final GraphBackedDiscoveryService discoveryService;

    @Inject
    DataSetLineageService(GraphProvider<TitanGraph> graphProvider, MetadataRepository metadataRepository,
                          GraphBackedDiscoveryService discoveryService) throws DiscoveryException {
        this.titanGraph = graphProvider.get();
        this.graphPersistenceStrategy = new DefaultGraphPersistenceStrategy(metadataRepository);
        this.discoveryService = discoveryService;
    }

    /**
     * Return the lineage outputs graph for the given datasetName.
     *
     * @param datasetName datasetName
     * @return Outputs Graph as JSON
     */
    @Override
    @GraphTransaction
    public String getOutputsGraph(String datasetName) throws AtlasException {
        LOG.info("Fetching lineage outputs graph for datasetName={}", datasetName);
        ParamChecker.notEmpty(datasetName, "dataset name");
        ReferenceableInstance datasetInstance = validateDatasetNameExists(datasetName);
        return getOutputsGraphForId(datasetInstance.getId()._getId());
    }

    /**
     * Return the lineage inputs graph for the given tableName.
     *
     * @param tableName tableName
     * @return Inputs Graph as JSON
     */
    @Override
    @GraphTransaction
    public String getInputsGraph(String tableName) throws AtlasException {
        LOG.info("Fetching lineage inputs graph for tableName={}", tableName);
        ParamChecker.notEmpty(tableName, "table name");
        ReferenceableInstance datasetInstance = validateDatasetNameExists(tableName);
        return getInputsGraphForId(datasetInstance.getId()._getId());
    }

    @Override
    @GraphTransaction
    public String getInputsGraphForEntity(String guid) throws AtlasException {
        LOG.info("Fetching lineage inputs graph for entity={}", guid);
        ParamChecker.notEmpty(guid, "Entity id");
        validateDatasetExists(guid);
        return getInputsGraphForId(guid);
    }

    private String getInputsGraphForId(String guid) {
        InputLineageClosureQuery
                inputsQuery = new InputLineageClosureQuery(AtlasClient.DATA_SET_SUPER_TYPE, SELECT_INSTANCE_GUID,
                guid, HIVE_PROCESS_TYPE_NAME,
                HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
                SELECT_ATTRIBUTES, true, graphPersistenceStrategy, titanGraph);
        return inputsQuery.graph().toInstanceJson();
    }

    @Override
    @GraphTransaction
    public String getOutputsGraphForEntity(String guid) throws AtlasException {
        LOG.info("Fetching lineage outputs graph for entity guid={}", guid);
        ParamChecker.notEmpty(guid, "Entity id");
        validateDatasetExists(guid);
        return getOutputsGraphForId(guid);
    }

    private String getOutputsGraphForId(String guid) {
        OutputLineageClosureQuery outputsQuery =
                new OutputLineageClosureQuery(AtlasClient.DATA_SET_SUPER_TYPE, SELECT_INSTANCE_GUID, guid, HIVE_PROCESS_TYPE_NAME,
                        HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
                        SELECT_ATTRIBUTES, true, graphPersistenceStrategy, titanGraph);
        return outputsQuery.graph().toInstanceJson();
    }

    /**
     * Return the schema for the given tableName.
     *
     * @param datasetName tableName
     * @return Schema as JSON
     */
    @Override
    @GraphTransaction
    public String getSchema(String datasetName) throws AtlasException {
        ParamChecker.notEmpty(datasetName, "table name");
        LOG.info("Fetching schema for tableName={}", datasetName);
        ReferenceableInstance datasetInstance = validateDatasetNameExists(datasetName);

        return getSchemaForId(datasetInstance.getTypeName(), datasetInstance.getId()._getId());
    }

    private String getSchemaForId(String typeName, String guid) throws DiscoveryException {
        final String schemaQuery =
                String.format(propertiesConf.getString(DATASET_SCHEMA_QUERY_PREFIX + typeName), guid);
        return discoveryService.searchByDSL(schemaQuery);
    }

    @Override
    @GraphTransaction
    public String getSchemaForEntity(String guid) throws AtlasException {
        ParamChecker.notEmpty(guid, "Entity id");
        LOG.info("Fetching schema for entity guid={}", guid);
        String typeName = validateDatasetExists(guid);
        return getSchemaForId(typeName, guid);
    }

    /**
     * Validate if indeed this is a table type and exists.
     *
     * @param datasetName table name
     */
    private ReferenceableInstance validateDatasetNameExists(String datasetName) throws AtlasException {
        final String tableExistsQuery = String.format(DATASET_NAME_EXISTS_QUERY, datasetName);
        GremlinQueryResult queryResult = discoveryService.evaluate(tableExistsQuery);
        if (!(queryResult.rows().length() > 0)) {
            throw new EntityNotFoundException(datasetName + " does not exist");
        }

        return (ReferenceableInstance)queryResult.rows().apply(0);
    }

    /**
     * Validate if indeed this is a table type and exists.
     *
     * @param guid entity id
     */
    private String validateDatasetExists(String guid) throws AtlasException {
        final String datasetExistsQuery = String.format(DATASET_EXISTS_QUERY, guid);
        GremlinQueryResult queryResult = discoveryService.evaluate(datasetExistsQuery);
        if (!(queryResult.rows().length() > 0)) {
            throw new EntityNotFoundException("Dataset with guid = " + guid + " does not exist");
        }

        ReferenceableInstance referenceable = (ReferenceableInstance)queryResult.rows().apply(0);
        return referenceable.getTypeName();
    }

}
