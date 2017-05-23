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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.discovery.graph.DefaultGraphPersistenceStrategy;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.query.GremlinQueryResult;
import org.apache.atlas.query.InputLineageClosureQuery;
import org.apache.atlas.query.OutputLineageClosureQuery;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.SchemaNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.Option;
import scala.Some;
import scala.collection.JavaConversions;
import scala.collection.immutable.List;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Hive implementation of Lineage service interface.
 */
@Singleton
@Component
public class DataSetLineageService implements LineageService {

    private static final Logger LOG = LoggerFactory.getLogger(DataSetLineageService.class);

    private static final Option<List<String>> SELECT_ATTRIBUTES =
            Some.apply(JavaConversions.asScalaBuffer(Arrays.asList(AtlasClient.NAME,
                    AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)).toList());
    public static final String SELECT_INSTANCE_GUID = "__guid";

    public static final String DATASET_SCHEMA_QUERY_PREFIX = "atlas.lineage.schema.query.";

    private static final String HIVE_PROCESS_TYPE_NAME = "Process";
    private static final String HIVE_PROCESS_INPUT_ATTRIBUTE_NAME = "inputs";
    private static final String HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME = "outputs";

    private static final Configuration propertiesConf;

    static {
        try {
            propertiesConf = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }


    private final AtlasGraph graph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;
    private final GraphBackedDiscoveryService discoveryService;

    @Inject
    DataSetLineageService(MetadataRepository metadataRepository,
                          GraphBackedDiscoveryService discoveryService,
                          AtlasGraph atlasGraph) throws DiscoveryException {
        this.graph = atlasGraph;
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
        datasetName = ParamChecker.notEmpty(datasetName, "dataset name");
        TypeUtils.Pair<String, String> typeIdPair = validateDatasetNameExists(datasetName);
        return getOutputsGraphForId(typeIdPair.right);
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
        tableName = ParamChecker.notEmpty(tableName, "table name");
        TypeUtils.Pair<String, String> typeIdPair = validateDatasetNameExists(tableName);
        return getInputsGraphForId(typeIdPair.right);
    }

    @Override
    @GraphTransaction
    public String getInputsGraphForEntity(String guid) throws AtlasException {
        LOG.info("Fetching lineage inputs graph for entity={}", guid);
        guid = ParamChecker.notEmpty(guid, "Entity id");
        validateDatasetExists(guid);
        return getInputsGraphForId(guid);
    }

    private String getInputsGraphForId(String guid) {
        InputLineageClosureQuery
                inputsQuery = new InputLineageClosureQuery(AtlasClient.DATA_SET_SUPER_TYPE, SELECT_INSTANCE_GUID,
                guid, HIVE_PROCESS_TYPE_NAME,
                HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
                SELECT_ATTRIBUTES, true, graphPersistenceStrategy, graph);
        GremlinQueryResult result = inputsQuery.evaluate();
        return inputsQuery.graph(result).toInstanceJson();
    }

    @Override
    @GraphTransaction
    public String getOutputsGraphForEntity(String guid) throws AtlasException {
        LOG.info("Fetching lineage outputs graph for entity guid={}", guid);
        guid = ParamChecker.notEmpty(guid, "Entity id");
        validateDatasetExists(guid);
        return getOutputsGraphForId(guid);
    }

    private String getOutputsGraphForId(String guid) {
        OutputLineageClosureQuery outputsQuery =
                new OutputLineageClosureQuery(AtlasClient.DATA_SET_SUPER_TYPE, SELECT_INSTANCE_GUID, guid, HIVE_PROCESS_TYPE_NAME,
                        HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
                        SELECT_ATTRIBUTES, true, graphPersistenceStrategy, graph);
        GremlinQueryResult result = outputsQuery.evaluate();
        return outputsQuery.graph(result).toInstanceJson();
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
        datasetName = ParamChecker.notEmpty(datasetName, "table name");
        LOG.info("Fetching schema for tableName={}", datasetName);
        TypeUtils.Pair<String, String> typeIdPair = validateDatasetNameExists(datasetName);

        return getSchemaForId(typeIdPair.left, typeIdPair.right);
    }

    private String getSchemaForId(String typeName, String guid) throws DiscoveryException, SchemaNotFoundException {
        String configName = DATASET_SCHEMA_QUERY_PREFIX + typeName;
        if (propertiesConf.getString(configName) != null) {
            final String schemaQuery =
                String.format(propertiesConf.getString(configName), guid);
            int limit = AtlasConfiguration.SEARCH_MAX_LIMIT.getInt();
            return discoveryService.searchByDSL(schemaQuery, new QueryParams(limit, 0));
        }
        throw new SchemaNotFoundException("Schema is not configured for type " + typeName + ". Configure " + configName);
    }

    @Override
    @GraphTransaction
    public String getSchemaForEntity(String guid) throws AtlasException {
        guid = ParamChecker.notEmpty(guid, "Entity id");
        LOG.info("Fetching schema for entity guid={}", guid);
        String typeName = validateDatasetExists(guid);
        return getSchemaForId(typeName, guid);
    }

    /**
     * Validate if indeed this is a table type and exists.
     *
     * @param datasetName table name
     */
    private TypeUtils.Pair<String, String> validateDatasetNameExists(String datasetName) throws AtlasException {
        Iterator<AtlasVertex> results = graph.query().has("Referenceable.qualifiedName", datasetName)
                                             .has(Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name())
                                             .has(Constants.SUPER_TYPES_PROPERTY_KEY, AtlasClient.DATA_SET_SUPER_TYPE)
                                             .vertices().iterator();
        while (results.hasNext()) {
            AtlasVertex vertex = results.next();
            return TypeUtils.Pair.of(GraphHelper.getTypeName(vertex), GraphHelper.getGuid(vertex));
        }
        throw new EntityNotFoundException("Dataset with name = " + datasetName + " does not exist");
    }

    /**
     * Validate if indeed this is a table type and exists.
     *
     * @param guid entity id
     */
    private String validateDatasetExists(String guid) throws AtlasException {
        for (AtlasVertex vertex : (Iterable<AtlasVertex>) graph.query().has(Constants.GUID_PROPERTY_KEY, guid)
                .has(Constants.SUPER_TYPES_PROPERTY_KEY, AtlasClient.DATA_SET_SUPER_TYPE)
                .vertices()) {
            return GraphHelper.getTypeName(vertex);
        }
        throw new EntityNotFoundException("Dataset with guid = " + guid + " does not exist");
    }
}
