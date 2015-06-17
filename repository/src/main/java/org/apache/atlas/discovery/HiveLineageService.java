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
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.ParamChecker;
import org.apache.atlas.PropertiesUtil;
import org.apache.atlas.discovery.graph.DefaultGraphPersistenceStrategy;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.query.Expressions;
import org.apache.atlas.query.GremlinQueryResult;
import org.apache.atlas.query.HiveLineageQuery;
import org.apache.atlas.query.HiveWhereUsedQuery;
import org.apache.atlas.repository.EntityNotFoundException;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.commons.configuration.PropertiesConfiguration;
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
public class HiveLineageService implements LineageService {

    private static final Logger LOG = LoggerFactory.getLogger(HiveLineageService.class);

    private static final Option<List<String>> SELECT_ATTRIBUTES =
            Some.<List<String>>apply(List.<String>fromArray(new String[]{"name"}));

    public static final String HIVE_TABLE_SCHEMA_QUERY_PREFIX = "atlas.lineage.hive.table.schema.query.";

    private static final String HIVE_TABLE_TYPE_NAME;
    private static final String HIVE_PROCESS_TYPE_NAME;
    private static final String HIVE_PROCESS_INPUT_ATTRIBUTE_NAME;
    private static final String HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME;

    private static final String HIVE_TABLE_EXISTS_QUERY;

    private static final PropertiesConfiguration propertiesConf;

    static {
        // todo - externalize this using type system - dog food
        try {
            propertiesConf = PropertiesUtil.getApplicationProperties();
            HIVE_TABLE_TYPE_NAME = propertiesConf.getString("atlas.lineage.hive.table.type.name", "DataSet");
            HIVE_PROCESS_TYPE_NAME = propertiesConf.getString("atlas.lineage.hive.process.type.name", "Process");
            HIVE_PROCESS_INPUT_ATTRIBUTE_NAME = propertiesConf.getString("atlas.lineage.hive.process.inputs.name", "inputs");
            HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME = propertiesConf.getString("atlas.lineage.hive.process.outputs.name", "outputs");

            HIVE_TABLE_EXISTS_QUERY = propertiesConf.getString("atlas.lineage.hive.table.exists.query",
                    "from " + HIVE_TABLE_TYPE_NAME + " where name=\"%s\"");
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }


    private final TitanGraph titanGraph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;
    private final GraphBackedDiscoveryService discoveryService;

    @Inject
    HiveLineageService(GraphProvider<TitanGraph> graphProvider, MetadataRepository metadataRepository,
            GraphBackedDiscoveryService discoveryService) throws DiscoveryException {
        this.titanGraph = graphProvider.get();
        this.graphPersistenceStrategy = new DefaultGraphPersistenceStrategy(metadataRepository);
        this.discoveryService = discoveryService;
    }

    /**
     * Return the lineage outputs for the given tableName.
     *
     * @param tableName tableName
     * @return Lineage Outputs as JSON
     */
    @Override
    @GraphTransaction
    public String getOutputs(String tableName) throws AtlasException {
        LOG.info("Fetching lineage outputs for tableName={}", tableName);
        ParamChecker.notEmpty(tableName, "table name cannot be null");
        validateTableExists(tableName);

        HiveWhereUsedQuery outputsQuery =
                new HiveWhereUsedQuery(HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                        HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
                        SELECT_ATTRIBUTES, true, graphPersistenceStrategy, titanGraph);

        Expressions.Expression expression = outputsQuery.expr();
        LOG.debug("Expression is [" + expression.toString() + "]");
        try {
            return discoveryService.evaluate(expression).toJson();
        } catch (Exception e) { // unable to catch ExpressionException
            throw new DiscoveryException("Invalid expression [" + expression.toString() + "]", e);
        }
    }

    /**
     * Return the lineage outputs graph for the given tableName.
     *
     * @param tableName tableName
     * @return Outputs Graph as JSON
     */
    @Override
    @GraphTransaction
    public String getOutputsGraph(String tableName) throws AtlasException {
        LOG.info("Fetching lineage outputs graph for tableName={}", tableName);
        ParamChecker.notEmpty(tableName, "table name cannot be null");
        validateTableExists(tableName);

        HiveWhereUsedQuery outputsQuery =
                new HiveWhereUsedQuery(HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                        HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
                        SELECT_ATTRIBUTES, true, graphPersistenceStrategy, titanGraph);
        return outputsQuery.graph().toInstanceJson();
    }

    /**
     * Return the lineage inputs for the given tableName.
     *
     * @param tableName tableName
     * @return Lineage Inputs as JSON
     */
    @Override
    @GraphTransaction
    public String getInputs(String tableName) throws AtlasException {
        LOG.info("Fetching lineage inputs for tableName={}", tableName);
        ParamChecker.notEmpty(tableName, "table name cannot be null");
        validateTableExists(tableName);

        HiveLineageQuery inputsQuery = new HiveLineageQuery(HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
                SELECT_ATTRIBUTES, true, graphPersistenceStrategy, titanGraph);

        Expressions.Expression expression = inputsQuery.expr();
        LOG.debug("Expression is [" + expression.toString() + "]");
        try {
            return discoveryService.evaluate(expression).toJson();
        } catch (Exception e) { // unable to catch ExpressionException
            throw new DiscoveryException("Invalid expression [" + expression.toString() + "]", e);
        }
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
        ParamChecker.notEmpty(tableName, "table name cannot be null");
        validateTableExists(tableName);

        HiveLineageQuery inputsQuery = new HiveLineageQuery(HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
                SELECT_ATTRIBUTES, true, graphPersistenceStrategy, titanGraph);
        return inputsQuery.graph().toInstanceJson();
    }

    /**
     * Return the schema for the given tableName.
     *
     * @param tableName tableName
     * @return Schema as JSON
     */
    @Override
    @GraphTransaction
    public String getSchema(String tableName) throws AtlasException {
        LOG.info("Fetching schema for tableName={}", tableName);
        ParamChecker.notEmpty(tableName, "table name cannot be null");
        String typeName = validateTableExists(tableName);

        final String schemaQuery =
                String.format(propertiesConf.getString(HIVE_TABLE_SCHEMA_QUERY_PREFIX + typeName), tableName);
        return discoveryService.searchByDSL(schemaQuery);
    }

    /**
     * Validate if indeed this is a table type and exists.
     *
     * @param tableName table name
     */
    private String validateTableExists(String tableName) throws AtlasException {
        final String tableExistsQuery = String.format(HIVE_TABLE_EXISTS_QUERY, tableName);
        GremlinQueryResult queryResult = discoveryService.evaluate(tableExistsQuery);
        if (!(queryResult.rows().length() > 0)) {
            throw new EntityNotFoundException(tableName + " does not exist");
        }

        ReferenceableInstance referenceable = (ReferenceableInstance)queryResult.rows().apply(0);
        return referenceable.getTypeName();
    }
}
