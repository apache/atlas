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

package org.apache.hadoop.metadata.discovery;

import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.metadata.GraphTransaction;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.PropertiesUtil;
import org.apache.hadoop.metadata.discovery.graph.DefaultGraphPersistenceStrategy;
import org.apache.hadoop.metadata.discovery.graph.GraphBackedDiscoveryService;
import org.apache.hadoop.metadata.query.Expressions;
import org.apache.hadoop.metadata.query.HiveLineageQuery;
import org.apache.hadoop.metadata.query.HiveWhereUsedQuery;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.repository.graph.GraphProvider;
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

    private static final String HIVE_TABLE_TYPE_NAME;
    private static final String HIVE_TABLE_COLUMNS_ATTRIBUTE_NAME;
    private static final String HIVE_PROCESS_TYPE_NAME;
    private static final String HIVE_PROCESS_INPUT_ATTRIBUTE_NAME;
    private static final String HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME;

    static {
        // todo - externalize this using type system - dog food
        try {
            PropertiesConfiguration conf = PropertiesUtil.getApplicationProperties();
            HIVE_TABLE_TYPE_NAME =
                    conf.getString("metadata.lineage.hive.table.type.name",  "hive_table");
            HIVE_TABLE_COLUMNS_ATTRIBUTE_NAME =
                    conf.getString("metadata.lineage.hive.table.column.name",  "columns");

            HIVE_PROCESS_TYPE_NAME =
                    conf.getString("metadata.lineage.hive.process.type.name", "hive_process");
            HIVE_PROCESS_INPUT_ATTRIBUTE_NAME =
                    conf.getString("metadata.lineage.hive.process.inputs.name", "inputTables");
            HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME =
                    conf.getString("metadata.lineage.hive.process.outputs.name", "outputTables");
        } catch (MetadataException e) {
            throw new RuntimeException(e);
        }
    }


    private final TitanGraph titanGraph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;
    private final GraphBackedDiscoveryService discoveryService;

    @Inject
    HiveLineageService(GraphProvider<TitanGraph> graphProvider,
                       MetadataRepository metadataRepository,
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
    public String getOutputs(String tableName) throws DiscoveryException {
        LOG.info("Fetching lineage outputs for tableName={}", tableName);

        HiveWhereUsedQuery outputsQuery = new HiveWhereUsedQuery(
                HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME,
                Option.empty(), SELECT_ATTRIBUTES, true,
                graphPersistenceStrategy, titanGraph);

        Expressions.Expression expression = outputsQuery.expr();
        LOG.debug("Expression is [" + expression.toString() +"]");
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
    public String getOutputsGraph(String tableName) throws DiscoveryException {
        LOG.info("Fetching lineage outputs graph for tableName={}", tableName);

        HiveWhereUsedQuery outputsQuery = new HiveWhereUsedQuery(
                HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME,
                Option.empty(), SELECT_ATTRIBUTES, true,
                graphPersistenceStrategy, titanGraph);
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
    public String getInputs(String tableName) throws DiscoveryException {
        LOG.info("Fetching lineage inputs for tableName={}", tableName);

        HiveLineageQuery inputsQuery = new HiveLineageQuery(
                HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME,
                Option.empty(), SELECT_ATTRIBUTES, true,
                graphPersistenceStrategy, titanGraph);

        Expressions.Expression expression = inputsQuery.expr();
        LOG.debug("Expression is [" + expression.toString() +"]");
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
    public String getInputsGraph(String tableName) throws DiscoveryException {
        LOG.info("Fetching lineage inputs graph for tableName={}", tableName);

        HiveLineageQuery inputsQuery = new HiveLineageQuery(
                HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME,
                Option.empty(), SELECT_ATTRIBUTES, true,
                graphPersistenceStrategy, titanGraph);
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
    public String getSchema(String tableName) throws DiscoveryException {
        // todo - validate if indeed this is a table type and exists
        String schemaQuery = HIVE_TABLE_TYPE_NAME
                + " where name=\"" + tableName + "\""
                + ", " + HIVE_TABLE_COLUMNS_ATTRIBUTE_NAME
                // + " as column select column.name, column.dataType, column.comment"
        ;
        return discoveryService.searchByDSL(schemaQuery);
    }
}
