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
import org.apache.hadoop.metadata.discovery.graph.DefaultGraphPersistenceStrategy;
import org.apache.hadoop.metadata.query.Expressions;
import org.apache.hadoop.metadata.query.GremlinQuery;
import org.apache.hadoop.metadata.query.GremlinTranslator;
import org.apache.hadoop.metadata.query.HiveLineageQuery;
import org.apache.hadoop.metadata.query.HiveWhereUsedQuery;
import org.apache.hadoop.metadata.query.QueryProcessor;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.repository.graph.GraphProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.immutable.List;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Hive implementation of Lineage service interface.
 */
@Singleton
public class HiveLineageService implements LineageService {

    private static final Logger LOG = LoggerFactory.getLogger(HiveLineageService.class);

    // todo - externalize these into configuration
    private static final String HIVE_TABLE_TYPE_NAME = "hive_table";
    private static final String HIVE_PROCESS_TYPE_NAME = "hive_process";
    private static final String HIVE_PROCESS_INPUT_ATTRIBUTE_NAME = "inputTables";
    private static final String HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME = "outputTables";

    private final TitanGraph titanGraph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;

    @Inject
    HiveLineageService(GraphProvider<TitanGraph> graphProvider,
                       MetadataRepository metadataRepository) throws DiscoveryException {
        this.titanGraph = graphProvider.get();
        this.graphPersistenceStrategy = new DefaultGraphPersistenceStrategy(metadataRepository);
    }

    /**
     * Return the lineage outputs for the given tableName.
     *
     * @param tableName tableName
     * @return Lineage Outputs as JSON
     */
    @Override
    public String getOutputs(String tableName) throws DiscoveryException {
        LOG.info("Fetching lineage outputs for tableName={}", tableName);

        try {
            HiveWhereUsedQuery outputsQuery = new HiveWhereUsedQuery(
                    HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                    HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME,
                    Option.empty(), Option.<List<String>>empty(), true,
                    graphPersistenceStrategy, titanGraph);

            Expressions.Expression expression = outputsQuery.expr();
            Expressions.Expression validatedExpression = QueryProcessor.validate(expression);
            GremlinQuery gremlinQuery = new GremlinTranslator(
                    validatedExpression, graphPersistenceStrategy).translate();
            if (LOG.isDebugEnabled()) {
                System.out.println("Query = " + validatedExpression);
                System.out.println("Expression Tree = " + validatedExpression.treeString());
                System.out.println("Gremlin Query = " + gremlinQuery.queryStr());
            }

            return outputsQuery.evaluate().toJson();
        } catch (Exception e) { // unable to catch ExpressionException
            throw new DiscoveryException("Invalid expression", e);
        }
    }

    /**
     * Return the lineage inputs for the given tableName.
     *
     * @param tableName tableName
     * @return Lineage Inputs as JSON
     */
    @Override
    public String getInputs(String tableName) throws DiscoveryException {
        LOG.info("Fetching lineage inputs for tableName={}", tableName);

        try {
            HiveLineageQuery inputsQuery = new HiveLineageQuery(
                    HIVE_TABLE_TYPE_NAME, tableName, HIVE_PROCESS_TYPE_NAME,
                    HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME,
                    Option.empty(), Option.<List<String>>empty(), true,
                    graphPersistenceStrategy, titanGraph);

            Expressions.Expression expression = inputsQuery.expr();
            Expressions.Expression validatedExpression = QueryProcessor.validate(expression);
            GremlinQuery gremlinQuery =
                    new GremlinTranslator(validatedExpression, graphPersistenceStrategy).translate();
            if (LOG.isDebugEnabled()) {
                System.out.println("Query = " + validatedExpression);
                System.out.println("Expression Tree = " + validatedExpression.treeString());
                System.out.println("Gremlin Query = " + gremlinQuery.queryStr());
            }

            return inputsQuery.evaluate().toJson();
        } catch (Exception e) { // unable to catch ExpressionException
            throw new DiscoveryException("Invalid expression", e);
        }
    }
}
