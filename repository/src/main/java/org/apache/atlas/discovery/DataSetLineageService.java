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

import com.google.common.base.Splitter;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasProperties;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.discovery.graph.DefaultGraphPersistenceStrategy;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.query.ClosureQuery;
import org.apache.atlas.query.Expressions;
import org.apache.atlas.query.GremlinEvaluator;
import org.apache.atlas.query.GremlinQuery;
import org.apache.atlas.query.GremlinQueryResult;
import org.apache.atlas.query.GremlinTranslator;
import org.apache.atlas.query.InputLineageClosureQuery;
import org.apache.atlas.query.OutputLineageClosureQuery;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.query.QueryProcessor;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.SchemaNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.collection.immutable.List;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.script.ScriptException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Hive implementation of Lineage service interface.
 */
@Singleton
public class DataSetLineageService implements LineageService {

    private static final Logger LOG = LoggerFactory.getLogger(DataSetLineageService.class);

    private static final Option<List<String>> SELECT_ATTRIBUTES =
            Some.<List<String>>apply(List.<String>fromArray(new String[]{AtlasClient.NAME,
                    AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME}));
    public static final String SELECT_INSTANCE_GUID = "__guid";

    public static final String DATASET_SCHEMA_QUERY_PREFIX = "atlas.lineage.schema.query.";

    public static final String DATASET_SCHEMA_ATTRIBUTE = "atlas.lineage.schema.attribute.";

    private static final String HIVE_PROCESS_TYPE_NAME = "Process";
    private static final String HIVE_PROCESS_INPUT_ATTRIBUTE_NAME = "inputs";
    private static final String HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME = "outputs";

    private static final String INPUT_PROCESS_EDGE      =  "__Process.inputs";
    private static final String OUTPUT_PROCESS_EDGE     =  "__Process.outputs";

    private static final Configuration propertiesConf;

    private MetadataRepository metadataRepository;

    private final TypeSystem typeSystem = TypeSystem.getInstance();

    private final GraphHelper graphHelper = GraphHelper.getInstance();

    private final TitanGraph titanGraph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;
    private final GraphBackedDiscoveryService discoveryService;

    private Map<String, String> schemaAttributeCache = new HashMap<>();

    /**
     *  Gremlin query to retrieve all (no fixed depth) input/output lineage for a DataSet entity.
     *  return list of Atlas vertices paths.
     */
    private static final String FULL_LINEAGE_QUERY    = "g.v(%s).as('src').in('%s').out('%s')." +
        "loop('src', {((it.path.contains(it.object)) ? false : true)}, " +
        "{((it.object.'__superTypeNames') ? " +
        "(it.object.'__superTypeNames'.contains('DataSet')) : false)})." +
        "enablePath().as('dest').select(['src', 'dest'], {[it.'Asset.name', it.'Referenceable.qualifiedName']}, {[it.'Asset.name', it.'Referenceable.qualifiedName']}).path().toList()";

    static {
        try {
            propertiesConf = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }


    @Inject
    DataSetLineageService(GraphProvider<TitanGraph> graphProvider, MetadataRepository metadataRepository,
                          GraphBackedDiscoveryService discoveryService) throws DiscoveryException {
        this.titanGraph = graphProvider.get();
        this.metadataRepository = metadataRepository;
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

    private String getInputsGraphForId(String guid) throws AtlasException {

        Vertex instanceVertex = GraphHelper.getInstance().getVertexForGUID(guid);
        Object instanceVertexId = instanceVertex.getId();

        String lineageQuery = String.format(FULL_LINEAGE_QUERY, instanceVertexId, OUTPUT_PROCESS_EDGE, INPUT_PROCESS_EDGE);
        InputLineageClosureQuery
            inputsQuery = new InputLineageClosureQuery(AtlasClient.DATA_SET_SUPER_TYPE, SELECT_INSTANCE_GUID,
            guid, HIVE_PROCESS_TYPE_NAME,
            HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
            SELECT_ATTRIBUTES, true, graphPersistenceStrategy, titanGraph);

        LOG.info("Evaluating gremlin lineage input query ={}", lineageQuery);

        GremlinQueryResult result = evaluateLineageQuery(inputsQuery, lineageQuery);
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

    private String getOutputsGraphForId(String guid) throws EntityNotFoundException {
        Object instanceVertexId = graphHelper.getVertexId(guid);
        String lineageQuery = String.format(FULL_LINEAGE_QUERY, instanceVertexId, INPUT_PROCESS_EDGE, OUTPUT_PROCESS_EDGE);

        OutputLineageClosureQuery outputsQuery =
                new OutputLineageClosureQuery(AtlasClient.DATA_SET_SUPER_TYPE, SELECT_INSTANCE_GUID, guid, HIVE_PROCESS_TYPE_NAME,
                        HIVE_PROCESS_INPUT_ATTRIBUTE_NAME, HIVE_PROCESS_OUTPUT_ATTRIBUTE_NAME, Option.empty(),
                        SELECT_ATTRIBUTES, true, graphPersistenceStrategy, titanGraph);

        LOG.info("Evaluating gremlin lineage output query ={}", lineageQuery);

        GremlinQueryResult result = evaluateLineageQuery(outputsQuery, lineageQuery);
        return outputsQuery.graph(result).toInstanceJson();
    }

    private GremlinQueryResult evaluateLineageQuery(ClosureQuery closureQuery, String lineageQuery) {
        Expressions.Expression validatedExpression = QueryProcessor.validate(closureQuery.expr());
        GremlinQuery gremlinQuery = new GremlinTranslator(validatedExpression, graphPersistenceStrategy).translate();
        //Replace with handcrafted gremlin till we optimize the DSL query associated with guid + typeName
        GremlinQuery optimizedQuery = new GremlinQuery(gremlinQuery.expr(), lineageQuery, gremlinQuery.resultMaping());


        return new GremlinEvaluator(optimizedQuery, graphPersistenceStrategy, titanGraph).evaluate();
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

    private String getSchemaForId(String typeName, String guid) throws AtlasException {
        String configName = DATASET_SCHEMA_QUERY_PREFIX + typeName;
        if (propertiesConf.getString(configName) != null) {
            final String schemaQuery =
                String.format(propertiesConf.getString(configName), guid);
            int limit = AtlasProperties.getProperty(AtlasProperties.AtlasProperty.SEARCH_MAX_LIMIT);
            return discoveryService.searchByDSL(schemaQuery, new QueryParams(limit, 0));
        }
        throw new SchemaNotFoundException("Schema is not configured for type " + typeName + ". Configure " + configName);
    }

    private String getSchemaForId(ITypedReferenceableInstance instance, ClassType type) throws AtlasException {
        String configName = DATASET_SCHEMA_QUERY_PREFIX + instance.getTypeName();
        String schemaAttrName = null;

        if ( schemaAttributeCache.containsKey(instance.getTypeName()) ) {
            schemaAttrName = schemaAttributeCache.get(instance.getTypeName());
        } else {
            schemaAttrName = getSchemaAttributeName(configName, instance.getTypeName());
            schemaAttributeCache.put(instance.getTypeName(), schemaAttrName);
        }

        if (schemaAttrName != null) {
            java.util.List schemaValue = (java.util.List) instance.get(schemaAttrName);
            GremlinQueryResult queryResult = new GremlinQueryResult(schemaAttrName, type, schemaValue);
            return queryResult.toJson();
        }
        throw new SchemaNotFoundException("Schema is not configured for type " + instance.getTypeName() + ". Configure " + configName);
    }


    private String getSchemaAttributeName(String configName, String typeName) throws SchemaNotFoundException {
        String schemaQuery = propertiesConf.getString(configName);
        final String[] configs = schemaQuery != null ? schemaQuery.split(",") : null;

        if (configs != null && configs.length == 2) {
            LOG.info("Extracted schema attribute {} for type {} with query {} ", configs[1], typeName, schemaQuery);
            return configs[1].trim();
        } else {
            throw new SchemaNotFoundException("Schema is not configured as expected for type " + typeName);
        }
    }

    @Override
    @GraphTransaction
    public String getSchemaForEntity(String guid) throws AtlasException {
        guid = ParamChecker.notEmpty(guid, "Entity id");
        LOG.info("Fetching schema for entity guid={}", guid);
        Pair<ITypedReferenceableInstance, ClassType> instanceClassTypePair = validateDatasetExists(guid);
        return getSchemaForId(instanceClassTypePair.getLeft(), instanceClassTypePair.getRight());
    }

    /**
     * Validate if indeed this is a table type and exists.
     *
     * @param datasetName table name
     */
    private TypeUtils.Pair<String, String> validateDatasetNameExists(String datasetName) throws AtlasException {
        Iterator<Vertex> results = titanGraph.query().has("Referenceable.qualifiedName", datasetName)
                                             .has(Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name())
                                             .has(Constants.SUPER_TYPES_PROPERTY_KEY, AtlasClient.DATA_SET_SUPER_TYPE).limit(1)
                                             .vertices().iterator();
        while (results.hasNext()) {
            Vertex vertex = results.next();
            return TypeUtils.Pair.of(GraphHelper.getTypeName(vertex), GraphHelper.getIdFromVertex(vertex));
        }
        throw new EntityNotFoundException("Dataset with name = " + datasetName + " does not exist");
    }

    /**
     * Validate if indeed this is a table type and exists.
     *
     * @param guid entity id
     */
    private Pair<ITypedReferenceableInstance, ClassType> validateDatasetExists(String guid) throws AtlasException {

        ITypedReferenceableInstance instance = metadataRepository.getEntityDefinition(guid);
        String typeName = instance.getTypeName();
        ClassType clsType = typeSystem.getDataType(ClassType.class, typeName);
        if ( !clsType.superTypes.contains(AtlasClient.DATA_SET_SUPER_TYPE) ) {
            throw new EntityNotFoundException("Dataset with guid = " + guid + " does not exist");
        }
        return Pair.of(instance, clsType);
    }
}
