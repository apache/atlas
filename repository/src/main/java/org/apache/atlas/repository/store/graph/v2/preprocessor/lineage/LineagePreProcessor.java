package org.apache.atlas.repository.store.graph.v2.preprocessor.lineage;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.indexSearchPaginated;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_CONNECTION_QN;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getStringAttribute;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class LineagePreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LineagePreProcessor.class);
    private static final List<String> FETCH_ENTITY_ATTRIBUTES = Arrays.asList(ATTR_POLICY_CONNECTION_QN);
    private static final String HAS_LINEAGE = "__hasLineage";

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final AtlasEntityStore entityStore;
    private final EntityDiscoveryService discovery;

    public LineagePreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, AtlasEntityStore entityStore) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;
        this.discovery = initializeDiscoveryService(typeRegistry, graph);
    }

    private EntityDiscoveryService initializeDiscoveryService(AtlasTypeRegistry typeRegistry, AtlasGraph graph) {
        try {
            return new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
        } catch (AtlasException e) {
            LOG.error("Error initializing EntityDiscoveryService", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processAttributesForLineagePreprocessor");

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("LineageProcessPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
            }

            AtlasEntity entity = (AtlasEntity) entityStruct;
            AtlasVertex vertex = context.getVertex(entity.getGuid());
            ArrayList<String> connectionProcessQNs = getConnectionProcessesForProcessEntity(entity, vertex);

            switch (operation) {
                case CREATE:
                    processCreateLineageProcess(entity, connectionProcessQNs);
                    break;
                case UPDATE:
                    processUpdateLineageProcess(entity, vertex, context, connectionProcessQNs);
                    break;
            }
        } catch (Exception exp) {
            LOG.error("Error in LineagePreProcessor.processAttributes", exp);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private ArrayList<String> getConnectionProcessesForProcessEntity(AtlasEntity processEntity, AtlasVertex vertex) throws AtlasBaseException {
        ArrayList<AtlasObjectId> inputsAssets = getInputsAssets(processEntity, vertex);
        ArrayList<AtlasObjectId> outputsAssets = getOutputsAssets(processEntity, vertex);

        Set<String> inputConnectionQNs = getConnectionQualifiedNames(inputsAssets);
        Set<String> outputConnectionQNs = getConnectionQualifiedNames(outputsAssets);

        return createConnectionProcesses(inputConnectionQNs, outputConnectionQNs);
    }

    private ArrayList<AtlasObjectId> getInputsAssets(AtlasEntity processEntity, AtlasVertex vertex) throws AtlasBaseException {
        ArrayList<AtlasObjectId> inputsAssets = (ArrayList<AtlasObjectId>) processEntity.getRelationshipAttributes().get("inputs");
        if ((inputsAssets == null || inputsAssets.isEmpty()) && vertex.isIdAssigned()) {
            inputsAssets = new ArrayList<>();
            AtlasEntity storedProduct = entityRetriever.toAtlasEntity(vertex);
            if (storedProduct != null) {
                inputsAssets = (ArrayList<AtlasObjectId>) storedProduct.getRelationshipAttribute("inputs");
            }
        }
        return inputsAssets;
    }

    private ArrayList<AtlasObjectId> getOutputsAssets(AtlasEntity processEntity, AtlasVertex vertex) throws AtlasBaseException {
        ArrayList<AtlasObjectId> outputsAssets = (ArrayList<AtlasObjectId>) processEntity.getRelationshipAttributes().get("outputs");
        if ((outputsAssets == null || outputsAssets.isEmpty()) && vertex.isIdAssigned()) {
            outputsAssets = new ArrayList<>();
            AtlasEntity storedProduct = entityRetriever.toAtlasEntity(vertex);
            if (storedProduct != null) {
                outputsAssets = (ArrayList<AtlasObjectId>) storedProduct.getRelationshipAttribute("outputs");
            }
        }
        return outputsAssets;
    }

    private Set<String> getConnectionQualifiedNames(ArrayList<AtlasObjectId> assets) throws AtlasBaseException {
        Set<String> connectionQNs = new HashSet<>();
        for (AtlasObjectId asset : assets) {

            AtlasEntity assetEntity = entityRetriever.toAtlasEntity(asset);
            String connectionQN = getStringAttribute(assetEntity, "connectionQualifiedName");
            if (connectionQN != null) {
                connectionQNs.add(connectionQN);
            }
        }
        return connectionQNs;
    }

    private ArrayList<String> createConnectionProcesses(Set<String> inputConnectionQNs, Set<String> outputConnectionQNs) throws AtlasBaseException {
        Set<Map<String, Object>> uniqueConnectionProcesses = new HashSet<>();
        for (String inputConnectionQN : inputConnectionQNs) {
            for (String outputConnectionQN : outputConnectionQNs) {
                if (!inputConnectionQN.equals(outputConnectionQN)) {
                    uniqueConnectionProcesses.add(createConnectionProcessMap(inputConnectionQN, outputConnectionQN));
                }
            }
        }

        return getConnectionProcessList(uniqueConnectionProcesses);
    }

    private Map<String, Object> createConnectionProcessMap(String inputConnectionQN, String outputConnectionQN) {
        String connectionProcessName = "(" + inputConnectionQN + ")->(" + outputConnectionQN + ")";
        String connectionProcessQualifiedName = outputConnectionQN + "/" + connectionProcessName;

        Map<String, Object> connectionProcessMap = new HashMap<>();
        connectionProcessMap.put("input", inputConnectionQN);
        connectionProcessMap.put("output", outputConnectionQN);
        connectionProcessMap.put("connectionProcessName", connectionProcessName);
        connectionProcessMap.put("connectionProcessQualifiedName", connectionProcessQualifiedName);

        return connectionProcessMap;
    }

    private ArrayList<String> getConnectionProcessList(Set<Map<String, Object>> uniqueConnectionProcesses) throws AtlasBaseException {
        ArrayList<String> connectionProcessList = new ArrayList<>();
        for (Map<String, Object> connectionProcessInfo : uniqueConnectionProcesses) {
            AtlasEntity connectionProcess = getOrCreateConnectionProcess(connectionProcessInfo);
            connectionProcessList.add(connectionProcess.getAttribute(QUALIFIED_NAME).toString());
        }
        return connectionProcessList;
    }

    private AtlasEntity getOrCreateConnectionProcess(Map<String, Object> connectionProcessInfo) throws AtlasBaseException {
        AtlasObjectId atlasObjectId = new AtlasObjectId(CONNECTION_PROCESS_ENTITY_TYPE, mapOf(QUALIFIED_NAME, connectionProcessInfo.get("connectionProcessQualifiedName")));
        AtlasVertex connectionProcessVertex = getConnectionProcessVertex(atlasObjectId);

        if (connectionProcessVertex == null) {
            return createConnectionProcessEntity(connectionProcessInfo);
        } else {
            if(Objects.equals(connectionProcessVertex.getProperty("__state", String.class), "DELETED")){
                connectionProcessVertex.setProperty("__state", "ACTIVE");
            }
            return entityRetriever.toAtlasEntity(connectionProcessVertex);
        }
    }

    private AtlasVertex getConnectionProcessVertex(AtlasObjectId atlasObjectId) throws AtlasBaseException {
        try {
            return entityRetriever.getEntityVertex(atlasObjectId);
        } catch (AtlasBaseException exp) {
            if (exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                return null;
            } else {
                throw exp;
            }
        }
    }

    private void processCreateLineageProcess(AtlasEntity entity, ArrayList<String> connectionProcessList) {
        if (!connectionProcessList.isEmpty()) {
            entity.setAttribute(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, connectionProcessList);
        }
    }

    private void processUpdateLineageProcess(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context, ArrayList<String> newConnectionProcessList) throws AtlasBaseException {
        List<String> oldConnectionProcessList = getOldConnectionProcessList(vertex);

        Set<String> connectionProcessesToRemove = new HashSet<>(oldConnectionProcessList);
        connectionProcessesToRemove.removeAll(newConnectionProcessList);

        for (String connectionProcessQn : connectionProcessesToRemove) {
            if (!checkIfChildProcessExistForConnectionProcess(connectionProcessQn)) {
                deleteConnectionProcess(connectionProcessQn);
            }
        }

        entity.setAttribute(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, newConnectionProcessList);
    }

    private List<String> getOldConnectionProcessList(AtlasVertex vertex) {
        try {
            Object propertyValue = vertex.getProperty(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, Object.class);
            if (propertyValue instanceof String) {
                return Arrays.asList((String) propertyValue);
            } else if (propertyValue instanceof List) {
                return (List<String>) propertyValue;
            } else if (propertyValue != null) {
                return Collections.singletonList(propertyValue.toString());
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private AtlasEntity createConnectionProcessEntity(Map<String, Object> connectionProcessInfo) throws AtlasBaseException {
        AtlasEntity processEntity = new AtlasEntity(CONNECTION_PROCESS_ENTITY_TYPE);
        processEntity.setAttribute(NAME, connectionProcessInfo.get("connectionProcessName"));
        processEntity.setAttribute(QUALIFIED_NAME, connectionProcessInfo.get("connectionProcessQualifiedName"));
        processEntity.setAttribute(HAS_LINEAGE, true);

        AtlasObjectId inputConnection = new AtlasObjectId(CONNECTION_ENTITY_TYPE, mapOf(QUALIFIED_NAME, connectionProcessInfo.get("input")));
        AtlasObjectId outputConnection = new AtlasObjectId(CONNECTION_ENTITY_TYPE, mapOf(QUALIFIED_NAME, connectionProcessInfo.get("output")));

        Map<String, Object> relationshipAttributes = new HashMap<>();
        relationshipAttributes.put("inputs", Collections.singletonList(inputConnection));
        relationshipAttributes.put("outputs", Collections.singletonList(outputConnection));
        processEntity.setRelationshipAttributes(relationshipAttributes);

        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            AtlasEntity.AtlasEntitiesWithExtInfo processExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            processExtInfo.addEntity(processEntity);
            EntityStream entityStream = new AtlasEntityStream(processExtInfo);
            entityStore.createOrUpdate(entityStream, false);

            checkAndUpdateConnectionLineage(inputConnection, true);
            checkAndUpdateConnectionLineage(outputConnection, true);
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(false);
        }

        return processEntity;
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeleteLineageProcess");

        try {
            Set<String> connectionProcessQNs = getConnectionProcessQNs(vertex);
            if (connectionProcessQNs.isEmpty()) {
                return;
            }

            for (String connectionProcessQn : connectionProcessQNs) {
                if (!checkIfChildProcessExistForConnectionProcess(connectionProcessQn)) {
                    deleteConnectionProcess(connectionProcessQn);
                }
            }
        }
        catch (Exception exp) {
            LOG.error("Error in LineagePreProcessor.processDelete", exp);
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private boolean checkIfChildProcessExistForConnectionProcess(String connectionProcessQn) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("checkIfMoreChildProcessExistForConnectionProcess");

        try {
            List<Map<String, Object>> mustClauseList = Arrays.asList(
                    mapOf("term", mapOf("__typeName.keyword", PROCESS_ENTITY_TYPE)),
                    mapOf("term", mapOf("__state", "ACTIVE")),
                    mapOf("term", mapOf(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, connectionProcessQn))
            );

            Map<String, Object> dsl = mapOf("query", mapOf("bool", mapOf("must", mustClauseList)));

            List<AtlasEntityHeader> process = indexSearchPaginated(dsl, new HashSet<>(Arrays.asList(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME)), this.discovery);

            return CollectionUtils.isNotEmpty(process) && process.size() > 1;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private Set<String> getConnectionProcessQNs(AtlasVertex vertex) {
        Set<String> connectionProcessQNs = new HashSet<>();
        try {
            Iterable<Object> values = vertex.getPropertyValues(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, Object.class);
            if (values != null) {
                for (Object value : values) {
                    if (value != null) {
                        connectionProcessQNs.add(value.toString());
                    }
                }
            }
        } catch (Exception e) {
            try {
                String value = vertex.getProperty(PARENT_CONNECTION_PROCESS_QUALIFIED_NAME, String.class);
                if (StringUtils.isNotEmpty(value)) {
                    connectionProcessQNs.add(value);
                }
            } catch (Exception ex) {
                LOG.warn("Error getting parentConnectionProcessQualifiedName property", ex);
            }
        }
        return connectionProcessQNs;
    }

    private void deleteConnectionProcess(String connectionProcessQn) throws AtlasBaseException {
        AtlasObjectId atlasObjectId = new AtlasObjectId(CONNECTION_PROCESS_ENTITY_TYPE, mapOf(QUALIFIED_NAME, connectionProcessQn));

        try {
            AtlasVertex connectionProcessVertex = entityRetriever.getEntityVertex(atlasObjectId);
            connectionProcessVertex.setProperty("__state", "DELETED");
            updateConnectionsHasLineageForConnectionProcess(connectionProcessVertex);
            entityStore.deleteById(connectionProcessVertex.getProperty("__guid", String.class));
        } catch (AtlasBaseException exp) {
            if (!exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                throw exp;
            }
        }

    }

    private void updateConnectionsHasLineageForConnectionProcess(AtlasVertex connectionProcessVertex) throws AtlasBaseException {
        try {
            AtlasEntity connectionProcess = entityRetriever.toAtlasEntity(connectionProcessVertex);

            List<AtlasObjectId> inputConnQNs = (List<AtlasObjectId>) connectionProcess.getRelationshipAttribute("inputs");
            List<AtlasObjectId> outputConnQNs = (List<AtlasObjectId>) connectionProcess.getRelationshipAttribute("outputs");

            Set<AtlasObjectId> connectionsToUpdate = new HashSet<>();
            connectionsToUpdate.addAll(inputConnQNs);
            connectionsToUpdate.addAll(outputConnQNs);

            for (AtlasObjectId connectionQN : connectionsToUpdate) {
                checkAndUpdateConnectionLineage(connectionQN, hasActiveConnectionProcesses(entityRetriever.getEntityVertex(connectionQN)));
            }
        } catch (AtlasBaseException exp) {
            if (!exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                throw exp;
            }
        }
    }

    private void checkAndUpdateConnectionLineage(AtlasObjectId atlasObjectId, boolean hasLineage) throws AtlasBaseException {
        try {
            AtlasVertex connectionVertex = entityRetriever.getEntityVertex(atlasObjectId);
            boolean currentHasLineage = getEntityHasLineage(connectionVertex);

            if (currentHasLineage != hasLineage) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Updating hasLineage for connection {} from {} to {}", atlasObjectId, currentHasLineage, hasLineage);
                }
                connectionVertex.setProperty(HAS_LINEAGE, hasLineage);
            }
        } catch (AtlasBaseException e) {
            if (!e.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                throw e;
            }
        }
    }

    private boolean hasActiveConnectionProcesses(AtlasVertex connectionVertex) {
        Iterator<AtlasEdge> edges = connectionVertex.getEdges(AtlasEdgeDirection.BOTH, new String[]{"__ConnectionProcess.inputs", "__ConnectionProcess.outputs"}).iterator();

        while (edges.hasNext()) {
            AtlasEdge edge = edges.next();
            if (getStatus(edge) == ACTIVE) {
                AtlasVertex processVertex = edge.getOutVertex().equals(connectionVertex) ? edge.getInVertex() : edge.getOutVertex();
                if (getStatus(processVertex) == ACTIVE && getTypeName(processVertex).equals(CONNECTION_PROCESS_ENTITY_TYPE)) {
                    return true;
                }
            }
        }
        return false;
    }
}