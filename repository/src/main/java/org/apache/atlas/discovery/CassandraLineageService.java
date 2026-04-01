package org.apache.atlas.discovery;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageListInfo;
import org.apache.atlas.model.lineage.AtlasLineageOnDemandInfo;
import org.apache.atlas.model.lineage.AtlasLineageOnDemandInfo.LineageDirection;
import org.apache.atlas.model.lineage.AtlasLineageOnDemandInfo.LineageInfoOnDemand;
import org.apache.atlas.model.lineage.AtlasLineageOnDemandInfo.LineageRelation;
import org.apache.atlas.model.lineage.LineageChildrenInfo;
import org.apache.atlas.model.lineage.LineageListRequest;
import org.apache.atlas.model.lineage.LineageOnDemandConstraints;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Direct Cassandra implementation of lineage on-demand traversal.
 *
 * Bypasses the graph abstraction layer (JanusGraph / CassandraGraph) and queries
 * Cassandra edge and vertex tables directly.  Returns the same
 * {@link AtlasLineageOnDemandInfo} response as {@link EntityLineageService}.
 *
 * <h3>Cassandra tables used</h3>
 * <ul>
 *   <li>{@code edges_in}  — incoming adjacency list (partition: in_vertex_id, clustering: edge_label)</li>
 *   <li>{@code edges_out} — outgoing adjacency list (partition: out_vertex_id, clustering: edge_label)</li>
 *   <li>{@code vertices}  — vertex properties (partition: vertex_id)</li>
 *   <li>{@code vertex_index} — 1:1 unique index for GUID → vertex_id lookup</li>
 * </ul>
 *
 * <h3>Edge labels for lineage</h3>
 * <ul>
 *   <li>{@code __Process.inputs}  — Process → input DataSet</li>
 *   <li>{@code __Process.outputs} — Process → output DataSet</li>
 * </ul>
 */
public class CassandraLineageService {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraLineageService.class);

    private static final String PROCESS_INPUTS_EDGE  = "__Process.inputs";
    private static final String PROCESS_OUTPUTS_EDGE = "__Process.outputs";
    private static final String PROCESS_SUPER_TYPE   = "Process";
    private static final String DATA_SET_SUPER_TYPE  = "Catalog";  // Matches Constants.DATA_SET_SUPER_TYPE — Catalog replaced deprecated DataSet
    private static final String CONNECTION_ENTITY_TYPE         = "Connection";
    private static final String CONNECTION_PROCESS_ENTITY_TYPE = "ConnectionProcess";
    private static final String DATA_PRODUCT_ENTITY_TYPE       = "DataProduct";
    private static final String SEPARATOR            = "->";
    private static final int    DEFAULT_DEPTH        = 3;

    private final CqlSession        session;
    private final AtlasTypeRegistry typeRegistry;

    // ---- Prepared statements ------------------------------------------------
    private final PreparedStatement findVertexIdByGuidStmt;
    private final PreparedStatement getVertexStmt;
    private final PreparedStatement getEdgesInByLabelStmt;
    private final PreparedStatement getEdgesOutByLabelStmt;

    // ---- Per-request vertex cache (avoids re-reads during recursion) --------
    private final ThreadLocal<Map<String, VertexData>> vertexCache =
            ThreadLocal.withInitial(HashMap::new);

    // ========================================================================
    // Data carriers (package-private for testability)
    // ========================================================================

    /** Lightweight representation of a Cassandra vertex row. */
    static class VertexData {
        final String              vertexId;
        final String              guid;
        final String              typeName;
        final String              state;
        final Map<String, Object> properties;   // full JSON-parsed properties

        VertexData(String vertexId, String guid, String typeName, String state,
                   Map<String, Object> properties) {
            this.vertexId   = vertexId;
            this.guid       = guid;
            this.typeName   = typeName;
            this.state      = state;
            this.properties = properties;
        }
    }

    /** Lightweight representation of a Cassandra edge row. */
    static class EdgeData {
        final String              edgeId;
        final String              outVertexId;
        final String              inVertexId;
        final String              label;
        final String              state;
        final Map<String, Object> properties;   // full JSON-parsed edge properties

        EdgeData(String edgeId, String outVertexId, String inVertexId,
                 String label, String state, Map<String, Object> properties) {
            this.edgeId      = edgeId;
            this.outVertexId = outVertexId;
            this.inVertexId  = inVertexId;
            this.label       = label;
            this.state       = state;
            this.properties  = properties;
        }

        /** Relationship GUID stored in the edge properties (key: {@code r:__guid}). */
        String getRelationshipGuid() {
            Object v = properties.get("r:__guid");
            return v != null ? v.toString() : edgeId;   // fall back to edge id
        }

        boolean isInputEdge(String inputLabel) {
            return inputLabel.equalsIgnoreCase(label);
        }
    }

    // ========================================================================
    // Constructor
    // ========================================================================

    public CassandraLineageService(CqlSession session, AtlasTypeRegistry typeRegistry) {
        this.session      = session;
        this.typeRegistry  = typeRegistry;

        findVertexIdByGuidStmt = session.prepare(
                "SELECT vertex_id FROM vertex_index " +
                "WHERE index_name = '__guid_idx' AND index_value = ?");

        getVertexStmt = session.prepare(
                "SELECT vertex_id, properties, type_name, state FROM vertices " +
                "WHERE vertex_id = ?");

        getEdgesInByLabelStmt = session.prepare(
                "SELECT edge_id, out_vertex_id, edge_label, properties, state " +
                "FROM edges_in WHERE in_vertex_id = ? AND edge_label = ?");

        getEdgesOutByLabelStmt = session.prepare(
                "SELECT edge_id, in_vertex_id, edge_label, properties, state " +
                "FROM edges_out WHERE out_vertex_id = ? AND edge_label = ?");
    }

    // ========================================================================
    // Public API — same contract as EntityLineageService.getLineageInfoOnDemand
    // ========================================================================

    /**
     * Compute lineage on-demand for the entity identified by {@code guid}.
     *
     * @param guid    the base entity GUID
     * @param context constraints, predicates, attributes from the REST request
     * @return lineage result identical in shape to the existing API
     */
    public AtlasLineageOnDemandInfo getLineageInfoOnDemand(
            String guid, AtlasLineageOnDemandContext context) throws AtlasBaseException {
        try {
            vertexCache.get().clear();

            // 1. Resolve GUID → vertex_id
            String vertexId = findVertexIdByGuid(guid);
            if (vertexId == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            // 2. Load vertex to determine entity type
            VertexData baseVertex = getVertexData(vertexId);
            if (baseVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            String lineageType = context.getLineageType();
            String inputLabel  = getLineageInputLabel();
            String outputLabel = getLineageOutputLabel();
            boolean isConnector = isConnectorVertex(baseVertex.typeName, lineageType);

            // 3. Resolve per-GUID constraints (direction, depth, limits)
            LineageOnDemandConstraints constraints = getConstraints(guid, context);
            LineageDirection direction = constraints.getDirection() != null
                    ? constraints.getDirection() : LineageDirection.BOTH;
            int depth = constraints.getDepth();
            if (depth == 0) depth = DEFAULT_DEPTH;

            // 4. Initialise the result object
            AtlasLineageOnDemandInfo ret = new AtlasLineageOnDemandInfo(
                    guid, new HashMap<>(), new HashSet<>(),
                    new HashSet<>(), new HashSet<>(), new HashMap<>());
            ret.getRelationsOnDemand().put(guid, new LineageInfoOnDemand(constraints));

            AtomicInteger inputTraversed   = new AtomicInteger(0);
            AtomicInteger outputTraversed  = new AtomicInteger(0);
            AtomicInteger traversalOrder   = new AtomicInteger(1);
            Set<String>   attributes       = context.getAttributes();

            // 5. Traverse
            if (!isConnector) {
                // Non-connector: Dataset, Connection, DataProduct — traverse via IN edges
                if (direction == LineageDirection.INPUT || direction == LineageDirection.BOTH)
                    traverseFromDataset(vertexId, true, depth, 0, new HashSet<>(),
                            context, ret, guid, inputTraversed, traversalOrder, attributes);
                if (direction == LineageDirection.OUTPUT || direction == LineageDirection.BOTH)
                    traverseFromDataset(vertexId, false, depth, 0, new HashSet<>(),
                            context, ret, guid, outputTraversed, traversalOrder, attributes);

                // Add the base entity to the result
                AtlasEntityHeader header = buildEntityHeader(baseVertex, attributes);
                header.setDepth(0);
                header.setTraversalOrder(0);
                header.setFinishTime(traversalOrder.get());
                ret.getGuidEntityMap().put(guid, header);
            } else {
                // Connector: Process, ConnectionProcess — make one hop via OUT edges first
                if (direction == LineageDirection.INPUT || direction == LineageDirection.BOTH) {
                    List<EdgeData> edges = getActiveEdgesOut(vertexId, inputLabel);
                    traverseFromProcess(edges, true, depth, 0, context, ret,
                            vertexId, guid, inputTraversed, traversalOrder, attributes);
                }
                if (direction == LineageDirection.OUTPUT || direction == LineageDirection.BOTH) {
                    List<EdgeData> edges = getActiveEdgesOut(vertexId, outputLabel);
                    traverseFromProcess(edges, false, depth, 0, context, ret,
                            vertexId, guid, outputTraversed, traversalOrder, attributes);
                }
            }

            cleanupRelationsOnDemand(ret);
            return ret;
        } finally {
            vertexCache.get().clear();
        }
    }

    // ========================================================================
    // Classic lineage (GET /lineage/{guid}, POST /lineage/getlineage)
    // Returns AtlasLineageInfo — same contract as EntityLineageService.getLineageInfoV2
    // ========================================================================

    public AtlasLineageInfo getClassicLineageInfo(AtlasLineageContext context) throws AtlasBaseException {
        try {
            vertexCache.get().clear();

            String guid = context.getGuid();
            AtlasLineageInfo.LineageDirection direction = context.getDirection();
            int depth = context.getDepth();
            if (depth == 0) depth = -1;

            String vertexId = findVertexIdByGuid(guid);
            if (vertexId == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }
            VertexData baseVertex = getVertexData(vertexId);
            if (baseVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            boolean isProcess = isProcessType(baseVertex.typeName);
            String inputLabel  = getLineageInputLabel();
            String outputLabel = getLineageOutputLabel();
            Set<String> attributes = context.getAttributes();

            AtlasLineageInfo ret = new AtlasLineageInfo(guid, new HashMap<>(), new HashSet<>(),
                    direction, context.getDepth(), context.getLimit(), context.getOffset());

            // Add base entity
            ret.getGuidEntityMap().put(guid, buildEntityHeader(baseVertex, attributes));

            if (!isProcess) {
                // Dataset path
                if (direction == AtlasLineageInfo.LineageDirection.INPUT || direction == AtlasLineageInfo.LineageDirection.BOTH)
                    classicTraverse(vertexId, true, depth, new HashSet<>(), ret, context);
                if (direction == AtlasLineageInfo.LineageDirection.OUTPUT || direction == AtlasLineageInfo.LineageDirection.BOTH)
                    classicTraverse(vertexId, false, depth, new HashSet<>(), ret, context);
            } else {
                // Process path — one hop to connected datasets, then recurse
                if (direction == AtlasLineageInfo.LineageDirection.INPUT || direction == AtlasLineageInfo.LineageDirection.BOTH) {
                    List<EdgeData> edges = getActiveEdgesOut(vertexId, inputLabel);
                    edges = filterIgnoredProcesses(edges, context.getIgnoredProcesses(), true);
                    ret.setHasChildrenForDirection(guid,
                            new LineageChildrenInfo(AtlasLineageInfo.LineageDirection.INPUT, hasActiveEdge(edges)));
                    for (EdgeData edge : edges) {
                        addClassicEdge(edge, ret, attributes);
                        classicTraverse(edge.inVertexId, true, depth - 1, new HashSet<>(), ret, context);
                    }
                }
                if (direction == AtlasLineageInfo.LineageDirection.OUTPUT || direction == AtlasLineageInfo.LineageDirection.BOTH) {
                    List<EdgeData> edges = getActiveEdgesOut(vertexId, outputLabel);
                    edges = filterIgnoredProcesses(edges, context.getIgnoredProcesses(), true);
                    ret.setHasChildrenForDirection(guid,
                            new LineageChildrenInfo(AtlasLineageInfo.LineageDirection.OUTPUT, hasActiveEdge(edges)));
                    for (EdgeData edge : edges) {
                        addClassicEdge(edge, ret, attributes);
                        classicTraverse(edge.inVertexId, false, depth - 1, new HashSet<>(), ret, context);
                    }
                }
            }

            return ret;
        } finally {
            vertexCache.get().clear();
        }
    }

    /** Recursive DFS for classic lineage (depth-based, no per-entity limits). */
    private void classicTraverse(
            String datasetVertexId, boolean isInput, int depth,
            Set<String> visitedVertices, AtlasLineageInfo ret,
            AtlasLineageContext context) throws AtlasBaseException {

        String inputLabel  = getLineageInputLabel();
        String outputLabel = getLineageOutputLabel();

        if (depth == 0) {
            // At depth limit — just check for children
            VertexData vertex = getVertexData(datasetVertexId);
            if (vertex == null) return;
            String inLabel = isInput ? outputLabel : inputLabel;
            List<EdgeData> edges = getActiveEdgesIn(datasetVertexId, inLabel);
            edges = filterIgnoredProcesses(edges, context.getIgnoredProcesses(), false);
            AtlasLineageInfo.LineageDirection dir = isInput
                    ? AtlasLineageInfo.LineageDirection.INPUT
                    : AtlasLineageInfo.LineageDirection.OUTPUT;
            ret.setHasChildrenForDirection(vertex.guid, new LineageChildrenInfo(dir, hasActiveEdge(edges)));
            return;
        }

        visitedVertices.add(datasetVertexId);
        Set<String> attributes = context.getAttributes();

        // Step 1: dataset ← process (incoming edges)
        String inLabel = isInput ? outputLabel : inputLabel;
        List<EdgeData> processEdges = getActiveEdgesIn(datasetVertexId, inLabel);
        processEdges = filterIgnoredProcesses(processEdges, context.getIgnoredProcesses(), false);

        for (EdgeData inEdge : processEdges) {
            String processVertexId = inEdge.outVertexId;
            VertexData processVertex = getVertexData(processVertexId);
            if (processVertex == null) continue;

            // Skip deleted processes unless allowed
            if (!context.isAllowDeletedProcess() && "DELETED".equals(processVertex.state)) continue;

            if (context.isHideProcess()) {
                // hideProcess: skip process node, make virtual dataset→dataset relations
                String outLabel = isInput ? inputLabel : outputLabel;
                List<EdgeData> nextEdges = getActiveEdgesOut(processVertexId, outLabel);

                for (EdgeData outEdge : nextEdges) {
                    String nextVertexId = outEdge.inVertexId;
                    VertexData nextVertex = getVertexData(nextVertexId);
                    if (nextVertex == null) continue;

                    // Add both edges to result (process is still in guidEntityMap)
                    addClassicEdge(inEdge, ret, attributes);
                    addClassicEdge(outEdge, ret, attributes);

                    if (!visitedVertices.contains(nextVertexId)) {
                        classicTraverse(nextVertexId, isInput, depth - 1, visitedVertices, ret, context);
                    }
                }
            } else {
                // Normal mode: add process + edges
                addClassicEdge(inEdge, ret, attributes);

                // Step 2: process → next datasets (outgoing edges)
                String outLabel = isInput ? inputLabel : outputLabel;
                List<EdgeData> nextEdges = getActiveEdgesOut(processVertexId, outLabel);

                for (EdgeData outEdge : nextEdges) {
                    addClassicEdge(outEdge, ret, attributes);
                    String nextVertexId = outEdge.inVertexId;
                    if (!visitedVertices.contains(nextVertexId)) {
                        classicTraverse(nextVertexId, isInput, depth - 1, visitedVertices, ret, context);
                    }
                }
            }
        }
    }

    /** Add an edge to classic lineage result (dedup by relationship GUID). */
    private void addClassicEdge(EdgeData edge, AtlasLineageInfo ret, Set<String> attributes) {
        VertexData inVertex  = getVertexData(edge.inVertexId);
        VertexData outVertex = getVertexData(edge.outVertexId);
        if (inVertex == null || outVertex == null) return;

        String relationGuid = edge.getRelationshipGuid();

        // Duplicate check by relationship GUID
        for (AtlasLineageInfo.LineageRelation r : ret.getRelations()) {
            if (relationGuid.equals(r.getRelationshipId())) return;
        }

        // Add vertices
        if (!ret.getGuidEntityMap().containsKey(inVertex.guid))
            ret.getGuidEntityMap().put(inVertex.guid, buildEntityHeader(inVertex, attributes));
        if (!ret.getGuidEntityMap().containsKey(outVertex.guid))
            ret.getGuidEntityMap().put(outVertex.guid, buildEntityHeader(outVertex, attributes));

        // Add relation (same direction semantics as on-demand)
        if (edge.isInputEdge(getLineageInputLabel())) {
            ret.getRelations().add(new AtlasLineageInfo.LineageRelation(inVertex.guid, outVertex.guid, relationGuid));
        } else {
            ret.getRelations().add(new AtlasLineageInfo.LineageRelation(outVertex.guid, inVertex.guid, relationGuid));
        }
    }

    /** Filter edges where the process vertex type is in the ignored set. */
    private List<EdgeData> filterIgnoredProcesses(List<EdgeData> edges, Set<String> ignoredProcesses, boolean processIsOut) {
        if (ignoredProcesses == null || ignoredProcesses.isEmpty()) return edges;
        List<EdgeData> filtered = new ArrayList<>();
        for (EdgeData e : edges) {
            // For in-edges: process is outVertexId. For out-edges from process: check inVertexId's connected process
            String processVid = processIsOut ? e.outVertexId : e.inVertexId;
            VertexData pv = getVertexData(processVid);
            if (pv != null && ignoredProcesses.contains(pv.typeName)) continue;
            filtered.add(e);
        }
        return filtered;
    }

    private boolean hasActiveEdge(List<EdgeData> edges) {
        return edges.stream().anyMatch(e -> !"DELETED".equals(e.state));
    }

    // ========================================================================
    // BFS list lineage (POST /lineage/list)
    // Returns AtlasLineageListInfo — same contract as EntityLineageService.traverseEdgesUsingBFS
    // ========================================================================

    public AtlasLineageListInfo getLineageListInfo(
            String guid, AtlasLineageListContext context) throws AtlasBaseException {
        try {
            vertexCache.get().clear();

            AtlasLineageListInfo ret = new AtlasLineageListInfo(new ArrayList<>());

            String vertexId = findVertexIdByGuid(guid);
            if (vertexId == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }
            VertexData baseVertex = getVertexData(vertexId);
            if (baseVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            String lineageType = context.getLineageType();
            boolean isBaseNotConnector = !isConnectorVertex(baseVertex.typeName, lineageType);
            boolean isInputDir = context.getDirection() == LineageListRequest.LineageDirection.INPUT;

            Set<String> visitedVertices = new HashSet<>();
            visitedVertices.add(guid);
            Set<String> skippedVertices = new HashSet<>();
            Queue<String> traversalQueue = new LinkedList<>();

            // immediateNeighbours tracking maps
            Map<String, List<String>> lineageParentsForEntityMap  = new HashMap<>();
            Map<String, List<String>> lineageChildrenForEntityMap = new HashMap<>();

            // Seed the queue with immediate neighbours
            bfsEnqueueNeighbours(baseVertex, isBaseNotConnector, isInputDir,
                    traversalQueue, visitedVertices, lineageParentsForEntityMap, lineageChildrenForEntityMap);

            int currentDepth = 0;
            int maxDepth = context.getDepth();
            int currentLevel = isBaseNotConnector ? 0 : 1;
            int entityCount = 0;
            int fromCounter = 0;

            if (Boolean.TRUE.equals(context.getImmediateNeighbours())) {
                // Add the base vertex to the result
                AtlasEntityHeader baseHeader = buildEntityHeader(baseVertex, context.getAttributes());
                baseHeader.setDepth(currentLevel);
                ret.getEntities().add(baseHeader);
            }

            while (!traversalQueue.isEmpty() && entityCount < context.getSize() && currentDepth < maxDepth) {
                currentDepth++;

                // Level increments every other depth
                if ((isBaseNotConnector && currentDepth % 2 != 0) ||
                    (!isBaseNotConnector && currentDepth % 2 == 0)) {
                    currentLevel++;
                }

                int entitiesInDepth = traversalQueue.size();
                for (int i = 0; i < entitiesInDepth; i++) {
                    if (entityCount >= context.getSize()) break;

                    String currentGUID = traversalQueue.poll();
                    if (currentGUID == null) break;

                    String curVertexId = findVertexIdByGuid(currentGUID);
                    if (curVertexId == null) continue;
                    VertexData currentVertex = getVertexData(curVertexId);
                    if (currentVertex == null) continue;

                    boolean isCurrentNotConnector = !isConnectorVertex(currentVertex.typeName, lineageType);

                    // Handle offset (skip first 'from' entities)
                    if (context.getFrom() > 0 && fromCounter < context.getFrom()) {
                        fromCounter++;
                        skippedVertices.add(currentGUID);
                        bfsEnqueueNeighbours(currentVertex, isCurrentNotConnector, isInputDir,
                                traversalQueue, visitedVertices, lineageParentsForEntityMap, lineageChildrenForEntityMap);
                        continue;
                    }

                    entityCount++;

                    // Enqueue neighbours for further traversal
                    bfsEnqueueNeighbours(currentVertex, isCurrentNotConnector, isInputDir,
                            traversalQueue, visitedVertices, lineageParentsForEntityMap, lineageChildrenForEntityMap);

                    // Add to result
                    AtlasEntityHeader header = buildEntityHeader(currentVertex, context.getAttributes());
                    header.setDepth(currentLevel);
                    ret.getEntities().add(header);

                    // Last entity at last depth
                    if (currentDepth == maxDepth && i == entitiesInDepth - 1) {
                        ret.setHasMore(false);
                        context.setHasMoreUpdated(true);
                    }
                }
            }

            // Update immediate neighbour info for each entity in result
            if (Boolean.TRUE.equals(context.getImmediateNeighbours())) {
                updateNeighbourNodesForEachEntity(context, ret, lineageParentsForEntityMap, lineageChildrenForEntityMap);
            }

            ret.setEntityCount(entityCount);

            // hasMore: more in queue or hit entity limit
            if (!context.isHasMoreUpdated()) {
                ret.setHasMore(!traversalQueue.isEmpty() || entityCount >= context.getSize());
            }

            return ret;
        } finally {
            vertexCache.get().clear();
        }
    }

    /** Enqueue BFS neighbours for a vertex (without tracking parent/children maps). */
    private void bfsEnqueueNeighbours(
            VertexData vertex, boolean isNotConnector, boolean isInputDir,
            Queue<String> queue, Set<String> visited) {
        bfsEnqueueNeighbours(vertex, isNotConnector, isInputDir, queue, visited, null, null);
    }

    /** Enqueue BFS neighbours for a vertex, optionally tracking parent/children maps for immediateNeighbours. */
    private void bfsEnqueueNeighbours(
            VertexData vertex, boolean isNotConnector, boolean isInputDir,
            Queue<String> queue, Set<String> visited,
            Map<String, List<String>> lineageParentsMap,
            Map<String, List<String>> lineageChildrenMap) {

        String inputLabel  = getLineageInputLabel();
        String outputLabel = getLineageOutputLabel();

        if (isNotConnector) {
            // Non-connector (Dataset, Connection, DataProduct) → follow incoming edges
            String label = isInputDir ? outputLabel : inputLabel;
            List<EdgeData> edges = getActiveEdgesIn(vertex.vertexId, label);
            for (EdgeData e : edges) {
                VertexData neighbour = getVertexData(e.outVertexId);
                if (neighbour != null) {
                    if (!visited.contains(neighbour.guid)) {
                        visited.add(neighbour.guid);
                        queue.add(neighbour.guid);
                    }
                    if (lineageParentsMap != null) {
                        lineageParentsMap
                                .computeIfAbsent(neighbour.guid, k -> new ArrayList<>())
                                .add(vertex.guid);
                        lineageChildrenMap
                                .computeIfAbsent(vertex.guid, k -> new ArrayList<>())
                                .add(neighbour.guid);
                    }
                }
            }
        } else {
            // Connector (Process, ConnectionProcess) → follow outgoing edges
            String label = isInputDir ? inputLabel : outputLabel;
            List<EdgeData> edges = getActiveEdgesOut(vertex.vertexId, label);
            for (EdgeData e : edges) {
                VertexData neighbour = getVertexData(e.inVertexId);
                if (neighbour != null) {
                    if (!visited.contains(neighbour.guid)) {
                        visited.add(neighbour.guid);
                        queue.add(neighbour.guid);
                    }
                    if (lineageParentsMap != null) {
                        lineageParentsMap
                                .computeIfAbsent(neighbour.guid, k -> new ArrayList<>())
                                .add(vertex.guid);
                        lineageChildrenMap
                                .computeIfAbsent(vertex.guid, k -> new ArrayList<>())
                                .add(neighbour.guid);
                    }
                }
            }
        }
    }

    /**
     * Populate immediateUpstream/immediateDownstream on each entity header in the result.
     * Mirrors EntityLineageService.updateNeighbourNodesForEachEntity().
     */
    private void updateNeighbourNodesForEachEntity(
            AtlasLineageListContext context,
            AtlasLineageListInfo ret,
            Map<String, List<String>> lineageParentsMap,
            Map<String, List<String>> lineageChildrenMap) {

        List<AtlasEntityHeader> entityList = ret.getEntities();
        if (entityList == null) return;

        boolean isInputDir = context.getDirection() == LineageListRequest.LineageDirection.INPUT;

        for (AtlasEntityHeader entity : entityList) {
            if (entity == null || entity.getGuid() == null) continue;
            updateLineageForEntity(entity, lineageParentsMap, true, isInputDir);
            updateLineageForEntity(entity, lineageChildrenMap, false, isInputDir);
        }
    }

    private void updateLineageForEntity(
            AtlasEntityHeader entity,
            Map<String, List<String>> lineageMap,
            boolean isParentMap,
            boolean isInputDir) {

        List<String> relatedProcessNodes = lineageMap.get(entity.getGuid());
        if (relatedProcessNodes == null) return;

        Set<String> seenGuids = new HashSet<>();
        List<Map<String, String>> relatedDatasetNodes = new ArrayList<>();

        for (String nodeGuid : relatedProcessNodes) {
            List<String> subNodes = lineageMap.get(nodeGuid);
            if (subNodes == null) continue;

            for (String subNodeGuid : subNodes) {
                String subVertexId = findVertexIdByGuid(subNodeGuid);
                if (subVertexId == null) continue;
                VertexData subVertex = getVertexData(subVertexId);
                if (subVertex != null && seenGuids.add(subNodeGuid)) {
                    Map<String, String> details = new HashMap<>();
                    details.put("guid", subVertex.guid);
                    Object qn = subVertex.properties.get("qualifiedName");
                    if (qn != null) details.put("qualifiedName", String.valueOf(qn));
                    Object name = subVertex.properties.get("name");
                    if (name != null) details.put("name", String.valueOf(name));
                    relatedDatasetNodes.add(details);
                }
            }
        }

        if (isParentMap) {
            if (isInputDir) {
                entity.setImmediateDownstream(relatedDatasetNodes);
            } else {
                entity.setImmediateUpstream(relatedDatasetNodes);
            }
        } else {
            if (isInputDir) {
                entity.setImmediateUpstream(relatedDatasetNodes);
            } else {
                entity.setImmediateDownstream(relatedDatasetNodes);
            }
        }
    }

    // ========================================================================
    // Traversal: starting from a Dataset vertex (on-demand)
    // ========================================================================

    /**
     * Recursive DFS from a dataset vertex.
     *
     * <pre>
     * Downstream (isInput=false):
     *   dataset ←(IN, __Process.inputs)— process →(OUT, __Process.outputs)→ next_dataset
     *
     * Upstream (isInput=true):
     *   dataset ←(IN, __Process.outputs)— process →(OUT, __Process.inputs)→ prev_dataset
     * </pre>
     */
    private void traverseFromDataset(
            String         datasetVertexId,
            boolean        isInput,
            int            depth,
            int            level,
            Set<String>    visitedVertices,
            AtlasLineageOnDemandContext context,
            AtlasLineageOnDemandInfo    ret,
            String         baseGuid,
            AtomicInteger  entitiesTraversed,
            AtomicInteger  traversalOrder,
            Set<String>    attributes) throws AtlasBaseException {

        if (entitiesTraversed.get() >= getMaxNodeCount()) return;
        if (depth == 0) return;

        visitedVertices.add(datasetVertexId);
        int nextLevel = isInput ? level - 1 : level + 1;
        LineageDirection direction = isInput ? LineageDirection.INPUT : LineageDirection.OUTPUT;

        // Step 1: find connected process vertices via incoming edges
        //   Upstream  → edges with label outputLabel where this dataset is IN-vertex
        //   Downstream→ edges with label inputLabel  where this dataset is IN-vertex
        String inputLabel    = getLineageInputLabel();
        String outputLabel   = getLineageOutputLabel();
        String incomingLabel = isInput ? outputLabel : inputLabel;
        List<EdgeData> incomingEdges = getActiveEdgesIn(datasetVertexId, incomingLabel);

        for (EdgeData incomingEdge : incomingEdges) {
            String processVertexId = incomingEdge.outVertexId;

            // --- offset handling ---
            VertexData datasetVertex = getVertexData(datasetVertexId);
            if (checkForOffset(incomingEdge, datasetVertex.guid, context, ret)) continue;

            // --- limit handling ---
            VertexData processVertex = getVertexData(processVertexId);
            if (processVertex == null) continue;

            if (incrementAndCheckIfRelationsLimitReached(
                    incomingEdge, !isInput, context, ret, depth,
                    entitiesTraversed, direction, visitedVertices, attributes)) {
                LineageInfoOnDemand entityOnDemandInfo = ret.getRelationsOnDemand().get(baseGuid);
                if (entityOnDemandInfo == null) continue;
                if (isInput ? entityOnDemandInfo.isInputRelationsReachedLimit()
                            : entityOnDemandInfo.isOutputRelationsReachedLimit()) break;
                else continue;
            } else {
                addEdgeToResult(incomingEdge, ret, attributes, level, traversalOrder);
            }

            // Step 2: from the process, find connected datasets via outgoing edges
            String outgoingLabel = isInput ? inputLabel : outputLabel;
            List<EdgeData> outgoingEdges = getActiveEdgesOut(processVertexId, outgoingLabel);

            for (EdgeData outgoingEdge : outgoingEdges) {
                String entityVertexId = outgoingEdge.inVertexId;

                if (checkForOffset(outgoingEdge, processVertex.guid, context, ret)) continue;

                if (incrementAndCheckIfRelationsLimitReached(
                        outgoingEdge, isInput, context, ret, depth,
                        entitiesTraversed, direction, visitedVertices, attributes)) {
                    String processGuid = processVertex.guid;
                    LineageInfoOnDemand processInfo = ret.getRelationsOnDemand().get(processGuid);
                    if (processInfo == null) continue;
                    if (isInput ? processInfo.isInputRelationsReachedLimit()
                                : processInfo.isOutputRelationsReachedLimit()) break;
                    else continue;
                } else {
                    addEdgeToResult(outgoingEdge, ret, attributes, nextLevel, traversalOrder);
                    entitiesTraversed.incrementAndGet();
                    traversalOrder.incrementAndGet();
                    if (entitiesTraversed.get() >= getMaxNodeCount()) {
                        if (isInput) ret.setUpstreamEntityLimitReached(true);
                        else         ret.setDownstreamEntityLimitReached(true);
                    }
                }

                // Step 3: recurse into the next dataset
                if (!visitedVertices.contains(entityVertexId)) {
                    traverseFromDataset(entityVertexId, isInput, depth - 1, nextLevel,
                            visitedVertices, context, ret, baseGuid,
                            entitiesTraversed, traversalOrder, attributes);

                    VertexData entityVertex = getVertexData(entityVertexId);
                    if (entityVertex != null) {
                        AtlasEntityHeader traversedEntity =
                                ret.getGuidEntityMap().get(entityVertex.guid);
                        if (traversedEntity != null)
                            traversedEntity.setFinishTime(traversalOrder.get());
                    }
                }
            }
        }
    }

    // ========================================================================
    // Traversal: starting from a Process vertex (one hop then delegates)
    // ========================================================================

    /**
     * Called when the base entity is a Process. Makes one hop to connected
     * datasets and then delegates to {@link #traverseFromDataset}.
     */
    private void traverseFromProcess(
            List<EdgeData> processEdges,
            boolean        isInput,
            int            depth,
            int            level,
            AtlasLineageOnDemandContext context,
            AtlasLineageOnDemandInfo    ret,
            String         processVertexId,
            String         baseGuid,
            AtomicInteger  entitiesTraversed,
            AtomicInteger  traversalOrder,
            Set<String>    attributes) throws AtlasBaseException {

        LineageDirection direction = isInput ? LineageDirection.INPUT : LineageDirection.OUTPUT;
        int nextLevel = isInput ? level - 1 : level + 1;

        for (EdgeData processEdge : processEdges) {
            String datasetVertexId = processEdge.inVertexId;
            VertexData datasetVertex = getVertexData(datasetVertexId);
            if (datasetVertex == null) continue;

            VertexData processVertex = getVertexData(processVertexId);
            if (checkForOffset(processEdge, processVertex != null ? processVertex.guid : baseGuid, context, ret))
                continue;

            boolean isInputEdge = processEdge.isInputEdge(getLineageInputLabel());
            if (incrementAndCheckIfRelationsLimitReached(
                    processEdge, isInputEdge, context, ret, depth,
                    entitiesTraversed, direction, new HashSet<>(), attributes)) {
                break;
            } else {
                addEdgeToResult(processEdge, ret, attributes, nextLevel, traversalOrder);
                traversalOrder.incrementAndGet();
            }

            String inGuid = datasetVertex.guid;
            LineageOnDemandConstraints inConstraints = getConstraints(inGuid, context);
            if (!ret.getRelationsOnDemand().containsKey(inGuid)) {
                ret.getRelationsOnDemand().put(inGuid, new LineageInfoOnDemand(inConstraints));
            }

            traverseFromDataset(datasetVertexId, isInput, depth - 1, nextLevel,
                    new HashSet<>(), context, ret, baseGuid,
                    entitiesTraversed, traversalOrder, attributes);
        }
    }

    // ========================================================================
    // Edge → Result mapping (mirrors EntityLineageService.processEdge)
    // ========================================================================

    /**
     * Add an edge and its endpoint vertices to the lineage result.
     * Maintains the same relation direction semantics as EntityLineageService:
     * <ul>
     *   <li>{@code __Process.inputs}  → relation: (inGuid → outGuid) i.e. dataset → process</li>
     *   <li>{@code __Process.outputs} → relation: (outGuid → inGuid) i.e. process → dataset</li>
     * </ul>
     */
    private void addEdgeToResult(
            EdgeData                    edge,
            AtlasLineageOnDemandInfo    ret,
            Set<String>                 attributes,
            int                         level,
            AtomicInteger               traversalOrder) throws AtlasBaseException {

        // Duplicate check via visitedEdges
        String visitedKey = edgeVisitedKey(edge);
        if (ret.getVisitedEdges().contains(visitedKey)) return;

        VertexData inVertex  = getVertexData(edge.inVertexId);
        VertexData outVertex = getVertexData(edge.outVertexId);
        if (inVertex == null || outVertex == null) return;

        String  inGuid       = inVertex.guid;
        String  outGuid      = outVertex.guid;
        String  relationGuid = edge.getRelationshipGuid();
        boolean isInputEdge  = edge.isInputEdge(getLineageInputLabel());

        // Determine which vertex is the Process (for depth/order assignment)
        boolean inIsProcess = isProcessType(inVertex.typeName);

        // Add IN-vertex to guidEntityMap (if not already present)
        if (!ret.getGuidEntityMap().containsKey(inGuid)) {
            AtlasEntityHeader header = buildEntityHeader(inVertex, attributes);
            if (!inIsProcess) {
                header.setDepth(level);
                header.setTraversalOrder(traversalOrder.get());
            }
            ret.getGuidEntityMap().put(inGuid, header);
        }

        // Add OUT-vertex to guidEntityMap (if not already present)
        if (!ret.getGuidEntityMap().containsKey(outGuid)) {
            AtlasEntityHeader header = buildEntityHeader(outVertex, attributes);
            if (inIsProcess) {
                header.setDepth(level);
                header.setTraversalOrder(traversalOrder.get());
            }
            ret.getGuidEntityMap().put(outGuid, header);
        }

        // Add relation (direction matches EntityLineageService convention)
        if (isInputEdge) {
            ret.getRelations().add(new LineageRelation(inGuid, outGuid, relationGuid));
        } else {
            ret.getRelations().add(new LineageRelation(outGuid, inGuid, relationGuid));
        }

        ret.getVisitedEdges().add(visitedKey);
    }

    // ========================================================================
    // Limit and offset helpers
    // ========================================================================

    /**
     * Increment relation counters on both endpoints. Returns {@code true} if
     * the limit has been reached (caller should skip this edge).
     */
    private boolean incrementAndCheckIfRelationsLimitReached(
            EdgeData    edge,
            boolean     isInput,
            AtlasLineageOnDemandContext context,
            AtlasLineageOnDemandInfo    ret,
            int         depth,
            AtomicInteger entitiesTraversed,
            LineageDirection direction,
            Set<String> visitedVertices,
            Set<String> attributes) throws AtlasBaseException {

        // Determine "in" and "out" entities from the lineage perspective
        VertexData edgeInVertex  = getVertexData(isInput ? edge.outVertexId : edge.inVertexId);
        VertexData edgeOutVertex = getVertexData(isInput ? edge.inVertexId  : edge.outVertexId);
        if (edgeInVertex == null || edgeOutVertex == null) return true;

        String inGuid  = edgeInVertex.guid;
        String outGuid = edgeOutVertex.guid;

        LineageOnDemandConstraints inConstraints  = getConstraints(inGuid, context);
        LineageOnDemandConstraints outConstraints = getConstraints(outGuid, context);

        LineageInfoOnDemand inInfo  = ret.getRelationsOnDemand()
                .computeIfAbsent(inGuid, k -> new LineageInfoOnDemand(inConstraints));
        LineageInfoOnDemand outInfo = ret.getRelationsOnDemand()
                .computeIfAbsent(outGuid, k -> new LineageInfoOnDemand(outConstraints));

        // Leaf detection: when at max depth or entity limit, check if more edges exist
        boolean isOutVertexVisited = visitedVertices.contains(edgeOutVertex.vertexId);
        boolean isInVertexVisited  = visitedVertices.contains(edgeInVertex.vertexId);

        if (depth == 1 || entitiesTraversed.get() >= getMaxNodeCount() - 1) {
            if (isInput && !isOutVertexVisited) {
                setHasUpstream(edgeOutVertex, outInfo);
            } else if (!isInput && !isInVertexVisited) {
                setHasDownstream(edgeInVertex, inInfo);
            }
        }

        // Check per-entity relation limits
        boolean hasLimitReached = false;

        if (inInfo.isInputRelationsReachedLimit()
                || outInfo.isOutputRelationsReachedLimit()
                || entitiesTraversed.get() >= getMaxNodeCount()) {
            inInfo.setHasMoreInputs(true);
            outInfo.setHasMoreOutputs(true);
            hasLimitReached = true;
        }

        if (!hasLimitReached) {
            inInfo.incrementInputRelationsCount();
            outInfo.incrementOutputRelationsCount();
        }

        return hasLimitReached;
    }

    /**
     * Check if the edge should be skipped due to the {@code from} offset.
     */
    private boolean checkForOffset(
            EdgeData edge, String entityGuid,
            AtlasLineageOnDemandContext context,
            AtlasLineageOnDemandInfo ret) {

        LineageOnDemandConstraints constraints = getConstraints(entityGuid, context);
        LineageInfoOnDemand info = ret.getRelationsOnDemand()
                .computeIfAbsent(entityGuid, k -> new LineageInfoOnDemand(constraints));

        if (constraints.getFrom() != 0 && info.getFromCounter() < constraints.getFrom()) {
            String visitedKey = edgeVisitedKey(edge);
            if (!ret.getSkippedEdges().contains(visitedKey)) {
                ret.getSkippedEdges().add(visitedKey);
                info.incrementFromCounter();
            }
            return true;
        }
        return false;
    }

    /**
     * Check whether this dataset vertex has upstream edges beyond the traversal boundary.
     */
    private void setHasUpstream(VertexData vertex, LineageInfoOnDemand info) {
        long count = countEdgesIn(vertex.vertexId, getLineageOutputLabel());
        info.setHasUpstream(count > 0);
    }

    /**
     * Check whether this dataset vertex has downstream edges beyond the traversal boundary.
     */
    private void setHasDownstream(VertexData vertex, LineageInfoOnDemand info) {
        long count = countEdgesIn(vertex.vertexId, getLineageInputLabel());
        info.setHasDownstream(count > 0);
    }

    // ========================================================================
    // Cassandra data access
    // ========================================================================

    /** Resolve GUID → vertex_id via the vertex_index table. */
    private String findVertexIdByGuid(String guid) {
        Row row = session.execute(findVertexIdByGuidStmt.bind(guid)).one();
        return row != null ? row.getString("vertex_id") : null;
    }

    /** Load a vertex by vertex_id (cached within the current request). */
    @SuppressWarnings("unchecked")
    private VertexData getVertexData(String vertexId) {
        if (vertexId == null) return null;

        Map<String, VertexData> cache = vertexCache.get();
        VertexData cached = cache.get(vertexId);
        if (cached != null) return cached;

        Row row = session.execute(getVertexStmt.bind(vertexId)).one();
        if (row == null) return null;

        String propsJson = row.getString("properties");
        Map<String, Object> props = (propsJson != null && !propsJson.isEmpty())
                ? AtlasType.fromJson(propsJson, Map.class)
                : new HashMap<>();
        if (props == null) props = new HashMap<>();

        String guid     = props.containsKey("__guid")     ? String.valueOf(props.get("__guid"))     : vertexId;
        String typeName = props.containsKey("__typeName")  ? String.valueOf(props.get("__typeName")) : row.getString("type_name");
        String state    = props.containsKey("__state")     ? String.valueOf(props.get("__state"))    : row.getString("state");

        VertexData vd = new VertexData(vertexId, guid, typeName, state, props);
        cache.put(vertexId, vd);
        return vd;
    }

    /** Get ACTIVE outgoing edges for a vertex by label. */
    @SuppressWarnings("unchecked")
    private List<EdgeData> getActiveEdgesOut(String vertexId, String label) {
        List<EdgeData> result = new ArrayList<>();
        ResultSet rs = session.execute(getEdgesOutByLabelStmt.bind(vertexId, label));
        for (Row row : rs) {
            String state = row.getString("state");
            if ("DELETED".equals(state)) continue;
            Map<String, Object> props = parseEdgeProperties(row.getString("properties"));
            result.add(new EdgeData(
                    row.getString("edge_id"), vertexId, row.getString("in_vertex_id"),
                    row.getString("edge_label"), state, props));
        }
        return result;
    }

    /** Get ACTIVE incoming edges for a vertex by label. */
    @SuppressWarnings("unchecked")
    private List<EdgeData> getActiveEdgesIn(String vertexId, String label) {
        List<EdgeData> result = new ArrayList<>();
        ResultSet rs = session.execute(getEdgesInByLabelStmt.bind(vertexId, label));
        for (Row row : rs) {
            String state = row.getString("state");
            if ("DELETED".equals(state)) continue;
            Map<String, Object> props = parseEdgeProperties(row.getString("properties"));
            result.add(new EdgeData(
                    row.getString("edge_id"), row.getString("out_vertex_id"), vertexId,
                    row.getString("edge_label"), state, props));
        }
        return result;
    }

    /** Count edges in the given direction (for hasUpstream/hasDownstream detection). */
    private long countEdgesIn(String vertexId, String label) {
        // Use the same query but just count results
        // (a COUNT(*) prepared statement would be more efficient, but this is only
        //  called at leaf nodes of the traversal)
        return getActiveEdgesIn(vertexId, label).size();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseEdgeProperties(String json) {
        if (json == null || json.isEmpty()) return new HashMap<>();
        Map<String, Object> props = AtlasType.fromJson(json, Map.class);
        return props != null ? props : new HashMap<>();
    }

    // ========================================================================
    // Entity header construction
    // ========================================================================

    /**
     * Build an {@link AtlasEntityHeader} from vertex properties.
     * This is a simplified version of EntityGraphRetriever.toAtlasEntityHeader()
     * that reads directly from the properties map instead of going through the
     * graph abstraction.
     */
    private AtlasEntityHeader buildEntityHeader(VertexData vertex, Set<String> requestedAttributes) {
        AtlasEntityHeader header = new AtlasEntityHeader();
        header.setGuid(vertex.guid);
        header.setTypeName(vertex.typeName);

        // Status
        String state = vertex.state;
        if ("ACTIVE".equals(state)) {
            header.setStatus(AtlasEntity.Status.ACTIVE);
        } else if ("DELETED".equals(state)) {
            header.setStatus(AtlasEntity.Status.DELETED);
        }

        // Display text (name or qualifiedName)
        Object name = vertex.properties.get("name");
        if (name != null) {
            header.setDisplayText(String.valueOf(name));
        } else {
            Object qn = vertex.properties.get("qualifiedName");
            if (qn != null) header.setDisplayText(String.valueOf(qn));
        }

        // Copy requested attributes
        if (requestedAttributes != null) {
            Map<String, Object> attrs = new HashMap<>();
            for (String attrName : requestedAttributes) {
                Object value = vertex.properties.get(attrName);
                if (value != null) {
                    attrs.put(attrName, value);
                }
            }
            if (!attrs.isEmpty()) {
                header.setAttributes(attrs);
            }
        }

        return header;
    }

    // ========================================================================
    // Type checks
    // ========================================================================

    private boolean isProcessType(String typeName) {
        if (typeName == null) return false;
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) return false;
        return entityType.getTypeAndAllSuperTypes().contains(PROCESS_SUPER_TYPE);
    }

    /**
     * Determines if a vertex acts as a "connector" (Process-like) in the lineage graph.
     * Connector vertices use OUT edges for traversal; non-connector use IN edges.
     *
     * For product-asset lineage: only DataProduct is non-connector.
     * For dataset-process lineage: Process and ConnectionProcess are connectors;
     * DataSet, Connection, and DataProduct are non-connectors.
     */
    private boolean isConnectorVertex(String typeName, String lineageType) {
        if (typeName == null) return false;
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) return false;
        Set<String> allTypes = entityType.getTypeAndAllSuperTypes();

        if (LineageListRequest.LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE.equals(lineageType)) {
            return !DATA_PRODUCT_ENTITY_TYPE.equals(entityType.getTypeName());
        }

        // Dataset-process lineage: Catalog, Connection, DataProduct are NOT connectors
        if (allTypes.contains(DATA_SET_SUPER_TYPE) || allTypes.contains(CONNECTION_ENTITY_TYPE)
                || DATA_PRODUCT_ENTITY_TYPE.equals(entityType.getTypeName())) {
            return false;
        }
        // Process, ConnectionProcess are connectors
        return true;
    }

    /**
     * Resolve the lineage input/output edge labels.
     * EntityLineageService sets these on RequestContext BEFORE delegating to us.
     * Falls back to PROCESS_INPUTS_EDGE/PROCESS_OUTPUTS_EDGE if not set.
     */
    private String getLineageInputLabel() {
        String label = RequestContext.get().getLineageInputLabel();
        return StringUtils.isNotEmpty(label) ? label : PROCESS_INPUTS_EDGE;
    }

    private String getLineageOutputLabel() {
        String label = RequestContext.get().getLineageOutputLabel();
        return StringUtils.isNotEmpty(label) ? label : PROCESS_OUTPUTS_EDGE;
    }

    // ========================================================================
    // Constraint helpers
    // ========================================================================

    private LineageOnDemandConstraints getConstraints(
            String guid, AtlasLineageOnDemandContext context) {
        if (context.getConstraints() != null && context.getConstraints().containsKey(guid)) {
            LineageOnDemandConstraints c = context.getConstraints().get(guid);
            if (c != null) return c;
        }
        // Fallback: build default constraints
        LineageOnDemandConstraints defaults = new LineageOnDemandConstraints();
        defaults.setDirection(LineageDirection.BOTH);
        defaults.setDepth(DEFAULT_DEPTH);
        if (context.getDefaultParams() != null) {
            defaults.setInputRelationsLimit(context.getDefaultParams().getInputRelationsLimit());
            defaults.setOutputRelationsLimit(context.getDefaultParams().getOutputRelationsLimit());
        } else {
            int defaultLimit = AtlasConfiguration.LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT.getInt();
            defaults.setInputRelationsLimit(defaultLimit);
            defaults.setOutputRelationsLimit(defaultLimit);
        }
        return defaults;
    }

    private int getMaxNodeCount() {
        return AtlasConfiguration.LINEAGE_MAX_NODE_COUNT.getInt();
    }

    // ========================================================================
    // Edge visited key (for duplicate detection, mirrors EntityLineageService)
    // ========================================================================

    private String edgeVisitedKey(EdgeData edge) {
        VertexData inVertex  = getVertexData(edge.inVertexId);
        VertexData outVertex = getVertexData(edge.outVertexId);
        if (inVertex == null || outVertex == null) return edge.edgeId;

        String inGuid  = inVertex.guid;
        String outGuid = outVertex.guid;
        return edge.isInputEdge(getLineageInputLabel())
                ? inGuid + SEPARATOR + outGuid
                : outGuid + SEPARATOR + inGuid;
    }

    // ========================================================================
    // Cleanup (mirrors EntityLineageService.cleanupRelationsOnDemand)
    // ========================================================================

    private void cleanupRelationsOnDemand(AtlasLineageOnDemandInfo lineageInfo) {
        if (lineageInfo != null && lineageInfo.getRelationsOnDemand() != null) {
            lineageInfo.getRelationsOnDemand().entrySet().removeIf(entry -> {
                LineageInfoOnDemand v = entry.getValue();
                return !(v.hasMoreInputs() || v.hasMoreOutputs()
                        || v.hasUpstream() || v.hasDownstream());
            });
        }
    }
}
