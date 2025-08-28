package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.repair.*;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.graphdb.database.util.StaleIndexRecordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;
import java.util.*;

public class IndexRepairService {
    private static final Logger LOG = LoggerFactory.getLogger(IndexRepairService.class);

    // Property keys
    private static final String QUALIFIED_NAME_PROPERTY = "__u_qualifiedName";
    private static final String TYPE_NAME_PROPERTY = "__typeName";
    private static final String GUID_PROPERTY_KEY = "__guid";
    private static final String STATE_PROPERTY_KEY = "__state";

    // Index names
    private static final String COMPOSITE_INDEX_QN_TYPE = "__u_qualifiedName__typeName";

    private final AtlasGraph graph;

    @Inject
    public IndexRepairService(AtlasGraph graph) {
        this.graph = graph;
    }

    // =============== SINGLE INDEX REPAIR ===============

    /**
     * Check if a vertex is corrupted via single index lookup (qualifiedName only)
     */
    public boolean isCorruptedSingleVertex(String qualifiedName) {
        Optional<Long> corruptedVertexId = findCorruptedSingleVertex(qualifiedName);
        return corruptedVertexId.isPresent();
    }

    /**
     * Find corrupted vertex using single index (qualifiedName only)
     */
    public Optional<Long> findCorruptedSingleVertex(String qualifiedName) {
        if (StringUtils.isBlank(qualifiedName)) {
            return Optional.empty();
        }

        AtlasGraphQuery query = graph.query()
                .has(QUALIFIED_NAME_PROPERTY, qualifiedName);

        Iterator<AtlasVertex> vertices = query.vertices().iterator();

        if (vertices.hasNext()) {
            AtlasVertex vertex = vertices.next();
            // Check if vertex is corrupted
            if (StringUtils.isNotEmpty(vertex.getProperty(GUID_PROPERTY_KEY, String.class)) || StringUtils.isNotEmpty(vertex.getProperty(TYPE_NAME_PROPERTY, String.class))){
                return vertex.getId() instanceof Long ? Optional.of((Long) vertex.getId()) : Optional.empty();
            }
        }
        return Optional.empty();
    }

    /**
     * Repair single index for a given qualifiedName
     */
    @GraphTransaction
    public RepairResult repairSingleIndex(String qualifiedName) throws AtlasBaseException {
        LOG.info("Starting single index repair for QN: {}", qualifiedName);

        Optional<Long> corruptedVertexId = findCorruptedSingleVertex(qualifiedName);

        if (!corruptedVertexId.isPresent()) {
            LOG.info("No corrupted vertex found for QN: {}", qualifiedName);
            return new RepairResult(false, null, "No corruption detected in single index");
        }

        Long vertexId = corruptedVertexId.get();

        // For single index, we can rename the property to remove it from index
        try {
            AtlasVertex vertex = graph.getVertex(vertexId.toString());
            if (vertex != null) {
                // Rename the indexed property to remove from index
                String tempPropertyName = QUALIFIED_NAME_PROPERTY + "_corrupted_" + System.currentTimeMillis();
                Object oldValue = vertex.getProperty(QUALIFIED_NAME_PROPERTY, String.class);

                if (oldValue != null) {
                    vertex.setProperty(tempPropertyName, oldValue);
                    vertex.removeProperty(QUALIFIED_NAME_PROPERTY);
                }

                // Now remove the vertex
                graph.removeVertex(vertex);

                LOG.info("Successfully repaired single index for QN: {}, VertexId: {}",
                        qualifiedName, vertexId);

                return new RepairResult(true, vertexId, "Successfully repaired single index");
            }
        } catch (Exception e) {
            LOG.error("Failed to repair single index for vertex: {}", vertexId, e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR,
                    "Failed to repair single index: " + e.getMessage());
        }

        return new RepairResult(false, null, "Vertex not found");
    }

    // =============== COMPOSITE INDEX REPAIR ===============

    /**
     * Check if a vertex is corrupted via composite index lookup
     */
    public boolean isCorruptedCompositeVertex(String qualifiedName, String typeName) {
        Optional<Long> corruptedVertexId = findCorruptedCompositeVertex(qualifiedName, typeName);
        return corruptedVertexId.isPresent();
    }

    /**
     * Find corrupted vertex using composite index
     */
    public Optional<Long> findCorruptedCompositeVertex(String qualifiedName, String typeName) {
        if (StringUtils.isBlank(qualifiedName) || StringUtils.isBlank(typeName)) {
            return Optional.empty();
        }

        AtlasGraphQuery query = graph.query()
                .has(QUALIFIED_NAME_PROPERTY, qualifiedName)
                .has(TYPE_NAME_PROPERTY, typeName);

        Iterator<AtlasVertex> vertices = query.vertices().iterator();

        if (vertices.hasNext()) {
            AtlasVertex vertex = vertices.next();
            if (StringUtils.isNotEmpty(vertex.getProperty(GUID_PROPERTY_KEY, String.class)) || StringUtils.isNotEmpty(vertex.getProperty(STATE_PROPERTY_KEY, String.class))) {
                return vertex.getId() instanceof Long ? Optional.of((Long) vertex.getId()) : Optional.empty();
            }
        }
        return Optional.empty();
    }

    /**
     * Repair composite index for given qualifiedName and typeName
     */
    @GraphTransaction
    public RepairResult repairCompositeIndex(String qualifiedName, String typeName) throws AtlasBaseException {
        LOG.info("Starting composite index repair for QN: {}, Type: {}", qualifiedName, typeName);

        Optional<Long> corruptedVertexId = findCorruptedCompositeVertex(qualifiedName, typeName);

        if (!corruptedVertexId.isPresent()) {
            LOG.info("No corrupted vertex found for QN: {}, Type: {}", qualifiedName, typeName);
            return new RepairResult(false, null, "No corruption detected in composite index");
        }

        Long vertexId = corruptedVertexId.get();

        Map<String, Object> indexProperties = new HashMap<>();
        indexProperties.put(QUALIFIED_NAME_PROPERTY, qualifiedName);
        indexProperties.put(TYPE_NAME_PROPERTY, typeName);

        removeCorruptedCompositeIndexEntry(vertexId, indexProperties, COMPOSITE_INDEX_QN_TYPE);

        LOG.info("Successfully repaired composite index for QN: {}, Type: {}, VertexId: {}",
                qualifiedName, typeName, vertexId);

        return new RepairResult(true, vertexId, "Successfully repaired composite index");
    }

    /**
     * Remove corrupted composite index entry
     */
    private void removeCorruptedCompositeIndexEntry(
            Long vertexId,
            Map<String, Object> indexProperties,
            String compositeIndexName) throws AtlasBaseException {

        try {
            JanusGraph janusGraph = ((AtlasJanusGraph) graph).getGraph();

            StaleIndexRecordUtil.forceRemoveVertexFromGraphIndex(
                    vertexId,
                    indexProperties,
                    janusGraph,
                    compositeIndexName
            );

            try {
                AtlasVertex vertex = graph.getVertex(vertexId.toString());
                if (vertex != null) {
                    graph.removeVertex(vertex);
                }
            } catch (Exception e) {
                LOG.debug("Vertex {} already removed or not accessible", vertexId);
            }

            LOG.info("Removed corrupted composite index entry for vertex: {}", vertexId);

        } catch (Exception e) {
            LOG.error("Failed to remove corrupted composite index entry for vertex: {}", vertexId, e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR,
                    "Failed to repair composite index: " + e.getMessage());
        }
    }

    // =============== AUTO REPAIR (BOTH INDEXES) ===============

    /**
     * Automatically detect and repair both single and composite indexes
     */
    @GraphTransaction
    public RepairResult autoRepair(String qualifiedName, String typeName) throws AtlasBaseException {
        LOG.info("Starting auto repair for QN: {}, Type: {}", qualifiedName, typeName);

        RepairResult result = null;
        boolean singleIndexRepaired = false;
        boolean compositeIndexRepaired = false;
        Long repairedVertexId = null;

        // Check and repair single index
        if (isCorruptedSingleVertex(qualifiedName)) {
            LOG.info("Detected corruption in single index for QN: {}", qualifiedName);
            RepairResult singleResult = repairSingleIndex(qualifiedName);
            singleIndexRepaired = singleResult.isRepaired();
            if (singleIndexRepaired) {
                repairedVertexId = singleResult.getRepairedVertexId();
            }
        }

        // Check and repair composite index (if typeName is provided)
        if (StringUtils.isNotBlank(typeName) && isCorruptedCompositeVertex(qualifiedName, typeName)) {
            LOG.info("Detected corruption in composite index for QN: {}, Type: {}", qualifiedName, typeName);
            RepairResult compositeResult = repairCompositeIndex(qualifiedName, typeName);
            compositeIndexRepaired = compositeResult.isRepaired();
            if (compositeIndexRepaired && repairedVertexId == null) {
                repairedVertexId = compositeResult.getRepairedVertexId();
            }
        }

        // Determine final result
        if (singleIndexRepaired && compositeIndexRepaired) {
            return new RepairResult(true, repairedVertexId, "Repaired both single and composite indexes");
        } else if (singleIndexRepaired) {
            return new RepairResult(true, repairedVertexId, "Repaired single index");
        } else if (compositeIndexRepaired) {
            return new RepairResult(true, repairedVertexId, "Repaired composite index");
        } else {
            return new RepairResult(false, null, "No corruption detected in any index");
        }
    }

    // =============== BATCH OPERATIONS ===============

    /**
     * Batch repair with index type specification
     */
    public BatchRepairResult repairBatch(List<RepairRequest> requests, IndexType indexType) {
        BatchRepairResult result = new BatchRepairResult();

        for (RepairRequest request : requests) {
            try {
                RepairResult repairResult = null;

                switch (indexType) {
                    case SINGLE:
                        repairResult = repairSingleIndex(request.getQualifiedName());
                        break;

                    case COMPOSITE:
                        if (StringUtils.isBlank(request.getTypeName())) {
                            result.addFailure(request, "TypeName is required for composite index repair");
                            continue;
                        }
                        repairResult = repairCompositeIndex(
                                request.getQualifiedName(),
                                request.getTypeName()
                        );
                        break;

                    case AUTO:
                        repairResult = autoRepair(
                                request.getQualifiedName(),
                                request.getTypeName()
                        );
                        break;
                }

                if (repairResult != null && repairResult.isRepaired()) {
                    result.addSuccess(request, repairResult);
                } else if (repairResult != null) {
                    result.addSkipped(request, repairResult.getMessage());
                }

            } catch (Exception e) {
                LOG.error("Failed to repair QN: {}, Type: {}",
                        request.getQualifiedName(), request.getTypeName(), e);
                result.addFailure(request, e.getMessage());
            }
        }

        return result;
    }

    // =============== ENUM FOR INDEX TYPES ===============

    public enum IndexType {
        SINGLE,      // Only single index (__u_qualifiedName)
        COMPOSITE,   // Only composite index (__u_qualifiedName__typeName)
        AUTO         // Check and repair both
    }
}