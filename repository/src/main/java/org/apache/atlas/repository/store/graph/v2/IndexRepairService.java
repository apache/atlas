package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.repair.*;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.commons.collections.CollectionUtils;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.graphdb.database.util.StaleIndexRecordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;

@Service
public class IndexRepairService {
    private static final Logger LOG = LoggerFactory.getLogger(IndexRepairService.class);

    // Index names
    private static final String COMPOSITE_INDEX_QN_TYPE = "__u_qualifiedName__typeName";

    private final AtlasGraph graph;

    @Inject
    public IndexRepairService(AtlasGraph graph) {
        this.graph = graph;
    }


    /**
     * Find corrupted vertex using single index (qualifiedName only)
     */
    public Optional<Long> findCorruptedSingleVertex(String qualifiedName) {
        if (StringUtils.isBlank(qualifiedName)) {
            return Optional.empty();
        }

        AtlasGraphQuery query = graph.query()
                .has(Constants.UNIQUE_QUALIFIED_NAME, qualifiedName);

        Iterator<AtlasVertex> vertices = query.vertices().iterator();

        if (vertices.hasNext()) {
            AtlasVertex vertex = vertices.next();
            // Check if vertex is corrupted
            if (CollectionUtils.isEmpty(vertex.getPropertyKeys())){
                return vertex.getId() instanceof Long ? Optional.of((Long) vertex.getId()) : Optional.empty();
            }
        }
        return Optional.empty();
    }

    /**
     * Repair single index for a given qualifiedName
     */
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
                String tempPropertyName = Constants.UNIQUE_QUALIFIED_NAME + "_corrupted_" + System.currentTimeMillis();
                Object oldValue = vertex.getProperty(Constants.UNIQUE_QUALIFIED_NAME, String.class);

                if (oldValue != null) {
                    vertex.setProperty(tempPropertyName, oldValue);
                    vertex.removeProperty(Constants.UNIQUE_QUALIFIED_NAME);
                }

                // Now remove the vertex
                graph.removeVertex(vertex);
                graph.commit();

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

    /**
     * Find corrupted vertex using composite index
     */
    public Optional<Long> findCorruptedCompositeVertex(String qualifiedName, String typeName) {
        if (StringUtils.isBlank(qualifiedName) || StringUtils.isBlank(typeName)) {
            return Optional.empty();
        }

        AtlasGraphQuery query = graph.query()
                .has(Constants.UNIQUE_QUALIFIED_NAME, qualifiedName)
                .has(Constants.TYPE_NAME_PROPERTY_KEY , typeName);

        Iterator<AtlasVertex> vertices = query.vertices().iterator();

        if (vertices.hasNext()) {
            AtlasVertex vertex = vertices.next();
            if (CollectionUtils.isEmpty(vertex.getPropertyKeys())) {
                return vertex.getId() instanceof Long ? Optional.of((Long) vertex.getId()) : Optional.empty();
            }
        }
         return Optional.empty();
    }

    /**
     * Repair composite index for given qualifiedName and typeName
     */
    public RepairResult repairCompositeIndex(String qualifiedName, String typeName) throws AtlasBaseException {
        LOG.info("Starting composite index repair for QN: {}, Type: {}", qualifiedName, typeName);

        Optional<Long> corruptedVertexId = findCorruptedCompositeVertex(qualifiedName, typeName);

        if (!corruptedVertexId.isPresent()) {
            LOG.info("No corrupted vertex found for QN: {}, Type: {}", qualifiedName, typeName);
            return new RepairResult(false, null, "No corruption detected in composite index");
        }

        Long vertexId = corruptedVertexId.get();

        Map<String, Object> indexProperties = new HashMap<>();
        indexProperties.put(Constants.UNIQUE_QUALIFIED_NAME, qualifiedName);
        indexProperties.put(Constants.TYPE_NAME_PROPERTY_KEY, typeName);

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

            janusGraph.tx().commit();
            LOG.info("Removed corrupted composite index entry for vertex: {}", vertexId);

        } catch (Exception e) {
            LOG.error("Failed to remove corrupted composite index entry for vertex: {}", vertexId, e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR,
                    "Failed to repair composite index: " + e.getMessage());
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
        COMPOSITE   // Only composite index (__u_qualifiedName__typeName)
    }
}