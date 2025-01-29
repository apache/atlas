package org.apache.atlas.repository.migration;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.graph.GraphHelper.getStatus;

public class ValidateProductEdgesMigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateProductEdgesMigrationService.class);

    private final AtlasGraph graph;
    private final Set<String> productGuids;
    private final GraphHelper graphHelper;

    public ValidateProductEdgesMigrationService(AtlasGraph graph, Set<String> productGuids, GraphHelper graphHelper) {
        this.graph = graph;
        this.productGuids = productGuids;
        this.graphHelper = graphHelper;
    }

    public boolean validateEdgeMigration() throws AtlasBaseException {
        try {
            int count = 0;
            int totalProductChecked = 0;
            boolean redundantEdgesFound = false;

            for (String productGuid: productGuids) {
                LOG.info("Validating edges for Product: {}", productGuid);

                if (productGuid != null && !productGuid.trim().isEmpty()) {
                    AtlasVertex productVertex = graphHelper.getVertexForGUID(productGuid);

                    AtlasEntity.Status vertexStatus = getStatus(productVertex);

                    if (vertexStatus == null) {
                        LOG.info("ProductGUID with no vertex found: {}", productGuid);
                    } else {
                        if (ACTIVE.equals(vertexStatus)) {
                            LOG.info("Validating edges for Active Product: {}", productGuid);
                            boolean softDeletedEdgesFound = validateEdgeForActiveProduct(productVertex);
                            if (softDeletedEdgesFound) {
                                count++;
                                totalProductChecked++;
                            } else {
                                totalProductChecked++;
                            }
                        } else {
                            LOG.info("Validating edges for Archived Product: {}", productGuid);
                            boolean edgeWithDifferentTimeStampFound = validateEdgeForArchivedProduct(productVertex);
                            if (edgeWithDifferentTimeStampFound) {
                                count++;
                                totalProductChecked++;
                            } else {
                                totalProductChecked++;
                            }
                        }
                    }
                }
            }

            if (count > 0) {
                redundantEdgesFound = true;
                LOG.info("Found {} products with redundant edges....", count);
            }

            LOG.info("Total products checked: {}", totalProductChecked);

            return redundantEdgesFound;
        } catch (Exception e) {
            LOG.error("Error while validating edges for Products: {}", productGuids, e);
            throw new AtlasBaseException(e);
        }
    }

    public boolean validateEdgeForActiveProduct (AtlasVertex productVertex) {
        boolean softDeletedEdgesFound = false;

        try {
            Iterator<AtlasEdge> existingEdges = productVertex.getEdges(AtlasEdgeDirection.BOTH).iterator();

            if (existingEdges == null || !existingEdges.hasNext()) {
                LOG.info("No edges found for Product: {}", productVertex);
                return softDeletedEdgesFound;
            }

            while (existingEdges.hasNext()) {
                AtlasEdge edge = existingEdges.next();

                AtlasEntity.Status edgeStatus = getStatus(edge);

                if (DELETED.equals(edgeStatus)) {
                    LOG.info("Found soft deleted edge: {}", edge);
                    softDeletedEdgesFound = true;
                }
            }
        } catch (Exception e) {
            LOG.error("Error while validating edges for Active Product: {}", productVertex, e);
            throw new RuntimeException(e);
        }

        return softDeletedEdgesFound;
    }

    public boolean validateEdgeForArchivedProduct (AtlasVertex productVertex) {
        boolean edgeWithDifferentTimeStampFound = false;
        try {
            Long updatedTime = productVertex.getProperty("__modificationTimestamp", Long.class);
            Iterator<AtlasEdge> existingEdges = productVertex.getEdges(AtlasEdgeDirection.BOTH).iterator();

            if (existingEdges == null || !existingEdges.hasNext()) {
                LOG.info("No edges found for Product: {}", productVertex);
                return edgeWithDifferentTimeStampFound;
            }

            while (existingEdges.hasNext()) {
                AtlasEdge edge = existingEdges.next();
                Long modifiedEdgeTimestamp = edge.getProperty("__modificationTimestamp", Long.class);

                if (!updatedTime.equals(modifiedEdgeTimestamp)) {
                    LOG.info("Found edge with different timestamp: {}", edge);
                    edgeWithDifferentTimeStampFound = true;
                }
            }
        } catch (Exception e) {
            LOG.error("Error while validating edges for Archived Product: {}", productVertex, e);
            throw new RuntimeException(e);
        }
        return edgeWithDifferentTimeStampFound;
    }
}
