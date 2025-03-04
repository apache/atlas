package org.apache.atlas.repository.migration;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.TransactionInterceptHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.Constants.EDGE_LABELS_FOR_HARD_DELETION;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.GraphHelper.getStatus;

public class SoftDeletionProductMigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(SoftDeletionProductMigrationService.class);

    private final AtlasGraph graph;
    private final Set<String> productGuids;
    private final GraphHelper graphHelper;
    private final TransactionInterceptHelper transactionInterceptHelper;

    public SoftDeletionProductMigrationService(AtlasGraph graph, Set<String> productGuids, GraphHelper graphHelper, TransactionInterceptHelper transactionInterceptHelper) {
        this.graph = graph;
        this.productGuids = productGuids;
        this.graphHelper = graphHelper;
        this.transactionInterceptHelper = transactionInterceptHelper;
    }

    public void startEdgeMigration() throws AtlasBaseException {
        try {
            int count = 0;
            int totalUpdatedCount = 0;
            for (String productGuid: productGuids) {
                LOG.info("Removing edges for Product: {}", productGuid);

                if (productGuid != null && !productGuid.trim().isEmpty()) {
                    AtlasVertex productVertex = graphHelper.getVertexForGUID(productGuid);

                    if (productVertex == null) {
                        LOG.info("ProductGUID with no vertex found: {}", productGuid);
                    } else {
                        AtlasEntity.Status vertexStatus = getStatus(productVertex);

                        if (ACTIVE.equals(vertexStatus)) {
                            boolean isCommitRequired = deleteEdgeForActiveProduct(productVertex);
                            if (isCommitRequired) {
                                count++;
                                totalUpdatedCount++;
                            }
                        }

                        if (DELETED.equals(vertexStatus)) {
                            boolean isCommitRequired = deleteEdgeForArchivedProduct(productVertex);
                            if (isCommitRequired) {
                                count++;
                                totalUpdatedCount++;
                            }
                        }

                        if (count == 20) {
                            LOG.info("Committing batch of 20 products...");
                            commitChanges();
                            count = 0;
                        }
                    }
                }
            }

            if (count > 0) {
                LOG.info("Committing remaining {} products...", count);
                commitChanges();
            }

            LOG.info("Total products updated: {}", totalUpdatedCount);
        } catch (Exception e) {
            LOG.error("Error while restoring state for Products: {}", productGuids, e);
            throw new AtlasBaseException(e);
        }
    }


    public boolean deleteEdgeForActiveProduct(AtlasVertex productVertex) {
        boolean isCommitRequired = false;
        try {
            Iterator<AtlasEdge> existingEdges = productVertex.getEdges(AtlasEdgeDirection.BOTH, (String[]) EDGE_LABELS_FOR_HARD_DELETION.toArray(new String[0])).iterator();

            if (existingEdges == null || !existingEdges.hasNext()) {
                return isCommitRequired;
            }

            while (existingEdges.hasNext()) {
                AtlasEdge edge = existingEdges.next();

                AtlasEntity.Status edgeStatus = getStatus(edge);

                if (DELETED.equals(edgeStatus)) {
                    graph.removeEdge(edge);
                    isCommitRequired = true;
                }
            }
        } catch (Exception e) {
            LOG.error("Error while deleting soft edges for Active Product: {}", productVertex, e);
            throw new RuntimeException(e);
        }
        return isCommitRequired;
    }


    private boolean deleteEdgeForArchivedProduct(AtlasVertex productVertex) {
        boolean isCommitRequired = false;
        try {
            Long updatedTime = productVertex.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
            Iterator<AtlasEdge> existingEdges = productVertex.getEdges(AtlasEdgeDirection.BOTH, (String[]) EDGE_LABELS_FOR_HARD_DELETION.toArray(new String[0])).iterator();

            if (existingEdges == null || !existingEdges.hasNext()) {
                return isCommitRequired;
            }

            while (existingEdges.hasNext()) {
                AtlasEdge edge = existingEdges.next();
                Long modifiedEdgeTimestamp = edge.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);

                if (!updatedTime.equals(modifiedEdgeTimestamp)) {
                    LOG.info("Removing edge with different timestamp: {}", edge);
                    graph.removeEdge(edge);
                    isCommitRequired = true;
                }
            }
        } catch (Exception e) {
            LOG.error("Error while deleting edges for Archived Product: {}", productVertex, e);
            throw new RuntimeException(e);
        }
        return isCommitRequired;
    }

    public void commitChanges() throws AtlasBaseException {
        try {
            transactionInterceptHelper.intercept();
            LOG.info("Committed a entity to the graph");
        } catch (Exception e) {
            LOG.error("Failed to commit asset: ", e);
            throw e;
        }
    }
}