package org.apache.atlas.repository.store.graph.v2.repair;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.TransactionInterceptHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.OUTPUT_PORT_GUIDS_ATTR;

public class RemoveInvalidGuidsRepairStrategy implements AtlasRepairAttributeStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(RemoveInvalidGuidsRepairStrategy.class);

    private final EntityGraphRetriever entityRetriever;


    private final TransactionInterceptHelper transactionInterceptHelper;


    private static final String REPAIR_TYPE = "REMOVE_INVALID_OUTPUT_PORT_GUIDS";
    private AtlasGraph graph;

    public RemoveInvalidGuidsRepairStrategy(EntityGraphRetriever entityRetriever, TransactionInterceptHelper transactionInterceptHelper, AtlasGraph graph) {
        this.entityRetriever = entityRetriever;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.graph = graph;
    }

    @Override
    public String getRepairType() {
        return REPAIR_TYPE;
    }

    @Override
    public void validate(Set<String> entityGuids, String attributeName) throws AtlasBaseException {
        for (String entityGuid : entityGuids) {
            AtlasVertex entityVertex = entityRetriever.getEntityVertex(entityGuid);

            if (entityVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, "Entity vertex not found for guid: " + entityGuid);
            }
        }
    }

    @Override
    public void repair(Set<String> entityGuids, String attributeName) throws AtlasBaseException {
        try {
            int count = 0;
            int totalUpdatedCount = 0;

            for (String entityGuid : entityGuids) {
                AtlasVertex entityVertex = entityRetriever.getEntityVertex(entityGuid);

                if (entityVertex == null) {
                    LOG.error("Entity vertex not found for guid: {}", entityGuid);
                    continue;
                }

                if (!entityVertex.getPropertyKeys().contains(attributeName)) {
                    LOG.info("Attribute: {} not found for entity: {}. Skipping repair for this entity.", attributeName, entityGuid);
                    continue;
                }

                if (OUTPUT_PORT_GUIDS_ATTR.equals(attributeName)) {
                    boolean isCommitRequired = repairAttr(entityVertex);
                    if (isCommitRequired){
                        count++;
                        totalUpdatedCount++;
                    } else {
                        LOG.info("No changes to commit for entity: {}", entityGuid);
                    }

                    if (count == 50) {
                        LOG.info("Committing batch of 50 entities...");
                        commitChanges();
                        count = 0;
                    }
                }
            }

            if (count > 0) {
                LOG.info("Committing remaining {} entities...", count);
                commitChanges();
            }

            LOG.info("Total Vertex updated: {}", totalUpdatedCount);

        } catch(Exception e){
            LOG.error("Error while performing repair: {}", entityGuids, e);
            throw e;
        }
    }

    private boolean repairAttr(AtlasVertex vertex) throws AtlasBaseException {
        try{
            boolean isCommitRequired = false;

            List<String> outputPortGuids = vertex.getMultiValuedProperty(OUTPUT_PORT_GUIDS_ATTR, String.class);
            if (outputPortGuids == null || outputPortGuids.isEmpty()) {
                LOG.info("No guids found in attribute: {} for entity: {}. Skipping repair for this entity.", OUTPUT_PORT_GUIDS_ATTR, vertex.getProperty("guid", String.class));
                return false;
            }

            List<String> validGuids = new ArrayList<>();
            List<String> invalidGuids = new ArrayList<>();

            for (String guid : outputPortGuids) {
                AtlasVertex portVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);
                if (portVertex != null) {
                    validGuids.add(guid);
                } else {
                    invalidGuids.add(guid);
                }
            }

            if (!invalidGuids.isEmpty()) {
                LOG.info("Removing invalid guids: {} from attribute: {} for entity: {}", invalidGuids, OUTPUT_PORT_GUIDS_ATTR, vertex.getProperty("guid", String.class));

                vertex.removeProperty(OUTPUT_PORT_GUIDS_ATTR);

                for (String validGuid : validGuids) {
                    AtlasGraphUtilsV2.addEncodedProperty(vertex, OUTPUT_PORT_GUIDS_ATTR, validGuid);
                }

                isCommitRequired = true;
            } else {
                LOG.info("All guids in attribute: {} for entity: {} are valid. No repair needed.", OUTPUT_PORT_GUIDS_ATTR, vertex.getProperty("guid", String.class));
            }

            return isCommitRequired;
        } catch (Exception e) {
            LOG.error("Failed to repair attribute for entity: ", e);
            throw e;
        }
    }

    public void commitChanges() throws AtlasBaseException {
        try {
            transactionInterceptHelper.intercept();
            LOG.info("Committed a entity to the graph");
        } catch (Exception e){
            LOG.error("Failed to commit asset: ", e);
            throw e;
        }
    }
}