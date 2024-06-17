package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;

public class DataProductInputsOutputsMigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(DataProductInputsOutputsMigrationService.class);

    private final EntityGraphRetriever entityRetriever;


    private String productGuid;
    private final TransactionInterceptHelper   transactionInterceptHelper;

    public DataProductInputsOutputsMigrationService(EntityGraphRetriever entityRetriever, String productGuid, TransactionInterceptHelper transactionInterceptHelper) {
        this.entityRetriever = entityRetriever;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.productGuid = productGuid;
    }

    public void migrateProduct() throws Exception {
        try {
            AtlasVertex productVertex = entityRetriever.getEntityVertex(this.productGuid);

            boolean isCommitRequired = migrateAttr(productVertex);
            if (isCommitRequired){
                LOG.info("Committing changes for Product: {}", this.productGuid);
                commitChanges();
            }
            else {
                LOG.info("No changes to commit for Product: {} as no migration needed", this.productGuid);
            }

        } catch (Exception e) {
            LOG.error("Error while migration inputs/outputs for Dataproduct: {}", this.productGuid, e);
            throw e;
        }
    }

    private boolean migrateAttr(AtlasVertex vertex) throws AtlasBaseException {
        boolean isCommitRequired = false;

        List<String> outputPortsRelationGuids = getAssetGuids(vertex, OUTPUT_PORT_PRODUCT_EDGE_LABEL);
        List<String> outputPortGuidsAttr = vertex.getMultiValuedProperty(OUTPUT_PORT_GUIDS_ATTR, String.class);


        List<String> inputPortsRelationGuids = getAssetGuids(vertex, INPUT_PORT_PRODUCT_EDGE_LABEL);
        List<String> inputPortGuidsAttr = vertex.getMultiValuedProperty(INPUT_PORT_GUIDS_ATTR, String.class);

        if(!CollectionUtils.isEqualCollection(outputPortsRelationGuids, outputPortGuidsAttr)) {
           LOG.info("Migrating outputPort guid attribute: {} for Product: {}", OUTPUT_PORT_GUIDS_ATTR, this.productGuid);
           addInternalAttr(vertex, OUTPUT_PORT_GUIDS_ATTR, outputPortsRelationGuids);
           isCommitRequired = true;
        }

        if(!CollectionUtils.isEqualCollection(inputPortsRelationGuids, inputPortGuidsAttr)) {
            LOG.info("Migrating inputPort guid attribute: {} for Product: {}", INPUT_PORT_GUIDS_ATTR, this.productGuid);
            addInternalAttr(vertex, INPUT_PORT_GUIDS_ATTR, inputPortsRelationGuids);
            isCommitRequired = true;
        }

        return isCommitRequired;
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

    private List<String> getAssetGuids(AtlasVertex vertex, String edgeLabel) throws AtlasBaseException {
        List<String> guids = new ArrayList<>();
        Iterator<AtlasVertex> activeParent = GraphHelper.getActiveParentVertices(vertex, edgeLabel);
        while(activeParent.hasNext()) {
            AtlasVertex child = activeParent.next();
            guids.add(child.getProperty(GUID_PROPERTY_KEY, String.class));
        }
        return guids;
    }

    private void addInternalAttr(AtlasVertex productVertex, String internalAttr, List<String> currentGuids){
        productVertex.removeProperty(internalAttr);
        if (CollectionUtils.isNotEmpty(currentGuids)) {
            currentGuids.forEach(guid -> AtlasGraphUtilsV2.addEncodedProperty(productVertex, internalAttr , guid));
        }
    }
}