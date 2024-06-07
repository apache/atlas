package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
            if(productVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, this.productGuid);
            }

            migrateAttr(productVertex);
            commitChanges();
        } catch (Exception e) {
            LOG.error("Error while migration inputs/outputs for Dataproduct: {}", this.productGuid, e);
            throw e;
        }
    }

    private void migrateAttr(AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntity productEntity = entityRetriever.toAtlasEntity(vertex);
        List<Object> outputPortsRelation = (List<Object>) productEntity.getRelationshipAttribute(OUTPUT_PORT_ATTR);
        List<String> outputPortsRelationGuids = getAssetGuids(outputPortsRelation);
        List<String> outputPortGuidsAttr = (List<String>) productEntity.getAttribute(OUTPUT_PORT_GUIDS_ATTR);

        List<Object> inputPortsRelation = (List<Object>) productEntity.getRelationshipAttribute(INPUT_PORT_ATTR);
        List<String> inputPortsRelationGuids = getAssetGuids(inputPortsRelation);
        List<String> inputPortGuidsAttr = (List<String>) productEntity.getAttribute(INPUT_PORT_GUIDS_ATTR);

        if(!CollectionUtils.isEqualCollection(outputPortsRelationGuids, outputPortGuidsAttr)) {
           LOG.info("Migrating outputPort guid attribute: {} for Product: {}", OUTPUT_PORT_GUIDS_ATTR, productEntity.getGuid());
           addInternalAttr(vertex, OUTPUT_PORT_GUIDS_ATTR, outputPortsRelationGuids);
        }

        if(!CollectionUtils.isEqualCollection(inputPortsRelationGuids, inputPortGuidsAttr)) {
            LOG.info("Migrating inputPort guid attribute: {} for Product: {}", INPUT_PORT_GUIDS_ATTR, productEntity.getGuid());
            addInternalAttr(vertex, INPUT_PORT_GUIDS_ATTR, inputPortsRelationGuids);
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

    private List<String> getAssetGuids(List<Object> elements){
        List<String> guids = new ArrayList<>();
        for(Object element : elements){
            AtlasRelatedObjectId relatedObjectId = (AtlasRelatedObjectId) element;
            guids.add(relatedObjectId.getGuid());
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