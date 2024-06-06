package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;

public class DataMeshAttrMigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(DataMeshQNMigrationService.class);

    private final EntityGraphRetriever entityRetriever;

    private final AtlasTypeRegistry typeRegistry;
    private final RedisService redisService;

    private String productGuid;
    private final TransactionInterceptHelper   transactionInterceptHelper;

    public DataMeshAttrMigrationService(EntityGraphRetriever entityRetriever, String productGuid, AtlasTypeRegistry typeRegistry, TransactionInterceptHelper transactionInterceptHelper, RedisService redisService) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.redisService = redisService;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.productGuid = productGuid;
    }

    public void migrateProduct() throws Exception {
        try {
            redisService.putValue(DATA_MESH_ATTR, MigrationStatus.IN_PROGRESS.name());

            AtlasVertex productVertex = entityRetriever.getEntityVertex(this.productGuid);
            migrateAttr(productVertex);
            commitChanges();
        } catch (Exception e) {
            LOG.error("Migration failed for entity", e);
            redisService.putValue(DATA_MESH_ATTR, MigrationStatus.FAILED.name());
            throw e;
        }

        redisService.putValue(DATA_MESH_ATTR, MigrationStatus.SUCCESSFUL.name());
    }

    private void migrateAttr(AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntity productEntity = entityRetriever.toAtlasEntity(vertex);
        List<Object> outputPorts = (List<Object>) productEntity.getRelationshipAttribute(OUTPUT_PORT_ATTR);
        List<String> outputPortGuids = getAssetGuids(outputPorts);
        List<String> outputPortGuidsAttr = vertex.getMultiValuedProperty(OUTPUT_PORT_GUIDS_ATTR, String.class);

        List<Object> inputPorts = (List<Object>) productEntity.getRelationshipAttribute(INPUT_PORT_ATTR);
        List<String> inputPortGuids = getAssetGuids(inputPorts);
        List<String> inputPortGuidsAttr = vertex.getMultiValuedProperty(INPUT_PORT_GUIDS_ATTR, String.class);

        if(CollectionUtils.isEqualCollection(outputPortGuids, outputPortGuidsAttr)) {
           LOG.info("Migrating outputPort guid attribute: {} for Product: {}", OUTPUT_PORT_GUIDS_ATTR, vertex.getProperty(QUALIFIED_NAME, String.class));
           addInternalAttr(vertex, OUTPUT_PORT_GUIDS_ATTR, outputPorts, outputPortGuids);
        }

        if(CollectionUtils.isEqualCollection(inputPortGuids, inputPortGuidsAttr)) {
            LOG.info("Migrating inputPort guid attribute: {} for Product: {}", INPUT_PORT_GUIDS_ATTR, vertex.getProperty(QUALIFIED_NAME, String.class));
            addInternalAttr(vertex, INPUT_PORT_GUIDS_ATTR, inputPorts, inputPortGuids);
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
            if(element instanceof Map){
                Map<String, Object> elementMap = (Map<String, Object>) element;
                guids.add((String) elementMap.get("guid"));
            }
        }
        return guids;
    }

    private void addInternalAttr(AtlasVertex productVertex, String internalAttr, List<Object> currentElements, List<String> currentGuids){
        if (CollectionUtils.isNotEmpty(currentGuids)) {
            currentGuids.forEach(guid -> AtlasGraphUtilsV2.addEncodedProperty(productVertex, internalAttr , guid));
        }
    }
}