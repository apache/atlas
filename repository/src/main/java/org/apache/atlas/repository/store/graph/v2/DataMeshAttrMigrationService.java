package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;

public class DataMeshAttrMigrationService implements MigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(DataMeshQNMigrationService.class);

    private final EntityGraphRetriever entityRetriever;

    private final AtlasTypeRegistry typeRegistry;
    private final RedisService redisService;

    private String productGuid;
    private boolean forceRegen;
    private final TransactionInterceptHelper   transactionInterceptHelper;

    public DataMeshAttrMigrationService(EntityGraphRetriever entityRetriever, String productGuid, AtlasTypeRegistry typeRegistry, TransactionInterceptHelper transactionInterceptHelper, RedisService redisService, boolean forceRegen) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.redisService = redisService;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.forceRegen = forceRegen;
        this.productGuid = productGuid;
    }

    public void startMigration() throws Exception {
        try {
            redisService.putValue(DATA_MESH_ATTR, MigrationStatus.IN_PROGRESS.name());

            AtlasVertex productVertex = entityRetriever.getEntityVertex(this.productGuid);
            migrateAttr(productVertex);
            commitChanges();
        } catch (Exception e) {
            LOG.error("Migration failed", e);
            redisService.putValue(DATA_MESH_ATTR, MigrationStatus.FAILED.name());
            throw e;
        }

        redisService.putValue(DATA_MESH_ATTR, MigrationStatus.SUCCESSFUL.name());
    }

    private void migrateAttr(AtlasVertex vertex) throws AtlasBaseException {
        List<Object> outputPorts = vertex.getMultiValuedProperty("outputPorts", Object.class);
        List<String> outputPortGuids = vertex.getMultiValuedProperty(OUTPUT_PORT_GUIDS_ATTR, String.class);

        List<Object> inputPorts = vertex.getMultiValuedProperty("inputPorts", Object.class);
        List<String> inputPortGuids = vertex.getMultiValuedProperty(INPUT_PORT_GUIDS_ATTR, String.class);

        if(outputPorts.size() != outputPortGuids.size()) {
           LOG.info("Migrating outputPort guid attribute: {} for Product: {}", OUTPUT_PORT_GUIDS_ATTR, vertex.getProperty(QUALIFIED_NAME, String.class));
           addGuids(vertex, OUTPUT_PORT_GUIDS_ATTR, outputPorts, outputPortGuids);
        }

        if(inputPorts.size() != inputPortGuids.size()) {
            LOG.info("Migrating inputPort guid attribute: {} for Product: {}", INPUT_PORT_GUIDS_ATTR, vertex.getProperty(QUALIFIED_NAME, String.class));
            addGuids(vertex, INPUT_PORT_GUIDS_ATTR, inputPorts, inputPortGuids);
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

    private void addGuids(AtlasVertex productVertex, String internalAttr, List<Object> currentElements, List<String> currentGuids){
        currentGuids = new ArrayList<>();
        for(Object port : currentElements){
            if(port instanceof Map){
                Map<String, Object> portMap = (Map<String, Object>) port;
                currentGuids.add((String) portMap.get("guid"));
            }
        }

        if (CollectionUtils.isNotEmpty(currentGuids)) {
            currentGuids.forEach(guid -> AtlasGraphUtilsV2.addEncodedProperty(productVertex, internalAttr , guid));
        }
    }

    @Override
    public void run() {
        try {
            LOG.info("Starting migration: {}", DATA_MESH_ATTR);
            startMigration();
            LOG.info("Finished migration: {}", DATA_MESH_ATTR);
        } catch (Exception e) {
            LOG.error("Error running migration : {}",e.toString());
            throw new RuntimeException(e);
        }
    }
}