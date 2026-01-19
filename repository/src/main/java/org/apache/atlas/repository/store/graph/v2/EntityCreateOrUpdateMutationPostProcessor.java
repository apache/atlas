package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.CassandraTagOperation;
import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.model.Tag;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class EntityCreateOrUpdateMutationPostProcessor implements EntityMutationPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(EntityCreateOrUpdateMutationPostProcessor.class);

    private final TagDAO tagDAO;

    public EntityCreateOrUpdateMutationPostProcessor() {
        this.tagDAO = TagDAOCassandraImpl.getInstance();
    }

    @Override
    public void executeESOperations(List<ESDeferredOperation> esDeferredOperations) {
        if (CollectionUtils.isEmpty(esDeferredOperations)) {
            return;
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(LOG))
                perf = AtlasPerfTracer.getPerfTracer(LOG, "EntityCreateOrUpdateMutationPostProcessor.executeESOperations");

            LOG.info("Executing {} ES operations", esDeferredOperations.size());

            /* MS-456: Group by entity ID instead of operation type to avoid overwrites.
             When multiple operations target the same entity (e.g., UPDATE + ADD),
             we keep only the operation with the most complete denorm data.
             ADD operations are processed last and contain the complete tag list. */
            Map<String, ESDeferredOperation> latestOpByEntity = new LinkedHashMap<>();

            for (ESDeferredOperation op : esDeferredOperations) {
                String entityId = op.getEntityId();
                ESDeferredOperation existing = latestOpByEntity.get(entityId);

                if (existing == null || shouldReplaceOperation(existing.getOperationType(), op.getOperationType())) {
                    latestOpByEntity.put(entityId, op);
                }
            }

            LOG.info("Merged {} ES operations into {} unique entity updates", 
                    esDeferredOperations.size(), latestOpByEntity.size());

            List<ESDeferredOperation> addOps = new ArrayList<>();
            List<ESDeferredOperation> otherOps = new ArrayList<>();

            for (ESDeferredOperation op : latestOpByEntity.values()) {
                if (op.getOperationType() == ESDeferredOperation.OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS) {
                    addOps.add(op);
                } else {
                    otherOps.add(op);
                }
            }

            int batchSize = AtlasConfiguration.ES_BULK_BATCH_SIZE.getInt();
            processOperationBatch(addOps, batchSize, true);
            processOperationBatch(otherOps, batchSize, false);

            LOG.info("Completed execution of ES operations.");
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Process a batch of ES operations with the specified upsert setting.
     */
    private void processOperationBatch(List<ESDeferredOperation> ops, int batchSize, boolean upsert) {
        if (ops.isEmpty()) {
            return;
        }

        for (int i = 0; i < ops.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ops.size());
            List<ESDeferredOperation> batch = ops.subList(i, end);

            Map<String, Map<String, Object>> batchPayload = new HashMap<>();
            for (ESDeferredOperation op : batch) {
                Map<String, Map<String, Object>> payload = op.getPayload();
                if (payload != null) {
                    batchPayload.putAll(payload);
                }
            }

            if (!batchPayload.isEmpty()) {
                ESConnector.writeTagProperties(batchPayload, upsert);
            }
        }
    }

    /**
     * Determines if the incoming operation should replace the existing one for the same entity.
     * 
     * Rules:
     * 1. Same type: always replace (later operation has more up-to-date state after Cassandra writes)
     * 2. Cross-type priority: ADD > UPDATE > DELETE
     *    - ADD operations are processed last in commitChanges() and read from Cassandra after
     *      all deletes and updates are complete, so they have the complete final tag list.
     *    - UPDATE operations run after DELETE, so they have more recent state than DELETE.
     *
     * @param existing  the operation type currently stored for this entity
     * @param incoming  the new operation type being considered
     * @return true if incoming should replace existing
     */
    private boolean shouldReplaceOperation(ESDeferredOperation.OperationType existing,
                                           ESDeferredOperation.OperationType incoming) {
        // Same type: always replace - later operations have more up-to-date state
        // (each operation reads from Cassandra AFTER the previous write)
        if (existing == incoming) {
            return true;
        }

        // ADD operations have the most complete denorm - always prefer them
        if (incoming == ESDeferredOperation.OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS) {
            return true;
        }

        // UPDATE should replace DELETE (UPDATE runs after DELETE in commitChanges)
        if (incoming == ESDeferredOperation.OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS
                && existing == ESDeferredOperation.OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS) {
            return true;
        }

        return false;
    }

    @Override
    public void rollbackCassandraTagOperations(Map<String, Stack<CassandraTagOperation>> cassandraOps) throws AtlasBaseException {
        if (MapUtils.isEmpty(cassandraOps))
            return;
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(LOG))
                perf = AtlasPerfTracer.getPerfTracer(LOG, "EntityCreateOrUpdateMutationPostProcessor.rollbackCassandraOperations");

            LOG.info("Rolling back {} Cassandra tag operations",
                    cassandraOps.values().stream().mapToInt(Stack::size).sum());

            // Collect tags to batch-delete
            int batchSize = AtlasConfiguration.CASSANDRA_BATCH_SIZE.getInt();
            List<Tag> tagsToDelete = new ArrayList<>(batchSize);

            for (Map.Entry<String, Stack<CassandraTagOperation>> entry : cassandraOps.entrySet()) {
                Stack<CassandraTagOperation> operations = entry.getValue();
                while (CollectionUtils.isNotEmpty(operations)) {
                    CassandraTagOperation op = operations.pop();
                    try {
                        if (op.getOperationType() == CassandraTagOperation.OperationType.INSERT) {
                            // Construct Tag for batch delete
                            Tag tag = toTagForDelete(op);
                            tagsToDelete.add(tag);
                            if (tagsToDelete.size() == batchSize) {
                                tagDAO.deleteTags(tagsToDelete);
                                tagsToDelete.clear();
                            }
                        } else {
                            rollbackUpdateOrDelete(entry.getKey(), op);
                        }
                    } catch (Exception rollbackError) {
                        LOG.error("Error rolling back Cassandra operation for entity {}: {}",
                                entry.getKey(), rollbackError.getMessage(), rollbackError);
                    }
                }
            }

            // Delete any remaining tags
            if (!tagsToDelete.isEmpty()) {
                tagDAO.deleteTags(tagsToDelete);
            }

            LOG.info("Completed rollback of Cassandra tag operations.");
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private static Tag toTagForDelete(CassandraTagOperation op) {
        Tag tag = new Tag();
        tag.setVertexId(op.getVertexId());
        tag.setTagTypeName(op.getTagTypeName());
        tag.setSourceVertexId(op.getVertexId());
        tag.setBucket(TagDAOCassandraImpl.calculateBucket(op.getVertexId()));
        return tag;
    }

    private void rollbackUpdateOrDelete(String entityGuid, CassandraTagOperation op) throws AtlasBaseException {
        switch (op.getOperationType()) {
            case UPDATE, DELETE -> tagDAO.putDirectTag(op.getVertexId(),
                    op.getTagTypeName(),
                    op.getAtlasClassification(),
                    op.getMinAssetMap());
            default -> LOG.warn("Unknown operation type {} for entity {}", op.getOperationType(), entityGuid);
        }
    }

}