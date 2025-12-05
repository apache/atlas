package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.CassandraTagOperation;
import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.model.Tag;
import org.apache.atlas.repository.graphdb.janus.cassandra.ESConnector;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.apache.atlas.utils.AtlasEntityUtil.calculateBucket;

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

            // Group by operation type
            Map<ESDeferredOperation.OperationType, List<ESDeferredOperation>> opsByType = new HashMap<>();
            for (ESDeferredOperation op : esDeferredOperations) {
                opsByType.computeIfAbsent(op.getOperationType(), k -> new ArrayList<>()).add(op);
            }

            for (Map.Entry<ESDeferredOperation.OperationType, List<ESDeferredOperation>> entry : opsByType.entrySet()) {
                ESDeferredOperation.OperationType opType = entry.getKey();
                List<ESDeferredOperation> ops = entry.getValue();

                boolean upsert = (opType == ESDeferredOperation.OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS);

                int batchSize = AtlasConfiguration.ES_BULK_BATCH_SIZE.getInt();
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

            LOG.info("Completed execution of ES operations.");
        } finally {
            AtlasPerfTracer.log(perf);
        }
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
        tag.setBucket(calculateBucket(op.getVertexId()));
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