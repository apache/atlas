package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.CassandraTagOperation;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Stack;

@Component
public class AtlasCassandraESRollbackHandler implements AtlasRollbackHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasCassandraESRollbackHandler.class);

    private final TagDAO tagDAO;

    public AtlasCassandraESRollbackHandler(TagDAO tagDAO) {
        this.tagDAO = tagDAO;
    }

    @Override
    public void rollbackCassandraTagOperations(Map<String, Stack<CassandraTagOperation>> cassandraOps) {
        if (MapUtils.isEmpty(cassandraOps))
            return;
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(LOG))
                perf = AtlasPerfTracer.getPerfTracer(LOG, "AtlasCassandraESRollbackHandler.rollbackCassandraOperations");

            LOG.info("Rolling back {} Cassandra tag operations",
                    cassandraOps.values().stream().mapToInt(Stack::size).sum());

            for (Map.Entry<String, Stack<CassandraTagOperation>> entry : cassandraOps.entrySet()) {
                rollbackEntityOperations(entry.getKey(), entry.getValue());
            }
            LOG.info("Completed rollback of Cassandra tag operations.");
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private void rollbackEntityOperations(String entityGuid, Stack<CassandraTagOperation> operations) {
        int opsCount = operations.size();
        LOG.info("Rolling back {} Cassandra tag operations for entity {}", opsCount, entityGuid);

        while (!operations.isEmpty()) {
            CassandraTagOperation op = operations.pop();
            try {
                rollbackOperation(entityGuid, op);
            } catch (Exception rollbackError) {
                LOG.error("Error rolling back Cassandra operation for entity {}: {}",
                        entityGuid, rollbackError.getMessage(), rollbackError);
            }
        }
        LOG.info("Completed rollback of {} operations for entity {}", opsCount, entityGuid);
    }

    private void rollbackOperation(String entityGuid, CassandraTagOperation op) throws AtlasBaseException {
        switch (op.getOperationType()) {
            case INSERT -> tagDAO.deleteDirectTag(op.getId(), op.getAtlasClassification());
            case UPDATE, DELETE -> tagDAO.putDirectTag(op.getId(),
                    op.getTagTypeName(),
                    op.getAtlasClassification(),
                    op.getMinAssetMap());
            default -> LOG.warn("Unknown operation type {} for entity {}", op.getOperationType(), entityGuid);
        }
    }

}