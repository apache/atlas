package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;

@Service
public class EntityMutationService {

    private static final Logger LOG = LoggerFactory.getLogger(EntityMutationService.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("store.EntityMutationService");

    private final AtlasEntityStoreV2 entityStore;
    private final AtlasRollbackHandler atlasRollbackHandler;

    @Inject
    public EntityMutationService(AtlasEntityStoreV2 entityStore, AtlasRollbackHandler atlasRollbackHandler) {
        this.entityStore = entityStore;
        this.atlasRollbackHandler = atlasRollbackHandler;
    }

    public EntityMutationResponse createOrUpdate(EntityStream entityStream,
                                                 BulkRequestContext context) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityMutationService.createOrUpdate");
        }

        boolean isGraphTransactionFailed = false;
        try {
            return entityStore.createOrUpdate(entityStream, context);
        } catch (Throwable e) {
            // Rollback
            isGraphTransactionFailed = true;
            atlasRollbackHandler.rollbackCassandraTagOperations(RequestContext.get().getCassandraTagOperations());
            throw e;
        } finally {
            // Only execute ES operations if no errors occurred
//            if (!rollbackRequired && !context.getPendingESOperations().isEmpty()) {
//                try {
//                    //executeESOperations(context.getPendingESOperations());
//                } catch (Exception e) {
//                    LOG.error("Error executing ES operations", e);
//                    // ES failure doesn't invalidate the transaction since graph is already committed
//                    // But we should log and monitor these failures
//                }
//            }

            AtlasPerfTracer.log(perf);
        }
    }

}