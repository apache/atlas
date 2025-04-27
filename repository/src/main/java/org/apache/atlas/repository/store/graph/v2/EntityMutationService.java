package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.CassandraTagOperation;
import org.apache.atlas.model.instance.AtlasEntityHeaders;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.Map;
import java.util.Stack;

@Service
public class EntityMutationService {

    private static final Logger LOG = LoggerFactory.getLogger(EntityMutationService.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("store.EntityMutationService");

    private final AtlasEntityStoreV2 entityStore;
    private final EntityMutationPostProcessor entityMutationPostProcessor;
    private final AtlasTypeRegistry typeRegistry;
    private final AtlasEntityStore entitiesStore;
    private final EntityGraphMapper entityGraphMapper;
    private final IAtlasEntityChangeNotifier entityChangeNotifier;
    private final AtlasInstanceConverter instanceConverter;
    private final EntityGraphRetriever entityGraphRetriever;

    @Inject
    public EntityMutationService(AtlasEntityStoreV2 entityStore, EntityMutationPostProcessor entityMutationPostProcessor, AtlasTypeRegistry typeRegistry, AtlasEntityStore entitiesStore, EntityGraphMapper entityGraphMapper, IAtlasEntityChangeNotifier entityChangeNotifier, AtlasInstanceConverter instanceConverter, EntityGraphRetriever entityGraphRetriever) {
        this.entityStore = entityStore;
        this.entityMutationPostProcessor = entityMutationPostProcessor;
        this.typeRegistry = typeRegistry;
        this.entitiesStore = entitiesStore;
        this.entityGraphMapper = entityGraphMapper;
        this.entityChangeNotifier = entityChangeNotifier;
        this.instanceConverter = instanceConverter;
        this.entityGraphRetriever = entityGraphRetriever;
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
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
            AtlasPerfTracer.log(perf);
        }
    }

    public void setClassifications(AtlasEntityHeaders entityHeaders, boolean overrideClassifications) throws AtlasBaseException {

        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityMutationService.setClassifications");
        }

        boolean isGraphTransactionFailed = false;
        try {
            ClassificationAssociator.Updater associator = new ClassificationAssociator.Updater(typeRegistry, entitiesStore, entityGraphMapper, entityChangeNotifier, instanceConverter, entityGraphRetriever);
            associator.setClassifications(entityHeaders.getGuidHeaderMap(), overrideClassifications);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
            AtlasPerfTracer.log(perf);
        }
    }

    private void executeESPostProcessing(boolean isGraphTransactionFailed) {
        if (!isGraphTransactionFailed && !RequestContext.get().getESDeferredOperations().isEmpty()) {
            try {
                entityMutationPostProcessor.executeESOperations(RequestContext.get().getESDeferredOperations());
            } catch (Exception e) {
                LOG.error("Failed to execute ES operations after graph transaction failure", e);
            }
        }
    }

    private void rollbackNativeCassandraOperations() throws AtlasBaseException {
        Map<String, Stack<CassandraTagOperation>> cassandraTagOps = RequestContext.get().getCassandraTagOperations();
        entityMutationPostProcessor.rollbackCassandraTagOperations(cassandraTagOps);

        // Can add more rollbacks for id-graph operations if needed
    }

}