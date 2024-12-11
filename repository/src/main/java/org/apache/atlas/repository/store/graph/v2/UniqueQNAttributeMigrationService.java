package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UniqueQNAttributeMigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(UniqueQNAttributeMigrationService.class);

    private final EntityGraphRetriever entityRetriever;


    private Set<String> entityGuids;
    private final TransactionInterceptHelper   transactionInterceptHelper;

    private final String QUALIFIED_NAME_ATTR = "qualifiedName";
    private final String UNIQUE_QUALIFIED_NAME_ATTR = "__u_qualifiedName";

    public UniqueQNAttributeMigrationService(EntityGraphRetriever entityRetriever, Set<String> entityGuids, TransactionInterceptHelper transactionInterceptHelper) {
        this.entityRetriever = entityRetriever;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.entityGuids = entityGuids;
    }

    public void migrateQN() throws Exception {
        try {
            int count = 0;
            int totalUpdatedCount = 0;
            for (String entityGuid : entityGuids) {
                AtlasVertex entityVertex = entityRetriever.getEntityVertex(entityGuid);

                if (entityVertex == null) {
                    LOG.error("Entity vertex not found for guid: {}", entityGuid);
                    continue;
                }

                boolean isCommitRequired = migrateuniqueQnAttr(entityVertex);
                if (isCommitRequired){
                    count++;
                    totalUpdatedCount++;
                }
                else {
                    LOG.info("No changes to commit for entity: {} as no migration needed", entityGuid);
                }

                if (count == 20) {
                    LOG.info("Committing batch of 20 entities...");
                    commitChanges();
                    count = 0;
                }
            }

            if (count > 0) {
                LOG.info("Committing remaining {} entities...", count);
                commitChanges();
            }

            LOG.info("Total Vertex updated: {}", totalUpdatedCount);

        } catch (Exception e) {
            LOG.error("Error while migration unique qualifiedName attribute for entities: {}", entityGuids, e);
            throw e;
        }
    }

    private boolean migrateuniqueQnAttr(AtlasVertex vertex) throws AtlasBaseException {
        try{
            boolean isCommitRequired = false;

            String qualifiedName = vertex.getProperty(QUALIFIED_NAME_ATTR, String.class);
            String uniqueQualifiedName = vertex.getProperty(UNIQUE_QUALIFIED_NAME_ATTR, String.class);

            if(!qualifiedName.equals(uniqueQualifiedName)) {
                vertex.setProperty(UNIQUE_QUALIFIED_NAME_ATTR, qualifiedName);
                isCommitRequired = true;
            }
            return isCommitRequired;
        }catch (Exception e) {
            LOG.error("Failed to migrate unique qualifiedName attribute for entity: ", e);
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