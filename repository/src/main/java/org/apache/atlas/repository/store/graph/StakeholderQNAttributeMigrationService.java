package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.TransactionInterceptHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STAKEHOLDER_ENTITY_TYPE;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol.StakeholderPreProcessor.ATTR_DOMAIN_QUALIFIED_NAME;

public class StakeholderQNAttributeMigrationService {
    private static final Logger LOG = LoggerFactory.getLogger(StakeholderQNAttributeMigrationService.class);

    private final EntityGraphRetriever entityRetriever;
    private final TransactionInterceptHelper transactionInterceptHelper;
    private final Set<String> stakeholderGuids;

    public StakeholderQNAttributeMigrationService(EntityGraphRetriever entityRetriever, Set<String> stakeholderGuids, TransactionInterceptHelper transactionInterceptHelper) {
        this.entityRetriever = entityRetriever;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.stakeholderGuids = stakeholderGuids;
    }

    public void migrateStakeholderQN() throws Exception {
        try {
            int count = 0;
            int totalUpdatedCount = 0;

            for (String stakeholderGuid: stakeholderGuids) {
                if (stakeholderGuid == null || stakeholderGuid.isEmpty()) {
                    LOG.error("Stakeholder is null, skipping migration.");
                    continue;
                }

                boolean isCommitRequired = migrateStakeholderQnAttr(stakeholderGuid);
                if (isCommitRequired) {
                    count++;
                    totalUpdatedCount++;
                }

                if (count == 20) {
                    LOG.info("Committing batch of 20 stakeholders...");
                    commitChanges();
                    count = 0;
                }
            }

            if (count > 0) {
                LOG.info("Committing remaining {} stakeholders...", count);
                commitChanges();
            }

            LOG.info("Total stakeholders migrated: {}", totalUpdatedCount);
        } catch (AtlasBaseException e) {
            throw new AtlasBaseException("Stakeholder QN migration failed", e);
        }
    }

    private boolean migrateStakeholderQnAttr(String stakeholderGuid) throws AtlasBaseException {
        boolean isCommitRequired = false;

        try {
            AtlasVertex vertex = entityRetriever.getEntityVertex(stakeholderGuid);
            if (vertex == null) {
                LOG.error("Entity vertex not found for stakeholder: {}", stakeholderGuid);
                return false;
            }

            String vertexTypeName = AtlasGraphUtilsV2.getTypeName(vertex);
            if (!STAKEHOLDER_ENTITY_TYPE.equals(vertexTypeName)) {
                LOG.warn("Skipping migration for entity {}: not a stakeholder entity type, found: {}",
                        stakeholderGuid, vertexTypeName);
                return false;
            }

            String qualifiedName = vertex.getProperty(QUALIFIED_NAME, String.class);
            String stakeholderDomainQualifiedName = vertex.getProperty(ATTR_DOMAIN_QUALIFIED_NAME, String.class);

            if (qualifiedName != null && stakeholderDomainQualifiedName != null) {
                String[] parts = qualifiedName.split("/", 3);
                if (parts.length == 3 && "default".equals(parts[0])) {
                    String uuid = parts[1];
                    String newQualifiedName = String.format("default/%s/%s", uuid, stakeholderDomainQualifiedName);
                    vertex.setProperty(QUALIFIED_NAME, newQualifiedName);
                    isCommitRequired = true;
                } else {
                    LOG.warn("Skipping migration for stakeholder {}: qualifiedName format not supported: {}",
                            stakeholderGuid, qualifiedName);
                }
            } else {
                LOG.warn("Skipping migration for stakeholder {}: missing qualifiedName or stakeholderDomainQualifiedName",
                        stakeholderGuid);
            }
        } catch (Exception e) {
            throw new AtlasBaseException("Failed to migrate stakeholder qualifiedName attribute", e);
        }

        return isCommitRequired;
    }

    public void commitChanges() throws AtlasBaseException {
        try {
            transactionInterceptHelper.intercept();
            LOG.info("Committed entities to the graph");
        } catch (Exception e) {
            LOG.error("Failed to commit changes: ", e);
            throw e;
        }
    }
}
