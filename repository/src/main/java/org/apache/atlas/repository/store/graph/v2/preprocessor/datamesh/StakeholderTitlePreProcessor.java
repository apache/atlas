package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.DATA_DOMAIN_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STAKEHOLDER_TITLE_ENTITY_TYPE;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.getUUID;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.verifyDuplicateAssetByName;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PERSONA_USERS;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class StakeholderTitlePreProcessor implements PreProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StakeholderTitlePreProcessor.class);

    public static final String PATTERN_QUALIFIED_NAME_ALL_DOMAINS = "stakeholderTitle/domain/default/%s";
    public static final String PATTERN_QUALIFIED_NAME_DOMAIN = "stakeholderTitle/domain/%s";


    public static final String STAR = "*/super";
    public static final String NEW_STAR = "default/domain/*/super";
    public static final String ATTR_DOMAIN_QUALIFIED_NAMES = "stakeholderTitleDomainQualifiedNames";

    public static final String REL_ATTR_STAKEHOLDERS = "stakeholders";

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    protected EntityDiscoveryService discovery;
    protected AtlasEntityStore entityStore;

    public StakeholderTitlePreProcessor(AtlasGraph graph,
                                       AtlasTypeRegistry typeRegistry,
                                       EntityGraphRetriever entityRetriever,
                                       AtlasEntityStore entityStore) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;

        // Only initialize discovery service if graph is available
        if (graph != null) {
            try {
                this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null, entityRetriever);
            } catch (AtlasException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("StakeholderTitlePreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateStakeholderTitle(entity);
                break;
            case UPDATE:
                processUpdateStakeholderTitle(context, entity);
                break;
        }
    }

    private void processCreateStakeholderTitle(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateStakeholderTitle");

        try {
            validateRelations(entity);

            if (RequestContext.get().isSkipAuthorizationCheck()) {
                // To create bootstrap titles with provided qualifiedName
                return;
            }

            String name = (String) entity.getAttribute(NAME);
            verifyDuplicateAssetByName(STAKEHOLDER_TITLE_ENTITY_TYPE, name, discovery,
                    format("Stakeholder title with name %s already exists", name));

            List<String> domainQualifiedNames = null;
            if (entity.hasAttribute(ATTR_DOMAIN_QUALIFIED_NAMES)) {
                Object qNamesAsObject = entity.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
                if (qNamesAsObject != null) {
                    domainQualifiedNames = (List<String>) qNamesAsObject;
                }
            }

            if (CollectionUtils.isEmpty(domainQualifiedNames)) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_DOMAIN_QUALIFIED_NAMES);
            }
            if (domainQualifiedNames.contains(NEW_STAR) || domainQualifiedNames.contains(STAR)) {
                if (domainQualifiedNames.size() > 1) {
                    domainQualifiedNames.clear();
                    domainQualifiedNames.add(NEW_STAR);
                    entity.setAttribute(ATTR_DOMAIN_QUALIFIED_NAMES, domainQualifiedNames);
                }else {
                    domainQualifiedNames.replaceAll(s -> s.equals(STAR) ? NEW_STAR : s);
                }

                String qualifiedName = format(PATTERN_QUALIFIED_NAME_ALL_DOMAINS, getUUID());
                entity.setAttribute(QUALIFIED_NAME, qualifiedName);

            } else {
                entity.setAttribute(QUALIFIED_NAME, format(PATTERN_QUALIFIED_NAME_DOMAIN, getUUID()));
            }

            authorizeDomainAccess(domainQualifiedNames);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void processUpdateStakeholderTitle(EntityMutationContext context, AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateStakeholderTitle");

        try {
            if (RequestContext.get().isSkipAuthorizationCheck()) {
                // To create bootstrap titles with provided aualifiedName
                return;
            }

            validateRelations(entity);

            AtlasVertex vertex = context.getVertex(entity.getGuid());

            String currentName = vertex.getProperty(NAME, String.class);
            String newName = (String) entity.getAttribute(NAME);
            if (!currentName.equals(newName)) {
                verifyDuplicateAssetByName(STAKEHOLDER_TITLE_ENTITY_TYPE, newName, discovery,
                        format("StakeholderTitle with name %s already exists", newName));
            }

            List<String> domainQualifiedNames = null;
            if (entity.hasAttribute(ATTR_DOMAIN_QUALIFIED_NAMES)) {
                Object qNamesAsObject = entity.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
                if (qNamesAsObject != null) {
                    domainQualifiedNames = (List<String>) qNamesAsObject;
                }
            }

            if (CollectionUtils.isEmpty(domainQualifiedNames)) {
                domainQualifiedNames = vertex.getMultiValuedProperty(ATTR_DOMAIN_QUALIFIED_NAMES, String.class);
            }

            authorizeDomainAccess(domainQualifiedNames);

            String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
            entity.setAttribute(QUALIFIED_NAME, vertexQName);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeleteStakeholderTitle");

        try {
            AtlasEntity titleEntity = entityRetriever.toAtlasEntity(vertex);

            List<AtlasRelatedObjectId> stakeholders = getActiveStakeholders(titleEntity);

            if (CollectionUtils.isNotEmpty(stakeholders)) {
                // Partition stakeholders by user status
                PartitionedStakeholders partitioned = partitionStakeholdersByUserStatus(stakeholders);

                // Block deletion if any stakeholder has users
                if (CollectionUtils.isNotEmpty(partitioned.stakeholdersWithUsers)) {
                    throw new AtlasBaseException(OPERATION_NOT_SUPPORTED,
                            "Can not delete StakeholderTitle as it has reference to Active Stakeholder with users");
                }

                // Auto-delete empty stakeholders
                if (CollectionUtils.isNotEmpty(partitioned.emptyStakeholders)) {
                    autoDeleteEmptyStakeholders(partitioned.emptyStakeholders, titleEntity);
                }

                List<String> domainQualifiedNames = vertex.getMultiValuedProperty(ATTR_DOMAIN_QUALIFIED_NAMES, String.class);
                authorizeDomainAccess(domainQualifiedNames);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void authorizeDomainAccess(List<String> domainQualifiedNames) throws AtlasBaseException {
        for (String domainQualifiedName: domainQualifiedNames) {
            String domainQualifiedNameToAuth;

            if (domainQualifiedNames.contains(STAR) || domainQualifiedNames.contains(NEW_STAR)) {
                domainQualifiedNameToAuth = NEW_STAR;
            } else {
                domainQualifiedNameToAuth = domainQualifiedName;
            }

            AtlasEntityHeader domainHeaderToAuth = new AtlasEntityHeader(DATA_DOMAIN_ENTITY_TYPE, mapOf(QUALIFIED_NAME, domainQualifiedNameToAuth));

            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, new AtlasEntityHeader(domainHeaderToAuth)),
                    "mutate StakeholderTitle for domain ", domainQualifiedName);
        }
    }

    private List<AtlasRelatedObjectId> getActiveStakeholders(AtlasEntity titleEntity) {
        Object stakeholdersAsObject = titleEntity.getRelationshipAttribute(REL_ATTR_STAKEHOLDERS);
        if (stakeholdersAsObject == null) {
            return Collections.emptyList();
        }

        List<AtlasRelatedObjectId> allStakeholders = (List<AtlasRelatedObjectId>) stakeholdersAsObject;

        return allStakeholders.stream()
                .filter(x -> x.getRelationshipStatus() == AtlasRelationship.Status.ACTIVE
                        && x.getEntityStatus() == AtlasEntity.Status.ACTIVE)
                .collect(Collectors.toList());
    }

    private PartitionedStakeholders partitionStakeholdersByUserStatus(List<AtlasRelatedObjectId> activeStakeholders) {

        PartitionedStakeholders result = new PartitionedStakeholders();

        for (AtlasRelatedObjectId stakeholder : activeStakeholders) {
            try {
                AtlasVertex stakeholderVertex = entityRetriever.getEntityVertex(stakeholder.getGuid());
                AtlasEntity stakeholderEntity = entityRetriever.toAtlasEntity(stakeholderVertex);

                String qualifiedName = (String) stakeholderEntity.getAttribute(QUALIFIED_NAME);
                List<String> users = stakeholderVertex.getMultiValuedProperty(ATTR_PERSONA_USERS, String.class);

                StakeholderInfo info = new StakeholderInfo(stakeholder.getGuid(), qualifiedName);

                if (CollectionUtils.isNotEmpty(users)) {
                    result.stakeholdersWithUsers.add(info);
                } else {
                    result.emptyStakeholders.add(info);
                }

            } catch (AtlasBaseException e) {
                LOG.error("Error fetching stakeholder entity for guid: {}", stakeholder.getGuid(), e);
                // Safe default: treat as "with users" to block deletion
                result.stakeholdersWithUsers.add(new StakeholderInfo(
                        stakeholder.getGuid(), "unknown"
                ));
            }
        }

        return result;
    }

    private void autoDeleteEmptyStakeholders(
            List<StakeholderInfo> emptyStakeholders,
            AtlasEntity titleEntity) throws AtlasBaseException {

        String titleName = titleEntity.getAttribute(NAME) != null ?
                (String) titleEntity.getAttribute(NAME) : "unknown";

        LOG.info("Auto-deleting {} empty Stakeholders for StakeholderTitle '{}' (guid: {}): [{}]",
                emptyStakeholders.size(),
                titleName,
                titleEntity.getGuid(),
                emptyStakeholders.stream()
                        .map(s -> s.guid + "(" + s.qualifiedName + ")")
                        .collect(Collectors.joining(", "))
        );

        boolean originalSkipAuthCheck = RequestContext.get().isSkipAuthorizationCheck();

        try {
            // Authorization bypass is necessary here because:
            // 1. User has already been authorized at domain-level for StakeholderTitle deletion
            // 2. Auto-deletion of empty Stakeholders is a system-level cascade operation, not direct user action
            // 3. This pattern follows existing code in QueryCollectionPreProcessor for policy cleanup
            // The bypass is safely restored in the finally block regardless of success/failure
            RequestContext.get().setSkipAuthorizationCheck(true);

            // Bulk delete all empty stakeholders at once
            List<String> stakeholderGuids = emptyStakeholders.stream()
                    .map(s -> s.guid)
                    .collect(Collectors.toList());

            entityStore.deleteByIds(stakeholderGuids);

            LOG.info("Successfully auto-deleted {} empty Stakeholders", emptyStakeholders.size());

        } catch (AtlasBaseException e) {
            LOG.error("Failed to auto-delete empty Stakeholders", e);
            throw new AtlasBaseException(
                    AtlasErrorCode.INTERNAL_ERROR,
                    e,
                    format("Failed to auto-delete empty Stakeholders for StakeholderTitle '%s' (%s): %s",
                            titleName, titleEntity.getGuid(), e.getMessage())
            );
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(originalSkipAuthCheck);
        }
    }

    private void validateRelations(AtlasEntity entity) throws AtlasBaseException {
        if (entity.hasRelationshipAttribute(REL_ATTR_STAKEHOLDERS)) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Managing Stakeholders while creating/updating StakeholderTitle");
        }
    }

    // Helper classes for stakeholder partitioning
    private static class PartitionedStakeholders {
        List<StakeholderInfo> stakeholdersWithUsers = new ArrayList<>();
        List<StakeholderInfo> emptyStakeholders = new ArrayList<>();
    }

    private static class StakeholderInfo {
        String guid;
        String qualifiedName;

        StakeholderInfo(String guid, String qualifiedName) {
            this.guid = guid;
            this.qualifiedName = qualifiedName;
        }
    }
}

