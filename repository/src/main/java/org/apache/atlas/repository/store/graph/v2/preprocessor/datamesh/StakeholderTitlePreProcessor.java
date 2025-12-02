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
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertex;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertexService;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.DATA_DOMAIN_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STAKEHOLDER_TITLE_ENTITY_TYPE;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.getUUID;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.verifyDuplicateAssetByName;
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

    public StakeholderTitlePreProcessor(AtlasGraph graph,
                                        AtlasTypeRegistry typeRegistry,
                                        EntityGraphRetriever entityRetriever,
                                        DynamicVertexService dynamicVertexService) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;

        try {
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, dynamicVertexService, null, entityRetriever);
        } catch (AtlasException e) {
            e.printStackTrace();
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

            List<AtlasRelatedObjectId> stakeholders = null;
            Object stakeholdersAsObject = titleEntity.getRelationshipAttribute(REL_ATTR_STAKEHOLDERS);
            if (stakeholdersAsObject != null) {
                stakeholders = (List<AtlasRelatedObjectId>) stakeholdersAsObject;
            }

            if (CollectionUtils.isNotEmpty(stakeholders)) {
                Optional activeStakeholder = stakeholders.stream().filter(x -> x.getRelationshipStatus() == AtlasRelationship.Status.ACTIVE).findFirst();
                if (activeStakeholder.isPresent()) {
                    throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can not delete StakeholderTitle as it has reference to Active Stakeholder");
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

    private void validateRelations(AtlasEntity entity) throws AtlasBaseException {
        if (entity.hasRelationshipAttribute(REL_ATTR_STAKEHOLDERS)) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Managing Stakeholders while creating/updating StakeholderTitle");
        }
    }
}

