package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
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
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.DATA_DOMAIN_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STAKEHOLDER_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.STAKEHOLDER_TITLE_ENTITY_TYPE;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.getUUID;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.indexSearchPaginated;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.verifyDuplicateAssetByName;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class StakeholderTitlePreProcessor implements PreProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StakeholderTitlePreProcessor.class);

    public static final String PATTERN_QUALIFIED_NAME_ALL_DOMAINS = "stakeholderTitle/domain/default/%s";
    public static final String PATTERN_QUALIFIED_NAME_DOMAIN = "stakeholderTitle/domain/%s";


    public static final String STAR = "*";
    public static final String ATTR_DOMAIN_QUALIFIED_NAMES = "domainQualifiedNames";

    public static final String REL_ATTR_STAKEHOLDERS = "stakeholders";

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private AtlasEntityStore entityStore;
    protected EntityDiscoveryService discovery;

    public StakeholderTitlePreProcessor(AtlasGraph graph,
                                       AtlasTypeRegistry typeRegistry,
                                       EntityGraphRetriever entityRetriever,
                                       AtlasEntityStore entityStore) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;

        try {
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
        } catch (AtlasException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("StakeholderTitle.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
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
            if (entity.hasRelationshipAttribute(REL_ATTR_STAKEHOLDERS)) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Managing Stakeholders while creating StakeholderTitle");
            }

            if (RequestContext.get().isSkipAuthorizationCheck()) {
                return;
            }

            String name = (String) entity.getAttribute(NAME);
            verifyDuplicateAssetByName(STAKEHOLDER_TITLE_ENTITY_TYPE, name, discovery,
                    String.format("Stakeholder title with name %s already exists", name));

            List<String> domainQualifiedNames = null;

            if (entity.hasAttribute(ATTR_DOMAIN_QUALIFIED_NAMES)) {
                domainQualifiedNames = (List<String>) entity.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
                if (domainQualifiedNames.size() == 0) {
                    throw new AtlasBaseException(BAD_REQUEST, "Please pass attribute domainQualifiedNames");
                }

            } else {
                throw new AtlasBaseException(BAD_REQUEST, "Please pass attribute domainQualifiedNames");
            }

            if (domainQualifiedNames.contains(STAR)) {
                if (domainQualifiedNames.size() > 1) {
                    domainQualifiedNames.clear();
                    domainQualifiedNames.add(STAR);
                    entity.setAttribute(ATTR_DOMAIN_QUALIFIED_NAMES, domainQualifiedNames);
                }

                String qualifiedName = String.format(PATTERN_QUALIFIED_NAME_ALL_DOMAINS, getUUID());
                entity.setAttribute(QUALIFIED_NAME, qualifiedName);

            } else {
                entity.setAttribute(QUALIFIED_NAME, String.format(PATTERN_QUALIFIED_NAME_DOMAIN, getUUID()));
            }

            authorizeDomainAccess(domainQualifiedNames, AtlasPrivilege.ENTITY_UPDATE);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void processUpdateStakeholderTitle(EntityMutationContext context, AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateStakeholderTitle");

        try {
            if (RequestContext.get().isSkipAuthorizationCheck()) {
                return;
            }

            if (entity.hasRelationshipAttribute(REL_ATTR_STAKEHOLDERS)) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Managing Stakeholders while updating StakeholderTitle");
            }

            AtlasVertex vertex = context.getVertex(entity.getGuid());

            String currentName = vertex.getProperty(NAME, String.class);
            String newName = (String) entity.getAttribute(NAME);
            if (!currentName.equals(newName)) {
                verifyDuplicateAssetByName(STAKEHOLDER_TITLE_ENTITY_TYPE, newName, discovery,
                        String.format("Stakeholder title with name %s already exists", newName));
            }

            List<String> domainQualifiedNames;
            if (entity.hasAttribute(ATTR_DOMAIN_QUALIFIED_NAMES)) {
                domainQualifiedNames = (List<String>) entity.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
            } else {
                domainQualifiedNames = vertex.getListProperty(ATTR_DOMAIN_QUALIFIED_NAMES, String.class);
            }

            authorizeDomainAccess(domainQualifiedNames, AtlasPrivilege.ENTITY_UPDATE);

            String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
            entity.setAttribute(QUALIFIED_NAME, vertexQName);

            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, new AtlasEntityHeader(entity)),
                    "update StakeholderTitle: ", entity.getAttribute(NAME));

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeleteStakeholderTitle");

        try {
            AtlasEntity titleEntity = entityRetriever.toAtlasEntity(vertex);
            List<AtlasRelatedObjectId> stakeholders = (List<AtlasRelatedObjectId>) titleEntity.getRelationshipAttribute(REL_ATTR_STAKEHOLDERS);

            Optional activeStakeholder = stakeholders.stream().filter(x -> x.getRelationshipStatus() == AtlasRelationship.Status.ACTIVE).findFirst();
            if (activeStakeholder.isPresent()) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can not delete StakeholderTitle as it has reference to Active Stakeholder");
            }

            List<String> domainQualifiedNames = vertex.getListProperty(ATTR_DOMAIN_QUALIFIED_NAMES, String.class);

            authorizeDomainAccess(domainQualifiedNames, AtlasPrivilege.ENTITY_UPDATE);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void authorizeDomainAccess(List<String> domainQualifiedNames, AtlasPrivilege permission) throws AtlasBaseException {
        for (String domainQualifiedName: domainQualifiedNames) {
            String domainQualifiedNameToAuth;
            if (domainQualifiedNames.contains(STAR)) {
                domainQualifiedNameToAuth = "*/super";
            } else {
                domainQualifiedNameToAuth = domainQualifiedName;
            }

            AtlasEntityHeader domainHeaderToAuth = new AtlasEntityHeader(DATA_DOMAIN_ENTITY_TYPE, mapOf(QUALIFIED_NAME, domainQualifiedNameToAuth));

            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, new AtlasEntityHeader(domainHeaderToAuth)),
                    "create StakeholderTitle for domain ", domainQualifiedName);
        }
    }
}

