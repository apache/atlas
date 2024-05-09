package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.DATA_DOMAIN_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.getUUID;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class StakeholderTitlePreProcessor implements PreProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StakeholderTitlePreProcessor.class);

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private AtlasEntityStore entityStore;

    public StakeholderTitlePreProcessor(AtlasGraph graph,
                                       AtlasTypeRegistry typeRegistry,
                                       EntityGraphRetriever entityRetriever,
                                       AtlasEntityStore entityStore) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;
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
            if (entity.hasRelationshipAttribute("stakeholders")) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Managing Stakeholders while creating StakeholderTitle");
            }

            if (RequestContext.get().isSkipAuthorizationCheck()) {
                return;
            }

            List<String> domainQualifiedNames = null;

            if (entity.hasAttribute("domainQualifiedNames")) {
                domainQualifiedNames = (List<String>) entity.getAttribute("domainQualifiedNames");
                if (domainQualifiedNames.size() == 0) {
                    throw new AtlasBaseException(BAD_REQUEST, "Please pass attribute domainQualifiedNames");
                }

            } else {
                throw new AtlasBaseException(BAD_REQUEST, "Please pass attribute domainQualifiedNames");
            }

            if ((domainQualifiedNames.size() == 1 && "*".equals(domainQualifiedNames.get(0)))
                    || domainQualifiedNames.contains("*")) {

                AtlasEntityHeader allDomainEntityHeader = new AtlasEntityHeader(DATA_DOMAIN_ENTITY_TYPE, mapOf(QUALIFIED_NAME, "*/super"));
                AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(allDomainEntityHeader)),
                        "create StakeholderTitle for all domains");

                String qualifiedName = String.format("stakeholderTitle/domain/default/%s", getUUID());
                entity.setAttribute(QUALIFIED_NAME, qualifiedName);
                entity.setAttribute("domainQualifiedNames", Collections.singletonList("*"));
            } else {
                for (String domainQualifiedName : domainQualifiedNames) {
                    //AtlasVertex domainVertex = entityRetriever.getEntityVertex(new AtlasObjectId(DATA_DOMAIN_ENTITY_TYPE, mapOf(QUALIFIED_NAME, domainQualifiedName)));
                    //AtlasEntityHeader domainHeader = entityRetriever.toAtlasEntityHeader(domainVertex);
                    AtlasEntityHeader domainHeader = new AtlasEntityHeader(DATA_DOMAIN_ENTITY_TYPE, mapOf(QUALIFIED_NAME, domainQualifiedName));
                    String qualifiedName = String.format("stakeholderTitle/domain/%s/%s", getUUID(), domainQualifiedName);

                    entity.setAttribute(QUALIFIED_NAME, qualifiedName);

                    AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(domainHeader)),
                            "create StakeholderTitle for domain ", domainQualifiedName);
                }

                entity.setAttribute(QUALIFIED_NAME, String.format("stakeholderTitle/domain/%s", getUUID()));
            }

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

            if (entity.hasRelationshipAttribute("stakeholders")) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Managing Stakeholders while updating StakeholderTitle");
            }

            AtlasVertex vertex = context.getVertex(entity.getGuid());
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
            List<AtlasRelatedObjectId> stakeholders = (List<AtlasRelatedObjectId>) titleEntity.getRelationshipAttribute("stakeholders");

            Optional activeStakeholder = stakeholders.stream().filter(x -> x.getRelationshipStatus() == AtlasRelationship.Status.ACTIVE).findFirst();
            if (activeStakeholder.isPresent()) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can not delete StakeholderTitle as it has reference to Active Stakeholder");
            }

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }
}

