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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.getUUID;

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
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can not attach a Stakeholder while creating StakeholderTitle");
            }

            String qualifiedName;

            String domainGuid = (String) entity.getAttribute("domainGuid");
            if ("*".equals(domainGuid)) {
                qualifiedName = String.format("stakeholderTitle/domain/default/%s", getUUID());
                //TODO: validate name duplication
            } else {

                AtlasVertex domain = entityRetriever.getEntityVertex(domainGuid);
                qualifiedName = String.format("stakeholderTitle/domain/%s/%s",
                        getUUID(),
                        domain.getProperty(QUALIFIED_NAME, String.class));

                //TODO: validate name duplication
            }

            entity.setAttribute(QUALIFIED_NAME, qualifiedName);

            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(entity)),
                    "create StakeholderTitle: ", entity.getAttribute(NAME));

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void processUpdateStakeholderTitle(EntityMutationContext context, AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateStakeholderTitle");

        try {
            if (entity.hasRelationshipAttribute("stakeholders")) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can not attach/detach a Stakeholder while updating StakeholderTitle");
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

