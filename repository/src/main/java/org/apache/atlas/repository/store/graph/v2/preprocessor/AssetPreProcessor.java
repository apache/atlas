package org.apache.atlas.repository.store.graph.v2.preprocessor ;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;

public class AssetPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AssetPreProcessor.class);

    private EntityMutationContext context;
    private AtlasTypeRegistry typeRegistry;
    private EntityGraphRetriever entityRetriever;

    public AssetPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("AssetPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }
        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;

        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateAsset(entity, vertex);
                break;
            case UPDATE:
                processUpdateAsset(entity, vertex);
                break;
        }
    }

    private void processCreateAsset(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateAsset");

        processDomainLinkAttribute(entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }


    private void processUpdateAsset(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateAsset");

        processDomainLinkAttribute(entity);

        RequestContext.get().endMetricRecord(metricRecorder);

    }

    private void processDomainLinkAttribute(AtlasEntity entity) throws AtlasBaseException {
        if(entity.hasAttribute(DOMAIN_GUIDS)){
            validateDomainAssetLinks(entity);
            isAuthorized(entity);
        }
    }

    private void validateDomainAssetLinks(AtlasEntity entity) throws AtlasBaseException {
        List<String> domainGuids = ( List<String>) entity.getAttribute(DOMAIN_GUIDS);

        if(domainGuids.size() > 1) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Asset can be linked to only one domain");
        }

        if(CollectionUtils.isNotEmpty(domainGuids)) {
            for(String domainGuid : domainGuids) {
                AtlasVertex domainVertex = entityRetriever.getEntityVertex(domainGuid);
                if(domainVertex == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, domainGuid);
                }

                if (!Objects.equals(entity.getTypeName(), DATA_DOMAIN_ENTITY_TYPE)){
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Asset can be linked to only domain");
                }
            }
        }
    }

    private void isAuthorized(AtlasEntity entity) throws AtlasBaseException {
        AtlasEntityHeader sourceEntity = new AtlasEntityHeader(entity);

        // source -> UPDATE + READ
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, sourceEntity),
                "update on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, sourceEntity),
                "read on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));

    }

}
