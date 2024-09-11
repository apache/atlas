package org.apache.atlas.repository.store.graph.v2.preprocessor ;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graphdb.AtlasGraph;
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
    private EntityGraphRetriever retrieverNoRelation = null;


    private static final Set<String> excludedTypes = new HashSet<>(Arrays.asList(ATLAS_GLOSSARY_ENTITY_TYPE, ATLAS_GLOSSARY_TERM_ENTITY_TYPE, ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE, DATA_PRODUCT_ENTITY_TYPE, DATA_DOMAIN_ENTITY_TYPE));

    public AssetPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.retrieverNoRelation = new EntityGraphRetriever(graph, typeRegistry, true);
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

        processDomainLinkAttribute(entity, vertex);

        RequestContext.get().endMetricRecord(metricRecorder);
    }


    private void processUpdateAsset(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateAsset");

        processDomainLinkAttribute(entity, vertex);

        RequestContext.get().endMetricRecord(metricRecorder);

    }

    private void processDomainLinkAttribute(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        if(entity.hasAttribute(DOMAIN_GUIDS)){
            validateDomainAssetLinks(entity);
            isAuthorized(vertex);
        }
    }

    private void validateDomainAssetLinks(AtlasEntity entity) throws AtlasBaseException {
        List<String> domainGuids = ( List<String>) entity.getAttribute(DOMAIN_GUIDS);

        if(CollectionUtils.isNotEmpty(domainGuids)){
            if(domainGuids.size() > 1) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Asset can be linked to only one domain");
            }

            if (excludedTypes.contains(entity.getTypeName())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "This AssetType is not allowed to link with Domain", entity.getTypeName());
            }

            for(String domainGuid : domainGuids) {
                AtlasVertex domainVertex = entityRetriever.getEntityVertex(domainGuid);
                if(domainVertex == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, domainGuid);
                }

                String domainEntityType = domainVertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class);

                if (!Objects.equals(domainEntityType, DATA_DOMAIN_ENTITY_TYPE)){
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Asset can be linked to only domain");
                }
            }
        }
    }

    private void isAuthorized(AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntityHeader sourceEntity = retrieverNoRelation.toAtlasEntityHeader(vertex);

        // source -> UPDATE + READ
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, sourceEntity),
                "update on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, sourceEntity),
                "read on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));

    }

}
