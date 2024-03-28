package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.resource.AbstractResourcePreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.Constants.ASSET_RELATION_ATTR;

public abstract class AbstractContractPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractResourcePreProcessor.class);

    public final AtlasTypeRegistry typeRegistry;
    public final EntityGraphRetriever entityRetriever;
    public final AtlasGraph graph;


    AbstractContractPreProcessor(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                 EntityGraphRetriever entityRetriever) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
    }

    void authorizeResourceUpdate(AtlasEntity resourceEntity, AtlasVertex ResourceVertex, String edgeLabel) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("authorizeResourceUpdate");

        try {
            AtlasEntityHeader assetEntity = null;

            AtlasObjectId asset = getAssetRelationAttr(resourceEntity);
            if (asset != null) {
                //Found linked asset in payload
                AtlasVertex assetVertex = entityRetriever.getEntityVertex(asset);
                assetEntity = entityRetriever.toAtlasEntityHeaderWithClassifications(assetVertex);

            } else {
                //Check for linked asset in store
                Iterator atlasVertexIterator = ResourceVertex.query()
                        .direction(AtlasEdgeDirection.IN)
                        .label(edgeLabel)
                        .has(STATE_PROPERTY_KEY, ACTIVE_STATE_VALUE)
                        .vertices()
                        .iterator();

                if (atlasVertexIterator.hasNext()) {
                    //Found linked asset in store
                    AtlasVertex assetVertex = (AtlasVertex) atlasVertexIterator.next();
                    assetEntity = entityRetriever.toAtlasEntityHeaderWithClassifications(assetVertex);
                }
            }

            if (assetEntity != null) {
                //First authorize entity update access
                verifyAssetAccess(assetEntity, AtlasPrivilege.ENTITY_UPDATE, resourceEntity, AtlasPrivilege.ENTITY_UPDATE);
            } else {
                //No linked asset to the Resource, check for resource update permission
                verifyAccess(resourceEntity, AtlasPrivilege.ENTITY_UPDATE);
            }

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    void authorizeResourceDelete(AtlasVertex resourceVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("authorizeResourceDelete");

        try {
            AtlasEntity resourceEntity = entityRetriever.toAtlasEntity(resourceVertex);

            AtlasObjectId asset = getAssetRelationAttr(resourceEntity);
            if (asset != null) {
                AtlasEntityHeader assetEntity =  entityRetriever.toAtlasEntityHeaderWithClassifications(asset.getGuid());
                verifyAssetAccess(assetEntity, AtlasPrivilege.ENTITY_UPDATE, resourceEntity, AtlasPrivilege.ENTITY_DELETE);
            } else {
                //No linked asset to the Resource, check for resource delete permission
                verifyAccess(resourceEntity, AtlasPrivilege.ENTITY_DELETE);
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private AtlasObjectId getAssetRelationAttr(AtlasEntity entity) {
        AtlasObjectId ret = null;

        if (entity.hasRelationshipAttribute(ASSET_RELATION_ATTR) &&
                entity.getRelationshipAttribute(ASSET_RELATION_ATTR) != null) {
            ret = (AtlasObjectId) entity.getRelationshipAttribute(ASSET_RELATION_ATTR);
        }

        return ret;
    }

    private void verifyAssetAccess(AtlasEntityHeader asset, AtlasPrivilege assetPrivilege,
                                   AtlasEntity resource, AtlasPrivilege resourcePrivilege) throws AtlasBaseException {
        verifyAccess(asset, assetPrivilege);
        verifyAccess(resource, resourcePrivilege);
    }

    private void verifyAccess(AtlasEntity entity, AtlasPrivilege privilege) throws AtlasBaseException {
        verifyAccess(new AtlasEntityHeader(entity), privilege);
    }

    private void verifyAccess(AtlasEntityHeader entityHeader, AtlasPrivilege privilege) throws AtlasBaseException {
        String errorMessage = privilege.name() + " entity: " + entityHeader.getTypeName();
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, privilege, entityHeader), errorMessage);
    }


}
