package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.TYPE_NAME_INVALID;
import static org.apache.atlas.repository.Constants.*;

public abstract class AbstractContractPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractContractPreProcessor.class);

    public final AtlasTypeRegistry typeRegistry;
    public final EntityGraphRetriever entityRetriever;
    public final AtlasGraph graph;


    AbstractContractPreProcessor(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                 EntityGraphRetriever entityRetriever) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
    }

    void authorizeContractCreateOrUpdate(AtlasEntity contractEntity, AtlasEntity.AtlasEntityWithExtInfo associatedAsset) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("authorizeContractUpdate");
        try {
            AtlasEntityHeader entityHeader = new AtlasEntityHeader(associatedAsset.getEntity());

            //First authorize entity update access
            verifyAssetAccess(entityHeader, AtlasPrivilege.ENTITY_UPDATE, contractEntity, AtlasPrivilege.ENTITY_UPDATE);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }


    private void verifyAssetAccess(AtlasEntityHeader asset, AtlasPrivilege assetPrivilege,
                                   AtlasEntity contract, AtlasPrivilege contractPrivilege) throws AtlasBaseException {
        verifyAccess(asset, assetPrivilege);
        verifyAccess(contract, contractPrivilege);
    }

    private void verifyAccess(AtlasEntity entity, AtlasPrivilege privilege) throws AtlasBaseException {
        verifyAccess(new AtlasEntityHeader(entity), privilege);
    }

    private void verifyAccess(AtlasEntityHeader entityHeader, AtlasPrivilege privilege) throws AtlasBaseException {
        String errorMessage = privilege.name() + " entity: " + entityHeader.getTypeName();
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, privilege, entityHeader), errorMessage);
    }

    AtlasEntity.AtlasEntityWithExtInfo getAssociatedAsset(String datasetQName, String typeName) throws AtlasBaseException {

        Map<String, Object> uniqAttributes = new HashMap<>();
        uniqAttributes.put(QUALIFIED_NAME, datasetQName);

        AtlasEntityType entityType = ensureEntityType(typeName);

        AtlasVertex entityVertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(graph, entityType, uniqAttributes);

        AtlasEntity.AtlasEntityWithExtInfo ret = entityRetriever.toAtlasEntityWithExtInfo(entityVertex);

        if (ret == null) {
            throw new AtlasBaseException(INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, entityType.getTypeName(),
                    uniqAttributes.toString());
        }
        return ret;
    }

    AtlasEntityType ensureEntityType(String typeName) throws AtlasBaseException {
        AtlasEntityType ret = typeRegistry.getEntityTypeByName(typeName);

        if (ret == null) {
            throw new AtlasBaseException(TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), typeName);
        }

        return ret;
    }


}
