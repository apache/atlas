package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.AtlasErrorCode.TYPE_NAME_INVALID;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public abstract class AbstractContractPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractContractPreProcessor.class);

    public final AtlasTypeRegistry typeRegistry;
    public final EntityGraphRetriever entityRetriever;
    public final AtlasGraph graph;
    private final EntityDiscoveryService discovery;


    AbstractContractPreProcessor(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                 EntityGraphRetriever entityRetriever, EntityDiscoveryService discovery) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.discovery = discovery;
    }

    void authorizeContractCreateOrUpdate(AtlasEntity contractEntity, AtlasEntity associatedAsset) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("authorizeContractUpdate");
        try {
            AtlasEntityHeader entityHeader = new AtlasEntityHeader(associatedAsset);

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

    public AtlasEntity getAssociatedAsset(String datasetQName, DataContract contract) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = new HashMap<>();
        int size = 2;

        List<Map<String, Object>> mustClauseList = new ArrayList<>();
        mustClauseList.add(mapOf("term", mapOf(QUALIFIED_NAME, datasetQName)));
        if (contract.getType() != null) {
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", contract.getType().name())));
        } else {
            mustClauseList.add(mapOf("term", mapOf("__superTypeNames.keyword", SQL_ENTITY_TYPE)));
        }

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));
        dsl.put("sort", Collections.singletonList(mapOf(ATTR_CONTRACT_VERSION, mapOf("order", "desc"))));
        dsl.put("size", size);

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setSuppressLogs(true);

        AtlasSearchResult result = discovery.directIndexSearch(indexSearchParams);
        if (result == null || CollectionUtils.isEmpty(result.getEntities())) {
            throw new AtlasBaseException("Dataset doesn't exist for given qualified name.");

        } else if (result.getEntities().size() >1 ) {
            throw new AtlasBaseException("Multiple dataset exists for given qualified name. " +
                    "Please specify the `type` attribute in contract.");
        } else {
            AtlasEntityHeader datasetEntity = result.getEntities().get(0);
            contract.setType(datasetEntity.getTypeName());
            return new AtlasEntity(datasetEntity);
        }

    }

    AtlasEntityType ensureEntityType(String typeName) throws AtlasBaseException {
        AtlasEntityType ret = typeRegistry.getEntityTypeByName(typeName);

        if (ret == null) {
            throw new AtlasBaseException(TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), typeName);
        }

        return ret;
    }


}
