package org.apache.atlas.repository.store.graph.v2.preprocessor ;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class AssetPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AssetPreProcessor.class);

    private EntityMutationContext context;
    private AtlasTypeRegistry typeRegistry;
    private EntityGraphRetriever entityRetriever;
    private EntityGraphRetriever retrieverNoRelation = null;
    private EntityDiscoveryService discovery;
    private final UserGroupAttributeValidator userGroupAttributeValidator;
    private final Set<String> referenceAttributeNames = new HashSet<>(Arrays.asList(OUTPUT_PORT_GUIDS_ATTR, INPUT_PORT_GUIDS_ATTR));
    private final Set<String> referencingEntityTypes = new HashSet<>(Arrays.asList(DATA_PRODUCT_ENTITY_TYPE));


    private static final Set<String> excludedTypes = new HashSet<>(Arrays.asList(ATLAS_GLOSSARY_ENTITY_TYPE, ATLAS_GLOSSARY_TERM_ENTITY_TYPE, ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE, DATA_PRODUCT_ENTITY_TYPE, DATA_DOMAIN_ENTITY_TYPE));
    private static final Set<String> excludedTypesForDataset = buildExcludedTypesForDataset();

    private static Set<String> buildExcludedTypesForDataset() {
        Set<String> types = new HashSet<>(excludedTypes);
        types.add(DATASET_ENTITY_TYPE);
        return types;
    }

    public AssetPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph) {
        this(typeRegistry, entityRetriever, graph, null);
    }

    public AssetPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, EntityDiscoveryService discovery) {
        this(typeRegistry, entityRetriever, graph, discovery, new EntityGraphRetriever(entityRetriever, true), new UserGroupAttributeValidator());
    }

    AssetPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, EntityDiscoveryService discovery, EntityGraphRetriever retrieverNoRelation, UserGroupAttributeValidator userGroupAttributeValidator) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.retrieverNoRelation = retrieverNoRelation;
        this.userGroupAttributeValidator = userGroupAttributeValidator;

        if (discovery != null) {
            this.discovery = discovery;
        } else {
            try {
                this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
            } catch (AtlasException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            Object qualifiedName = entityStruct.getAttribute(QUALIFIED_NAME);
            LOG.debug("AssetPreProcessor.processAttributes: pre processing {}, {}",
                    qualifiedName != null ? qualifiedName.toString().replaceAll("[\r\n]", "_") : "null", operation);
        }
        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;

        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateAsset(entity, vertex, operation);
                break;
            case UPDATE:
                processUpdateAsset(entity, vertex, operation);
                break;
        }
    }

    private void processCreateAsset(AtlasEntity entity, AtlasVertex vertex, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateAsset");

        userGroupAttributeValidator.validate(entity);
        processDomainLinkAttribute(entity, vertex, operation);
        processDatasetLinkAttribute(entity, vertex, operation);

        RequestContext.get().endMetricRecord(metricRecorder);
    }


    private void processUpdateAsset(AtlasEntity entity, AtlasVertex vertex, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateAsset");

        userGroupAttributeValidator.validate(entity);
        processDomainLinkAttribute(entity, vertex, operation);
        processDatasetLinkAttribute(entity, vertex, operation);

        RequestContext.get().endMetricRecord(metricRecorder);

    }

    private void processDomainLinkAttribute(AtlasEntity entity, AtlasVertex vertex, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if(entity.hasAttribute(DOMAIN_GUIDS)){
            validateDomainAssetLinks(entity);
            isAuthorized(vertex, operation, entity);
        }
    }

    private void processDatasetLinkAttribute(AtlasEntity entity, AtlasVertex vertex, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (entity.hasAttribute(CATALOG_DATASET_GUID_ATTR)) {
            validateDatasetAssetLinks(entity);
            isAuthorized(vertex, operation, entity);
        }
    }

    private void validateDatasetAssetLinks(AtlasEntity entity) throws AtlasBaseException {
        String datasetGuid = (String) entity.getAttribute(CATALOG_DATASET_GUID_ATTR);

        if (StringUtils.isEmpty(datasetGuid)) {
            return;
        }

        validateLinkedEntity(entity, datasetGuid, DATASET_ENTITY_TYPE, "Dataset");
    }

    private void validateDomainAssetLinks(AtlasEntity entity) throws AtlasBaseException {
        List<String> domainGuids = (List<String>) entity.getAttribute(DOMAIN_GUIDS);

        if (CollectionUtils.isEmpty(domainGuids)) {
            return;
        }

        if (domainGuids.size() > 1) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Asset can be linked to only one domain");
        }

        for (String domainGuid : domainGuids) {
            validateLinkedEntity(entity, domainGuid, DATA_DOMAIN_ENTITY_TYPE, "Domain");
        }
    }

    private void validateLinkedEntity(AtlasEntity entity, String guid,
                                          String expectedEntityType, String label) throws AtlasBaseException {
        if(DATASET_ENTITY_TYPE.equals(expectedEntityType)) {
            if (excludedTypesForDataset.contains(entity.getTypeName())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                        entity.getTypeName() + " is not allowed to link with " + label, entity.getTypeName());
            }
        } else {
            if (excludedTypes.contains(entity.getTypeName())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                        entity.getTypeName() + " is not allowed to link with " + label, entity.getTypeName());
            }
        }

        AtlasVertex linkedVertex = entityRetriever.getEntityVertex(guid);
        if (linkedVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        String actualType = linkedVertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class);
        if (!Objects.equals(actualType, expectedEntityType)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                    "Asset can be linked to only a " + label + " entity");
        }
    }

    private void isAuthorized(AtlasVertex vertex, EntityMutations.EntityOperation operation, AtlasEntity entity) throws AtlasBaseException {
        AtlasEntityHeader sourceEntity;

        if (operation == EntityMutations.EntityOperation.CREATE) {
            sourceEntity = new AtlasEntityHeader(entity);
        } else {
            sourceEntity = retrieverNoRelation.toAtlasEntityHeaderWithClassifications(vertex);
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, sourceEntity),
                "update on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, sourceEntity),
                "read on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeleteAsset");

        try {
            DeleteType deleteType = RequestContext.get().getDeleteType();
            if (deleteType.equals(DeleteType.HARD) || deleteType.equals(DeleteType.PURGE)) {
                removeAssetGuidFromAttributeReferences(vertex, referencingEntityTypes, referenceAttributeNames);
            } else {
                LOG.info("processDeleteAsset: Skipping cleanup for soft delete of asset: {}", GraphHelper.getGuid(vertex));
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void removeAssetGuidFromAttributeReferences(AtlasVertex vertex, Set<String> referencingEntityTypes, Set<String> referenceAttributeNames) {
        try {
            if (isAssetType(vertex)) {
                String guid = GraphHelper.getGuid(vertex);
                int totalAttributeRefsRemoved = 0;

                if (CollectionUtils.isEmpty(referencingEntityTypes) || CollectionUtils.isEmpty(referenceAttributeNames)) {
                    LOG.warn("removeAssetGuidFromAttributeReferences: Empty entity types or attribute names set for asset: {}", guid);
                    return;
                }

                for (String entityType: referencingEntityTypes) {
                    int currentEntityRefcount = 0;

                    for (String attributeName: referenceAttributeNames) {
                        int currentAttributeRefcount = 0;

                        try {
                            List<AtlasVertex> entityVertices = fetchEntityVerticesUsingIndexSearch(entityType, attributeName, guid);

                            for (AtlasVertex entityVertex: entityVertices) {

                                AtlasGraphUtilsV2.removeItemFromListPropertyValue(
                                        entityVertex,
                                        attributeName,
                                        guid
                                );
                                currentAttributeRefcount += 1;
                                currentEntityRefcount += 1;
                                totalAttributeRefsRemoved += 1;
                            }
                        } catch (Exception e) {
                            LOG.error("removeAssetGuidFromAttributeReferences: failed to cleanup attribute reference for asset {} from individual entity", guid, e);
                        }

                        if (currentAttributeRefcount > 0) {
                            LOG.info("removeAssetGuidFromAttributeReferences: removed {} references for attribute {} in entity type {} for asset: {}", 
                                currentAttributeRefcount, attributeName, entityType, guid);
                        }
                    }

                    if (currentEntityRefcount > 0) {
                        LOG.info("removeAssetGuidFromAttributeReferences: removed {} total references for entity type {} for asset: {}", 
                            currentEntityRefcount, entityType, guid);
                    }
                }

                if (totalAttributeRefsRemoved > 0) {
                    LOG.info("removeAssetGuidFromAttributeReferences: successfully cleaned up {} total attribute references for asset: {}", 
                        totalAttributeRefsRemoved, guid);
                }
            }
        }
        catch (Exception e) {
            LOG.error("removeAssetGuidFromAttributeReferences: unexpected error during cleanup", e);
        }
    }

    private List<AtlasVertex> fetchEntityVerticesUsingIndexSearch(String typeName, String attributeName, String guid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("findProductsWithPortGuid");
        try {
            List<Map<String, Object>> mustClauses = new ArrayList<>();
            mustClauses.add(mapOf("term", mapOf("__typeName.keyword", typeName)));
            mustClauses.add(mapOf("term", mapOf(attributeName, guid)));

            Map<String, Object> bool = new HashMap<>();
            bool.put("must", mustClauses);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            return retrieveVerticesFromIndexSearchPaginated(dsl, null, discovery);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private boolean isAssetType(AtlasVertex vertex) {
        String typeName = GraphHelper.getTypeName(vertex);
        if (excludedTypes.contains(typeName)) {
            return false;
        }

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

        return entityType != null && entityType.getTypeAndAllSuperTypes().contains("Asset");
    }
}
