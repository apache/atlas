package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.*;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.*;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.indexSearchPaginated;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;
import static org.apache.atlas.type.AtlasTypeUtil.getAtlasObjectId;

public class ContractPreProcessor extends AbstractContractPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ContractPreProcessor.class);
    public static final String ATTR_ASSET_GUID = "dataContractAssetGuid";
    public static final String REL_ATTR_LATEST_CONTRACT = "dataContractLatest";
    public static final String REL_ATTR_GOVERNED_ASSET_CERTIFIED = "dataContractLatestCertified";
    public static final String REL_ATTR_PREVIOUS_VERSION = "dataContractPreviousVersion";
    public static final String ASSET_ATTR_HAS_CONTRACT = "hasContract";
    public static final String CONTRACT_QUALIFIED_NAME_SUFFIX = "contract";
    public static final String CONTRACT_ATTR_STATUS = "status";
    private static final String CONTRACT_DELETE_SCOPE_HEADER = "x-atlan-contract-delete-scope";
    private static final String CASCADE_DELETE_KEY_PREFIX = "contractCascadeDelete:";
    private static final Set<String> contractAttributes = new HashSet<>();
    static {
        contractAttributes.add(ATTR_CONTRACT);
        contractAttributes.add(ATTR_CONTRACT_JSON);
        contractAttributes.add(ATTR_CERTIFICATE_STATUS);
        contractAttributes.add(ATTR_CONTRACT_VERSION);
    }
    private final boolean storeDifferentialAudits;
    private final EntityDiscoveryService discovery;
    private final AtlasEntityStore entityStore;

    private final AtlasEntityComparator entityComparator;


    public ContractPreProcessor(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                EntityGraphRetriever entityRetriever,
                                boolean storeDifferentialAudits, EntityDiscoveryService discovery,
                                AtlasEntityStore entityStore) {

        super(graph, typeRegistry, entityRetriever, discovery);
        this.storeDifferentialAudits = storeDifferentialAudits;
        this.discovery = discovery;
        this.entityStore = entityStore;
        this.entityComparator = new AtlasEntityComparator(typeRegistry, entityRetriever, null, new BulkRequestContext());
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        AtlasEntity entity = (AtlasEntity) entityStruct;
        switch (operation) {
            case CREATE:
                processCreateContract(entity, context);
                break;
            case UPDATE:
                // Updating an existing version of the contract
                processUpdateContract(entity, context);
        }

    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeleteContract");

        try {
            DeleteType deleteType = RequestContext.get().getDeleteType();
            if (deleteType == DeleteType.SOFT || deleteType == DeleteType.DEFAULT) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED,
                        "Soft delete is not supported for DataContract. Use hard delete (PURGE) instead.");
            }

            if (!AtlasEntity.Status.ACTIVE.equals(GraphHelper.getStatus(vertex))) {
                LOG.info("Contract {} is already deleted/purged, skipping processDelete", GraphHelper.getGuid(vertex));
                return;
            }

            String contractGuid = GraphHelper.getGuid(vertex);
            String assetGuid    = vertex.getProperty(ATTR_ASSET_GUID, String.class);

            if (StringUtils.isEmpty(assetGuid)) {
                LOG.warn("Contract {} has no {} attribute, skipping asset cleanup", contractGuid, ATTR_ASSET_GUID);
                return;
            }

            String scope = RequestContext.get().getRequestContextHeaders()
                    .getOrDefault(CONTRACT_DELETE_SCOPE_HEADER, "all");

            if ("single".equalsIgnoreCase(scope)) {
                processSingleVersionDelete(contractGuid, assetGuid);
            } else {
                processDeleteAllVersions(contractGuid, assetGuid);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void processDeleteAllVersions(String contractGuid, String assetGuid) throws AtlasBaseException {
        String cascadeKey = CASCADE_DELETE_KEY_PREFIX + assetGuid;
        Map<String, String> headers = RequestContext.get().getRequestContextHeaders();
        if (headers.containsKey(cascadeKey)) {
            LOG.info("Contract {} is being cascade-deleted for asset {}, skipping", contractGuid, assetGuid);
            return;
        }
        headers.put(cascadeKey, "true");

        List<AtlasEntityHeader> allVersions = getAllVersions(assetGuid);
        List<String> siblingGuids = allVersions.stream()
                .map(AtlasEntityHeader::getGuid)
                .filter(guid -> !guid.equals(contractGuid))
                .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(siblingGuids)) {
            LOG.info("Cascade deleting {} sibling contract versions for asset {}", siblingGuids.size(), assetGuid);
            entityStore.deleteByIds(siblingGuids);
        }

        cleanupAssetAttributes(assetGuid);
        LOG.info("processDeleteAllVersions completed: contract={}, asset={}", contractGuid, assetGuid);
    }

    private void processSingleVersionDelete(String contractGuid, String assetGuid) throws AtlasBaseException {
        AtlasEntity latestVersion = getCurrentVersion(assetGuid);
        if (latestVersion == null || !latestVersion.getGuid().equals(contractGuid)) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED,
                    "Only the latest contract version can be deleted. Delete all versions or delete the latest first.");
        }

        AtlasEntity previousVersion = getSecondLatestVersion(assetGuid);

        if (previousVersion != null) {
            restoreAssetContractPointers(assetGuid, previousVersion);
            LOG.info("processSingleVersionDelete: contract={}, asset={}, previousVersion={}",
                    contractGuid, assetGuid, previousVersion.getGuid());
        } else {
            cleanupAssetAttributes(assetGuid);
            LOG.info("processSingleVersionDelete: last version deleted, contract={}, asset={}", contractGuid, assetGuid);
        }
    }

    private void restoreAssetContractPointers(String assetGuid, AtlasEntity previousVersion) throws AtlasBaseException {
        AtlasVertex assetVertex = AtlasGraphUtilsV2.findByGuid(assetGuid);
        if (assetVertex == null) {
            LOG.warn("Asset vertex not found for guid {}, cannot restore contract pointers", assetGuid);
            return;
        }

        String assetTypeName = GraphHelper.getTypeName(assetVertex);
        String assetQualifiedName = assetVertex.getProperty(QUALIFIED_NAME, String.class);

        AtlasEntity assetUpdate = new AtlasEntity(assetTypeName);
        assetUpdate.setGuid(assetGuid);
        assetUpdate.setAttribute(QUALIFIED_NAME, assetQualifiedName);

        assetUpdate.setRelationshipAttribute(REL_ATTR_LATEST_CONTRACT, getAtlasObjectId(previousVersion));

        String prevStatus = (String) previousVersion.getAttribute(ATTR_CERTIFICATE_STATUS);
        if (DataContract.Status.VERIFIED.name().equals(prevStatus)) {
            assetUpdate.setRelationshipAttribute(REL_ATTR_GOVERNED_ASSET_CERTIFIED, getAtlasObjectId(previousVersion));
        }

        EntityStream entityStream = new AtlasEntityStream(new AtlasEntity.AtlasEntitiesWithExtInfo(assetUpdate));

        boolean originalSkipAuthCheck = RequestContext.get().isSkipAuthorizationCheck();
        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            entityStore.createOrUpdate(entityStream, true);
            LOG.info("Restored contract pointers on asset {} to previous version {}", assetGuid, previousVersion.getGuid());
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(originalSkipAuthCheck);
        }
    }

    private List<AtlasEntityHeader> getAllVersions(String assetGuid) throws AtlasBaseException {
        Map<String, Object> dsl = new HashMap<>();

        List<Map<String, Object>> mustClauseList = new ArrayList<>();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", CONTRACT_ENTITY_TYPE)));
        mustClauseList.add(mapOf("term", mapOf(ATTR_ASSET_GUID, assetGuid)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        return indexSearchPaginated(dsl, contractAttributes, discovery);
    }

    private AtlasEntity getSecondLatestVersion(String assetGuid) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = new HashMap<>();

        List<Map<String, Object>> mustClauseList = new ArrayList<>();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", CONTRACT_ENTITY_TYPE)));
        mustClauseList.add(mapOf("term", mapOf(ATTR_ASSET_GUID, assetGuid)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));
        dsl.put("sort", Collections.singletonList(mapOf(ATTR_CONTRACT_VERSION, mapOf("order", "desc"))));
        dsl.put("size", 2);

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setAttributes(contractAttributes);
        indexSearchParams.setSuppressLogs(true);

        AtlasSearchResult result = discovery.directIndexSearch(indexSearchParams);
        if (result == null || CollectionUtils.isEmpty(result.getEntities()) || result.getEntities().size() < 2) {
            return null;
        }
        return new AtlasEntity(result.getEntities().get(1));
    }

    private void cleanupAssetAttributes(String assetGuid) {
        try {
            AtlasVertex assetVertex = AtlasGraphUtilsV2.findByGuid(assetGuid);
            if (assetVertex == null) {
                LOG.warn("Asset vertex not found for guid {}, skipping attribute cleanup", assetGuid);
                return;
            }

            AtlasGraphUtilsV2.setProperty(assetVertex, ASSET_ATTR_HAS_CONTRACT, false);
            LOG.info("Cleared hasContract on asset {}", assetGuid);
        } catch (Exception e) {
            LOG.error("Failed to cleanup asset attributes for asset {}", assetGuid, e);
        }
    }

    private void processUpdateContract(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        String contractString = getContractString(entity);
        AtlasVertex vertex = context.getVertex(entity.getGuid());
        AtlasEntity existingContractEntity = entityRetriever.toAtlasEntity(vertex);
        // No update to relationships allowed for the existing contract version
        resetAllRelationshipAttributes(entity);
        if (existingContractEntity.getAttribute(ATTR_CERTIFICATE_STATUS) == DataContract.Status.VERIFIED.name()) {
            // Update the same asset(entity)
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can't update published version of contract.");
        }
    }
    private void processCreateContract(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        /*
          Low-level Design
               | Authorization
               | Deserialization of the JSON
               ---| Validation of spec
               | Validation of contract
               | Create Version
               | Create Draft
               ---| asset to contract sync
               | Create Publish
               ---| two-way sync of attribute
         */

        String contractQName = (String) entity.getAttribute(QUALIFIED_NAME);
        validateAttribute(!contractQName.endsWith(String.format("/%s", CONTRACT_QUALIFIED_NAME_SUFFIX)), "Invalid qualifiedName for the contract.");

        String contractString = getContractString(entity);
        DataContract contract = DataContract.deserialize(contractString);
        String datasetQName = contractQName.substring(0, contractQName.lastIndexOf('/'));
        AtlasEntity associatedAsset = getAssociatedAsset(datasetQName, contract);
        contractQName = String.format("%s/%s/%s", datasetQName, associatedAsset.getTypeName(), CONTRACT_QUALIFIED_NAME_SUFFIX);

        authorizeContractCreateOrUpdate(entity, associatedAsset);

        boolean contractSync = syncContractCertificateStatus(entity, contract);
        if (!isContractYaml(entity)) {
            contractString = DataContract.serialize(contract);
            entity.setAttribute(ATTR_CONTRACT, contractString);
        }
        String contractStringJSON = DataContract.serializeJSON(contract);
        entity.setAttribute(ATTR_CONTRACT_JSON, contractStringJSON);

        AtlasEntity currentVersionEntity = getCurrentVersion(associatedAsset.getGuid());
        Long newVersionNumber = 1L;
        if (currentVersionEntity == null && contract.getStatus() == DataContract.Status.VERIFIED) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can't create a new published version");
        }
        if (currentVersionEntity != null) {
            // Contract already exist
            Long currentVersionNumber = (Long) currentVersionEntity.getAttribute(ATTR_CONTRACT_VERSION);
            List<String> attributes = getDiffAttributes(entity, currentVersionEntity);
            if (attributes.isEmpty()) {
                // No changes in the contract, Not creating new version
                removeCreatingVertex(context, entity);
                return;
            } else if (!currentVersionEntity.getAttribute(ATTR_CERTIFICATE_STATUS).equals(DataContract.Status.VERIFIED.name())) {
                resetAllRelationshipAttributes(entity);
                // Contract is in draft state. Update the same version
                updateExistingVersion(context, entity, currentVersionEntity);
                newVersionNumber = currentVersionNumber;
            } else {
                // Current version is published. Creating a new draft version.
                if (contract.getStatus() == DataContract.Status.VERIFIED) {
                    throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can't create a new published version");
                }
                newVersionNumber =  currentVersionNumber + 1;

                resetAllRelationshipAttributes(entity);
                // Attach previous version via rel
                entity.setRelationshipAttribute(REL_ATTR_PREVIOUS_VERSION, getAtlasObjectId(currentVersionEntity));
                AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(currentVersionEntity.getGuid());
                AtlasEntityType entityType = ensureEntityType(currentVersionEntity.getTypeName());
                context.addUpdated(currentVersionEntity.getGuid(), currentVersionEntity, entityType, vertex);

            }
        }
        entity.setAttribute(QUALIFIED_NAME, String.format("%s/V%s", contractQName, newVersionNumber));
        entity.setAttribute(ATTR_CONTRACT_VERSION, newVersionNumber);
        entity.setAttribute(ATTR_ASSET_GUID, associatedAsset.getGuid());

        datasetAttributeSync(context, associatedAsset, entity);

    }

    private List<String> getDiffAttributes(AtlasEntity entity, AtlasEntity latestExistingVersion) throws AtlasBaseException {
        AtlasEntityComparator.AtlasEntityDiffResult diffResult = entityComparator.getDiffResult(entity, latestExistingVersion, false);
        List<String> attributesSet = new ArrayList<>();

        if (diffResult.hasDifference()) {
            for (Map.Entry<String, Object> entry : diffResult.getDiffEntity().getAttributes().entrySet()) {
                if (!entry.getKey().equals(QUALIFIED_NAME)) {
                    attributesSet.add(entry.getKey());
                }
            }
        }
        return attributesSet;
    }

    private void updateExistingVersion(EntityMutationContext context, AtlasEntity entity, AtlasEntity currentVersionEntity) throws AtlasBaseException {
        removeCreatingVertex(context, entity);
        entity.setAttribute(QUALIFIED_NAME, currentVersionEntity.getAttribute(QUALIFIED_NAME));
        entity.setGuid(currentVersionEntity.getGuid());
        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entity.getGuid());
        AtlasEntityType entityType = ensureEntityType(entity.getTypeName());

        context.addUpdated(entity.getGuid(), entity, entityType, vertex);
        recordEntityMutatedDetails(context, entity, vertex);

    }

    public AtlasEntity getCurrentVersion(String datasetGuid) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = new HashMap<>();
        int size = 1;

        List<Map<String, Object>> mustClauseList = new ArrayList<>();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", CONTRACT_ENTITY_TYPE)));
        mustClauseList.add(mapOf("term", mapOf(ATTR_ASSET_GUID, datasetGuid)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));
        dsl.put("sort", Collections.singletonList(mapOf(ATTR_CONTRACT_VERSION, mapOf("order", "desc"))));
        dsl.put("size", size);

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setAttributes(contractAttributes);
        indexSearchParams.setSuppressLogs(true);

        AtlasSearchResult result = discovery.directIndexSearch(indexSearchParams);
        if (result == null || CollectionUtils.isEmpty(result.getEntities())) {
            return null;
        }
        return new AtlasEntity(result.getEntities().get(0));
    }

    private void removeCreatingVertex(EntityMutationContext context, AtlasEntity entity) {
        context.getCreatedEntities().remove(entity);
        graph.removeVertex(context.getVertex(entity.getGuid()));
    }

    private void resetAllRelationshipAttributes(AtlasEntity entity) {
        if (entity.getRemoveRelationshipAttributes() != null) {
            entity.setRemoveRelationshipAttributes(null);
        }
        if (entity.getAppendRelationshipAttributes() != null) {
            entity.setAppendRelationshipAttributes(null);
        }
        if (entity.getRelationshipAttributes() != null) {
            entity.setRelationshipAttributes(null);
        }
    }

    private boolean syncContractCertificateStatus(AtlasEntity entity, DataContract contract) throws AtlasBaseException {
        boolean contractSync = false;
        // Sync certificateStatus
        if (!Objects.equals(entity.getAttribute(ATTR_CERTIFICATE_STATUS), contract.getStatus().name())) {
            /*
            CertificateStatus    |    Status      |     Result
               DRAFT                  VERIFIED       cert -> VERIFIED  >
               VERIFIED               DRAFT          stat -> VERIFIED  >
                 -                    DRAFT          cert -> DRAFT
                 -                    VERIFIED       cert -> VERIFIED  >
               DRAFT                    -            stat -> DRAFT
               VERIFIED                 -            stat -> VERIFIED  >

             */
            if (Objects.equals(entity.getAttribute(ATTR_CERTIFICATE_STATUS), DataContract.Status.VERIFIED.name())) {
                contract.setStatus(String.valueOf(DataContract.Status.VERIFIED));
                contractSync = true;
            } else if (Objects.equals(contract.getStatus(), DataContract.Status.VERIFIED)) {
                entity.setAttribute(ATTR_CERTIFICATE_STATUS, DataContract.Status.VERIFIED.name());
            } else {
                entity.setAttribute(ATTR_CERTIFICATE_STATUS, DataContract.Status.DRAFT);
                contract.setStatus(String.valueOf(DataContract.Status.DRAFT));
                contractSync = true;
            }

        }
        return contractSync;

    }

    private void datasetAttributeSync(EntityMutationContext context, AtlasEntity associatedAsset, AtlasEntity contractAsset) throws AtlasBaseException {
        // Creating new empty AtlasEntity to update with selective attributes only
        AtlasEntity entity = new AtlasEntity(associatedAsset.getTypeName());
        entity.setGuid(associatedAsset.getGuid());
        entity.setAttribute(QUALIFIED_NAME, associatedAsset.getAttribute(QUALIFIED_NAME));
        if (associatedAsset.getAttribute(ASSET_ATTR_HAS_CONTRACT) == null || associatedAsset.getAttribute(ASSET_ATTR_HAS_CONTRACT).equals(false)) {
            entity.setAttribute(ASSET_ATTR_HAS_CONTRACT, true);
        }

        // Update relationship with contract
        entity.setRelationshipAttribute(REL_ATTR_LATEST_CONTRACT, getAtlasObjectId(contractAsset));
        if (Objects.equals(contractAsset.getAttribute(ATTR_CERTIFICATE_STATUS), DataContract.Status.VERIFIED.name()) ) {
            entity.setRelationshipAttribute(REL_ATTR_GOVERNED_ASSET_CERTIFIED, getAtlasObjectId(contractAsset));
        }

        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entity.getGuid());
        AtlasEntityType entityType = ensureEntityType(entity.getTypeName());
        context.addUpdated(entity.getGuid(), entity, entityType, vertex);
        recordEntityMutatedDetails(context, entity, vertex);
    }

    private void recordEntityMutatedDetails(EntityMutationContext context, AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntityComparator entityComparator = new AtlasEntityComparator(typeRegistry, entityRetriever, context.getGuidAssignments(), new BulkRequestContext());
        AtlasEntityComparator.AtlasEntityDiffResult diffResult   = entityComparator.getDiffResult(entity, vertex, !storeDifferentialAudits);
        RequestContext        reqContext           = RequestContext.get();
        if (diffResult.hasDifference()) {
            if (storeDifferentialAudits) {
                diffResult.getDiffEntity().setGuid(entity.getGuid());
                reqContext.cacheDifferentialEntity(diffResult.getDiffEntity());
            }
        }
    }

    private static void validateAttribute(boolean isInvalid, String errorMessage) throws AtlasBaseException {
        if (isInvalid)
            throw new AtlasBaseException(BAD_REQUEST, errorMessage);
    }

    private static String getContractString(AtlasEntity entity) {
        String contractString = (String) entity.getAttribute(ATTR_CONTRACT);
        if (StringUtils.isEmpty(contractString)) {
            contractString = (String) entity.getAttribute(ATTR_CONTRACT_JSON);
        }
        return contractString;
    }

    private static boolean isContractYaml(AtlasEntity entity) {
        return !StringUtils.isEmpty((String) entity.getAttribute(ATTR_CONTRACT));
    }
}
