package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.*;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

import static org.apache.atlas.AtlasErrorCode.*;
import static org.apache.atlas.repository.Constants.ATTR_CERTIFICATE_STATUS;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class ContractPreProcessor extends AbstractContractPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ContractPreProcessor.class);
    public static final String ATTR_CONTRACT = "contract";
    public static final String ATTR_VERSION = "contractVersion";
    public static final String ATTR_ASSET_QUALIFIED_NAME = "contractAssetQualifiedName";
    public static final String ATTR_PARENT_GUID = "parentGuid";
    public static final String ATTR_HAS_CONTRACT = "hasContract";
    public static final String CONTRACT_QUALIFIED_NAME_SUFFIX = "contract";
    public static final String VERSION_PREFIX = "version";
    public static final String CONTRACT_ATTR_STATUS = "status";
    private final AtlasEntityStore entityStore;
    private final EntityGraphMapper entityGraphMapper;


    public ContractPreProcessor(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                EntityGraphRetriever entityRetriever, AtlasEntityStore entityStore, EntityGraphMapper entityGraphMapper) {

        super(graph, typeRegistry, entityRetriever);
        this.entityStore = entityStore;
        this.entityGraphMapper = entityGraphMapper;
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

    private void processUpdateContract(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        String contractString = (String) entity.getAttribute(ATTR_CONTRACT);
        AtlasVertex vertex = context.getVertex(entity.getGuid());
        AtlasEntity existingContractEntity = entityRetriever.toAtlasEntity(vertex);

        if (!isEqualContract(contractString, (String) existingContractEntity.getAttribute(ATTR_CONTRACT))) {
            // Update the same asset(entity)
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can't update a specific version of contract");
        }
        // Add cases for update in status field and certificateStatus
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

        String contractString = (String) entity.getAttribute(ATTR_CONTRACT);
        DataContract contract = DataContract.deserialize(contractString);
        DataContract.Dataset dataset = contract.dataset;
        AtlasEntityWithExtInfo associatedAsset = getAssociatedAsset(contractQName, dataset.type.name());

        authorizeContractCreateOrUpdate(entity, associatedAsset);

        contractAttributeSync(entity, contract);
        contractString = DataContract.serialize(contract);
        entity.setAttribute(ATTR_CONTRACT, contractString);


        ContractVersionUtils versionUtil = new ContractVersionUtils(entity, context, entityRetriever, typeRegistry, entityStore, graph);
        AtlasEntity latestExistingVersion = versionUtil.getLatestVersion();

        if (latestExistingVersion != null) {
            // Contract already exist
            String qName = (String) latestExistingVersion.getAttribute(QUALIFIED_NAME);
            Integer latestVersionNumber = Integer.valueOf(qName.substring(qName.lastIndexOf("/V") + 2));
            List<String> attributes = getDiffAttributes(context, entity, latestExistingVersion);
            if (attributes.isEmpty()) {
                removeCreatingVertex(context, entity);
                return;
            }

            if (attributes.size() == 1 && attributes.contains(ATTR_CERTIFICATE_STATUS)) {
                if (Objects.equals(entity.getAttribute(ATTR_CERTIFICATE_STATUS).toString(), DataContract.STATUS.VERIFIED.name())) {
                    //update existing entity
                    updateExistingVersion(context, entity, latestExistingVersion);
                }
                // Contract status changed, either to draft or verified

            } else if (attributes.contains(ATTR_CONTRACT)) {
                //Contract is changed
                if (isEqualContract(contractString, (String) latestExistingVersion.getAttribute(ATTR_CONTRACT))) {
                    // Update the same asset(entity)
                    updateExistingVersion(context, entity, latestExistingVersion);

                } else {
                    // Create New version of entity
                    entity.setAttribute(QUALIFIED_NAME, String.format("%s/%s/V%s", contractQName, VERSION_PREFIX, ++latestVersionNumber));
                    entity.setAttribute(ATTR_VERSION, String.format("V%s", latestVersionNumber));
                    entity.setAttribute(ATTR_ASSET_QUALIFIED_NAME, associatedAsset.getEntity().getAttribute(QUALIFIED_NAME));
                    entity.setAttribute(ATTR_PARENT_GUID, latestExistingVersion.getGuid());

                }
            }

        } else {
            // Create new contract
            entity.setAttribute(QUALIFIED_NAME, String.format("%s/%s/%s", contractQName, VERSION_PREFIX, "V1"));
            entity.setAttribute(ATTR_VERSION, "V1");
            entity.setAttribute(ATTR_ASSET_QUALIFIED_NAME, associatedAsset.getEntity().getAttribute(QUALIFIED_NAME));

        }
        datasetAttributeSync(associatedAsset.getEntity(), contract, entity);

    }

    private List<String> getDiffAttributes(EntityMutationContext context, AtlasEntity entity, AtlasEntity latestExistingVersion) throws AtlasBaseException {
        AtlasEntityComparator entityComparator = new AtlasEntityComparator(typeRegistry, entityRetriever, context.getGuidAssignments(), true, true);
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

    private boolean isEqualContract(String firstNode, String secondNode) throws AtlasBaseException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode actualObj1 = mapper.readTree(firstNode);
            JsonNode actualObj2 = mapper.readTree(secondNode);
            //Ignore status field change
            ((ObjectNode) actualObj1).remove(CONTRACT_ATTR_STATUS);
            ((ObjectNode) actualObj2).remove(CONTRACT_ATTR_STATUS);

            return actualObj1.equals(actualObj2);
        } catch (JsonProcessingException e) {
            throw new AtlasBaseException(JSON_ERROR, e.getMessage());
        }

    }

    private void updateExistingVersion(EntityMutationContext context, AtlasEntity entity, AtlasEntity latestExistingVersion) throws AtlasBaseException {
        removeCreatingVertex(context, entity);
        entity.setAttribute(QUALIFIED_NAME, latestExistingVersion.getAttribute(QUALIFIED_NAME));
        entity.setGuid(latestExistingVersion.getGuid());

        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entity.getGuid());

        AtlasEntityType entityType = ensureEntityType(entity.getTypeName());

        context.addUpdated(entity.getGuid(), entity, entityType, vertex);

    }

    private void removeCreatingVertex(EntityMutationContext context, AtlasEntity entity) throws AtlasBaseException {
        context.getCreatedEntities().remove(entity);
        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            Set<String> guids = new HashSet<>();
            guids.add(entity.getGuid());
            entityStore.purgeByIds(guids);
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(false);
        }

    }

    private void contractAttributeSync(AtlasEntity entity, DataContract contract) {
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
            if (Objects.equals(entity.getAttribute(ATTR_CERTIFICATE_STATUS), DataContract.STATUS.VERIFIED.name())) {
                contract.setStatus(DataContract.STATUS.VERIFIED);
            } else if (Objects.equals(contract.getStatus(), DataContract.STATUS.VERIFIED)) {
                entity.setAttribute(ATTR_CERTIFICATE_STATUS, DataContract.STATUS.VERIFIED.name());
            } else {
                entity.setAttribute(ATTR_CERTIFICATE_STATUS, DataContract.STATUS.DRAFT);
                contract.setStatus(DataContract.STATUS.DRAFT);
            }

        }

    }

    private void datasetAttributeSync(AtlasEntity associatedAsset, DataContract contract, AtlasEntity contractAsset) throws AtlasBaseException {
        associatedAsset.setAttribute(ATTR_HAS_CONTRACT, true);
        if (contract.getStatus() == DataContract.STATUS.VERIFIED &&
                contractAsset.getAttribute(ATTR_CERTIFICATE_STATUS).equals(DataContract.STATUS.VERIFIED.name())) {
            DataContract.Dataset dataset = contract.dataset;
            // Will implement dataset attribute sync from the contract attributes
        }
        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            EntityStream entityStream = new AtlasEntityStream(associatedAsset);
            entityStore.createOrUpdate(entityStream, false);
            LOG.info("Updated associated asset attributes of contract {}", associatedAsset.getAttribute(QUALIFIED_NAME));
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(false);
        }
    }

    private static void validateAttribute(boolean isInvalid, String errorMessage) throws AtlasBaseException {
        if (isInvalid)
            throw new AtlasBaseException(BAD_REQUEST, errorMessage);
    }
}
