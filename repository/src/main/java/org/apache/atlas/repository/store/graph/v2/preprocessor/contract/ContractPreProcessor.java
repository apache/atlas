package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.sun.xml.bind.v2.TODO;
import joptsimple.internal.Strings;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.featureflag.FeatureFlagStore;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.resource.AbstractResourcePreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.resource.ReadmePreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.pkl.config.java.Config;
//import org.pkl.config.java.ConfigEvaluator;
//import org.pkl.core.ModuleSource;
//import org.pkl.config.java.JavaType;

//import org.pkl.codegen.java.JavaCodeGenerator;

import com.fasterxml.jackson.databind.ObjectMapper;


import java.util.*;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.repository.Constants.ASSET_README_EDGE_LABEL;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.*;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;

public class ContractPreProcessor extends AbstractContractPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ReadmePreProcessor.class);
    public static final String ATTR_CONTRACT  = "contract";
    public static final String ATTR_CERTIFICATE_STATUS  = "certificateStatus";
    private AtlasEntityStore entityStore;


    public ContractPreProcessor(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                EntityGraphRetriever entityRetriever, AtlasEntityStore entityStore) {

        super(graph, typeRegistry, entityRetriever);
        this.entityStore = entityStore;

    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        AtlasEntity entity = (AtlasEntity) entityStruct;
        switch (operation) {
            case CREATE:
                processCreateContract(entity, context);
                break;
            case UPDATE:
                processUpdateContract(entity, context);
                break;
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

        String contractString = (String) entity.getAttribute(ATTR_CONTRACT);
        if (StringUtils.isEmpty(contractString)) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_CONTRACT);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        DataContract contract;
        try {
            contract = objectMapper.readValue(contractString, DataContract.class);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new AtlasBaseException("Failed at this");
        }

        contractAttributeSync(entity, contract);
        String contractQName = (String) entity.getAttribute(QUALIFIED_NAME);
        validateAttribute(!contractQName.endsWith("/contract"), "Invalid qualifiedName for the contract.");

        String datasetQName = contractQName.substring(0, contractQName.lastIndexOf('/'));
        ContractVersionUtils versionUtil = new ContractVersionUtils(entity, context, entityRetriever, typeRegistry, entityStore, graph);
        AtlasEntity latestVersion = versionUtil.getLatestVersion();

//        List<AtlasVertex> vertexList = new ArrayList<>();
//        while (allContracts.hasNext()) {
//            AtlasVertex v = allContracts.next();
//            vertexList.add(v);
//        }
//        RequestContext.get().endMetricRecord(metric);

        if (latestVersion != null) {
            String qName = (String) latestVersion.getAttribute(QUALIFIED_NAME);
            Integer latestVersionNumber = Integer.valueOf(qName.substring(qName.lastIndexOf("/V")+2));

            entity.setAttribute(QUALIFIED_NAME, String.format("%s/version/V%s", contractQName, ++latestVersionNumber));

        } else {
            entity.setAttribute(QUALIFIED_NAME, String.format("%s/%s", contractQName, "version/V1"));

        }

        DataContract.Dataset dataset = contract.dataset;
        AtlasEntityWithExtInfo associatedAsset = getAssociatedAsset(datasetQName, dataset.type.name());

        if (contract.getStatus() == DataContract.STATUS.VERIFIED &&
                entity.getAttribute(ATTR_CERTIFICATE_STATUS).equals(DataContract.STATUS.VERIFIED.name())) {
            datasetAttributeSync(associatedAsset.getEntity(), contract);
        }


//        versionUtil.createNewVersion();

    }

    private void processUpdateContract(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        throw new AtlasBaseException("Can't update a specific version of contract");


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
            if (entity.getAttribute(ATTR_CERTIFICATE_STATUS) == DataContract.STATUS.VERIFIED.name()) {
                contract.setStatus(DataContract.STATUS.VERIFIED);
            } else if (contract.getStatus() == DataContract.STATUS.VERIFIED) {
                entity.setAttribute(ATTR_CERTIFICATE_STATUS, DataContract.STATUS.VERIFIED.name());
            } else {
                entity.setAttribute(ATTR_CERTIFICATE_STATUS, DataContract.STATUS.DRAFT);
                contract.setStatus(DataContract.STATUS.DRAFT);
            }

        }

    }

    private void datasetAttributeSync(AtlasEntity associatedAsset, DataContract contract) throws AtlasBaseException {

        DataContract.Dataset dataset = contract.dataset;
        // Will implement dataset attribute sync from the contract attributes

    }

    private AtlasEntityWithExtInfo getLatestContract(String datasetQName) throws AtlasBaseException {
        Map<String, Object> uniqAttributes = new HashMap<>();
        uniqAttributes.put(QUALIFIED_NAME, String.format("%s/%s", datasetQName, "contract/version/*"));
        AtlasEntityType entityType = ensureEntityType(Constants.CONTRACT_ENTITY_TYPE);

        AtlasVertex entityVertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(graph, entityType, uniqAttributes);

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(graph, typeRegistry, true);

        AtlasEntityWithExtInfo ret = entityRetriever.toAtlasEntityWithExtInfo(entityVertex);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, entityType.getTypeName(),
                    uniqAttributes.toString());
        }
        return ret;

    }
    private AtlasEntityWithExtInfo getAssociatedAsset(String qualifiedName, String typeName) throws AtlasBaseException {
        Map<String, Object> uniqAttributes = new HashMap<>();
        uniqAttributes.put(QUALIFIED_NAME, qualifiedName);

        AtlasEntityType entityType = ensureEntityType(typeName);

        AtlasVertex entityVertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(graph, entityType, uniqAttributes);

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(graph, typeRegistry, true);

        AtlasEntityWithExtInfo ret = entityRetriever.toAtlasEntityWithExtInfo(entityVertex);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, entityType.getTypeName(),
                    uniqAttributes.toString());
        }
        return ret;
    }

    private AtlasEntityType ensureEntityType(String typeName) throws AtlasBaseException {
        AtlasEntityType ret = typeRegistry.getEntityTypeByName(typeName);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), typeName);
        }

        return ret;
    }

    private static void validateAttribute(boolean isInvalid, String errorMessage) throws AtlasBaseException {
        if (isInvalid)
            throw new AtlasBaseException(BAD_REQUEST, errorMessage);
    }
}
