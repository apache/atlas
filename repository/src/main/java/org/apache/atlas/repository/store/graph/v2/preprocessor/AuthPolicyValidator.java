package org.apache.atlas.repository.store.graph.v2.preprocessor;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.JsonToElasticsearchQuery;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_ADD_CLASSIFICATION;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_CREATE;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_DELETE;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_READ;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_READ_CLASSIFICATION;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_REMOVE_CLASSIFICATION;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_UPDATE;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_UPDATE_CLASSIFICATION;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.repository.Constants.CONNECTION_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.*;

public class AuthPolicyValidator {
    private final EntityGraphRetriever entityRetriever;
    
    AuthPolicyValidator(EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
    }

    private static final Set<String> PERSONA_POLICY_VALID_SUB_CATEGORIES = new HashSet<String>(){{
        add(POLICY_SUB_CATEGORY_METADATA);
        add(POLICY_SUB_CATEGORY_DATA);
        add(POLICY_SUB_CATEGORY_GLOSSARY);
        add(POLICY_SUB_CATEGORY_AI);
    }};

    private static final Set<String> PURPOSE_POLICY_VALID_SUB_CATEGORIES = new HashSet<String>(){{
        add(POLICY_SUB_CATEGORY_METADATA);
        add(POLICY_SUB_CATEGORY_DATA);
    }};

    private static final Set<String> DATAMESH_POLICY_VALID_SUB_CATEGORIES = new HashSet<String>(){{
        add(POLICY_SUB_CATEGORY_PRODUCT);
    }};

    private static final Set<String> PERSONA_METADATA_POLICY_ACTIONS = new HashSet<String>(){{
        add("persona-asset-read");
        add("persona-asset-update");
        add("persona-api-create");
        add("persona-api-delete");
        add("persona-business-update-metadata");
        add("persona-entity-add-classification");
        add("persona-entity-update-classification");
        add("persona-entity-remove-classification");
        add("persona-add-terms");
        add("persona-remove-terms");
        add("persona-dq-update");
        add("persona-dq-read");
        add("persona-dq-create");
        add("persona-dq-delete");
    }};

    private static final Set<String> DATA_POLICY_ACTIONS = new HashSet<String>(){{
        add("select");
    }};

    private static final Set<String> PERSONA_GLOSSARY_POLICY_ACTIONS = new HashSet<String>(){{
        add("persona-glossary-read");
        add("persona-glossary-update");
        add("persona-glossary-create");
        add("persona-glossary-delete");
        add("persona-glossary-update-custom-metadata");
        add("persona-glossary-add-classifications");
        add("persona-glossary-update-classifications");
        add("persona-glossary-delete-classifications");
    }};

    private static final Set<String> AI_POLICY_ACTIONS = new HashSet<String>(){{
        add("persona-ai-application-read");
        add("persona-ai-application-create");
        add("persona-ai-application-update");
        add("persona-ai-application-delete");
        add("persona-ai-application-business-update-metadata");
        add("persona-ai-application-add-terms");
        add("persona-ai-application-remove-terms");
        add("persona-ai-application-add-classification");
        add("persona-ai-application-remove-classification"); 

        add("persona-ai-model-read");
        add("persona-ai-model-create");
        add("persona-ai-model-update");
        add("persona-ai-model-delete");
        add("persona-ai-model-business-update-metadata");
        add("persona-ai-model-add-terms");
        add("persona-ai-model-remove-terms");
        add("persona-ai-model-add-classification");
        add("persona-ai-model-remove-classification"); 
    }};

    private static final Set<String> PERSONA_METADATA_ABAC_POLICY_ACTIONS = new HashSet<>(){{
        add("persona-asset-read");
        add("persona-asset-update");
        add("persona-api-create");
        add("persona-api-delete");
        add("persona-business-update-metadata");
        add("persona-entity-add-classification");
        add("persona-entity-update-classification");
        add("persona-entity-remove-classification");
        add("persona-add-terms");
        add("persona-remove-terms");
    }};

    private static final Map<String, Set<String>> PERSONA_POLICY_VALID_ACTIONS = new HashMap<String, Set<String>>(){{
        put(POLICY_SERVICE_NAME_ATLAS + POLICY_SUB_CATEGORY_METADATA, PERSONA_METADATA_POLICY_ACTIONS);
        put(POLICY_SERVICE_NAME_ATLAS + POLICY_SUB_CATEGORY_DATA, DATA_POLICY_ACTIONS);
        put(POLICY_SERVICE_NAME_ATLAS + POLICY_SUB_CATEGORY_GLOSSARY, PERSONA_GLOSSARY_POLICY_ACTIONS);
        put(POLICY_SERVICE_NAME_ATLAS + POLICY_SUB_CATEGORY_AI, AI_POLICY_ACTIONS);

        put(POLICY_SERVICE_NAME_HEKA + POLICY_SUB_CATEGORY_DATA, DATA_POLICY_ACTIONS);
        put(POLICY_SERVICE_NAME_ABAC + POLICY_SUB_CATEGORY_METADATA, PERSONA_METADATA_ABAC_POLICY_ACTIONS);
    }};

    private static final Set<String> PURPOSE_METADATA_POLICY_ACTIONS = new HashSet<String>(){{
        add(ENTITY_READ.getType());
        add(ENTITY_CREATE.getType());
        add(ENTITY_UPDATE.getType());
        add(ENTITY_DELETE.getType());
        add(ENTITY_READ_CLASSIFICATION.getType());
        add(ENTITY_ADD_CLASSIFICATION.getType());
        add(ENTITY_UPDATE_CLASSIFICATION.getType());
        add(ENTITY_REMOVE_CLASSIFICATION.getType());
        add(ENTITY_UPDATE_BUSINESS_METADATA.getType());
        add("purpose-add-terms");
        add("purpose-remove-terms");
    }};

    private static final Map<String, Set<String>> PURPOSE_POLICY_VALID_ACTIONS = new HashMap<String, Set<String>>(){{
        put(POLICY_SUB_CATEGORY_METADATA, PURPOSE_METADATA_POLICY_ACTIONS);
        put(POLICY_SUB_CATEGORY_DATA, DATA_POLICY_ACTIONS);
    }};

    private static final Set<String> DATAMESH_POLICY_ACTIONS = new HashSet<String>(){{
        add(ENTITY_READ.getType());
    }};

    private static final Map<String, Set<String>> DATAMESH_POLICY_VALID_ACTIONS = new HashMap<String, Set<String>>(){{
        put(POLICY_SUB_CATEGORY_PRODUCT, DATAMESH_POLICY_ACTIONS);
    }};

    private static final Set<String> PERSONA_POLICY_VALID_RESOURCE_KEYS = new HashSet<String>() {{
        add("entity");
        add("entity-type");
    }};

    private static final String INVALID_FILTER_CRITERIA = "Invalid filter criteria: ";
    private static final int FILTER_CRITERIA_MAX_NESTING_LEVEL = 2;
    private static final Set<String> POLICY_SERVICE_ALLOWED_UPDATE = new HashSet<>(){{
        add(POLICY_SERVICE_NAME_ATLAS);
        add(POLICY_SERVICE_NAME_ABAC);
    }};

    public void validate(AtlasEntity policy, AtlasEntity existingPolicy,
                         AtlasEntity accessControl, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        String policyCategory = null;

        if (policy.hasAttribute(ATTR_POLICY_CATEGORY)) {
            policyCategory = getPolicyCategory(policy);
        } else if (existingPolicy != null) {
            policyCategory = getPolicyCategory(existingPolicy);
        }

        if (POLICY_CATEGORY_PERSONA.equals(policyCategory) || POLICY_CATEGORY_PURPOSE.equals(policyCategory) || POLICY_CATEGORY_DATAMESH.equals(policyCategory)) {

            if (operation == CREATE) {
                String policySubCategory = getPolicySubCategory(policy);
                List<String> policyActions = getPolicyActions(policy);

                validateParam(StringUtils.isEmpty(getPolicyServiceName(policy)), "Please provide attribute " + ATTR_POLICY_SERVICE_NAME);

                validateParam(StringUtils.isEmpty(getPolicyType(policy)), "Please provide attribute " + ATTR_POLICY_TYPE);

                validateParam(CollectionUtils.isEmpty(policyActions), "Please provide attribute " + ATTR_POLICY_ACTIONS);


                if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
                    validateOperation(!AtlasEntity.Status.ACTIVE.equals(accessControl.getStatus()), accessControl.getTypeName() + " is not Active");

                    validateParam(!PERSONA_ENTITY_TYPE.equals(accessControl.getTypeName()), "Please provide Persona as accesscontrol");

                    validateParam(!PERSONA_POLICY_VALID_SUB_CATEGORIES.contains(policySubCategory),
                            "Please provide valid value for attribute " + ATTR_POLICY_SUB_CATEGORY + ":" + PERSONA_POLICY_VALID_SUB_CATEGORIES);


                    List<String> resources = getPolicyResources(policy);
                    if (isABACPolicyService(policy)) {
                        validatePolicyFilterCriteria(getPolicyFilterCriteria(policy));
                    } else {
                        validateParam(CollectionUtils.isEmpty(resources), "Please provide attribute " + ATTR_POLICY_RESOURCES);
                    }

                    //validate persona policy resources keys
                    Set<String> policyResourceKeys = resources.stream().map(x -> x.split(RESOURCES_SPLITTER)[0]).collect(Collectors.toSet());
                    policyResourceKeys.removeAll(PERSONA_POLICY_VALID_RESOURCE_KEYS);
                    validateParam(CollectionUtils.isNotEmpty(policyResourceKeys),
                            "Please provide valid type of policy resources: " + PERSONA_POLICY_VALID_RESOURCE_KEYS);


                    if (POLICY_SUB_CATEGORY_DATA.equals(policySubCategory)) {
                        validateParam(!POLICY_RESOURCE_CATEGORY_PERSONA_ENTITY.equals(getPolicyResourceCategory(policy)), "Invalid resource category for Persona");
                        validateConnection(getPolicyConnectionQN(policy), resources);

                    } else if (POLICY_SUB_CATEGORY_METADATA.equals(policySubCategory)) {
                        validateParam(!POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM.equals(getPolicyResourceCategory(policy)), "Invalid resource category for Persona");
                        if (!isABACPolicyService(policy)) {
                            validateConnection(getPolicyConnectionQN(policy), resources);
                        }

                    } else if (POLICY_SUB_CATEGORY_GLOSSARY.equals(policySubCategory)) {

                        validateParam(!POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM.equals(getPolicyResourceCategory(policy)), "Invalid resource category for Persona");
                    }

                    //validate persona policy actions
                    Set<String> validActions = PERSONA_POLICY_VALID_ACTIONS.get(getPolicyServiceName(policy) + policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam(CollectionUtils.isNotEmpty(copyOfActions),
                            "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS + ": Invalid actions " + copyOfActions);
                }

                if (POLICY_CATEGORY_PURPOSE.equals(policyCategory)) {
                    validateOperation (!AtlasEntity.Status.ACTIVE.equals(accessControl.getStatus()), accessControl.getTypeName() + " is not Active");

                    validateParam (!PURPOSE_ENTITY_TYPE.equals(accessControl.getTypeName()),  "Please provide Purpose as accesscontrol");

                    validateParam (!PURPOSE_POLICY_VALID_SUB_CATEGORIES.contains(policySubCategory),
                            "Please provide valid value for attribute " + ATTR_POLICY_SUB_CATEGORY + ":"+ PURPOSE_POLICY_VALID_SUB_CATEGORIES);

                    validateParam (!POLICY_RESOURCE_CATEGORY_PURPOSE.equals(getPolicyResourceCategory(policy)), "Invalid resource category for Purpose");

                    //validateParam (CollectionUtils.isNotEmpty(getPolicyResources(policy)), "Provided unexpected attribute " + ATTR_POLICY_RESOURCES);

                    //validate purpose policy actions
                    Set<String> validActions = PURPOSE_POLICY_VALID_ACTIONS.get(policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam (CollectionUtils.isNotEmpty(copyOfActions),
                            "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS + ": Invalid actions "+ copyOfActions);
                }

                if (POLICY_CATEGORY_DATAMESH.equals(policyCategory)) {
                    validateParam (!DATAMESH_POLICY_VALID_SUB_CATEGORIES.contains(policySubCategory),
                            "Please provide valid value for attribute " + ATTR_POLICY_SUB_CATEGORY + ":"+ DATAMESH_POLICY_VALID_SUB_CATEGORIES);

                    //validate datamesh policy actions
                    Set<String> validActions = DATAMESH_POLICY_VALID_ACTIONS.get(policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam (CollectionUtils.isNotEmpty(copyOfActions),
                            "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS + ": Invalid actions "+ copyOfActions);
                }

            } else {

                validateOperation (StringUtils.isNotEmpty(policyCategory) && !policyCategory.equals(getPolicyCategory(existingPolicy)),
                        ATTR_POLICY_CATEGORY + " change not Allowed");

                validateParam(policy.hasAttribute(ATTR_POLICY_ACTIONS) && CollectionUtils.isEmpty(getPolicyActions(policy)),
                        "Please provide attribute " + ATTR_POLICY_ACTIONS);

                // allow service name update only for interchanging atlas and abac, not heka
                String newServiceName = getPolicyServiceName(policy);
                if (StringUtils.isNotEmpty(newServiceName) && !newServiceName.equals(getPolicyServiceName(existingPolicy))) {
                    String existingServiceName = getPolicyServiceName(existingPolicy);
                    validateOperation (!POLICY_SERVICE_ALLOWED_UPDATE.contains(newServiceName) || !POLICY_SERVICE_ALLOWED_UPDATE.contains(existingServiceName),
                        ATTR_POLICY_SERVICE_NAME + " change not Allowed");
                }

                String newResourceCategory = getPolicyResourceCategory(policy);
                validateOperation (StringUtils.isNotEmpty(newResourceCategory) && !newResourceCategory.equals(getPolicyResourceCategory(existingPolicy)),
                        ATTR_POLICY_RESOURCES_CATEGORY + " change not Allowed");

                validateOperation (!AtlasEntity.Status.ACTIVE.equals(existingPolicy.getStatus()), "Policy is not Active");

                if (policy.hasAttribute(ATTR_POLICY_SUB_CATEGORY)) {
                    String newSubCategory = getPolicySubCategory(policy);
                    validateOperation (!getPolicySubCategory(existingPolicy).equals(newSubCategory), "Policy sub category change not Allowed");
                }

                List<String> policyActions = policy.hasAttribute(ATTR_POLICY_ACTIONS) ? getPolicyActions(policy) : getPolicyActions(existingPolicy);
                String policySubCategory = policy.hasAttribute(ATTR_POLICY_SUB_CATEGORY) ? getPolicySubCategory(policy) : getPolicySubCategory(existingPolicy);

                if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
                    List<String> resources = getPolicyResources(policy);
                    if (isABACPolicyService(policy)) {
                        validatePolicyFilterCriteria(getPolicyFilterCriteria(policy));
                    } else {
                        validateParam (CollectionUtils.isEmpty(resources),
                            "Please provide attribute " + ATTR_POLICY_RESOURCES);
                    }

                    //validate persona policy resources keys
                    Set<String> policyResourceKeys = resources.stream().map(x -> x.split(RESOURCES_SPLITTER)[0]).collect(Collectors.toSet());
                    policyResourceKeys.removeAll(PERSONA_POLICY_VALID_RESOURCE_KEYS);
                    validateParam(CollectionUtils.isNotEmpty(policyResourceKeys),
                            "Please provide valid policy resources" + PERSONA_POLICY_VALID_RESOURCE_KEYS);

                    String newConnectionQn = getPolicyConnectionQN(policy);
                    if (!isABACPolicyService(policy) && (POLICY_SUB_CATEGORY_METADATA.equals(policySubCategory) || POLICY_SUB_CATEGORY_DATA.equals(policySubCategory))) {
                        validateParam(StringUtils.isEmpty(newConnectionQn), "Please provide attribute " + ATTR_POLICY_CONNECTION_QN);

                        String existingConnectionQn = getPolicyConnectionQN(existingPolicy);
                        validateOperation (StringUtils.isNotEmpty(existingConnectionQn) && !newConnectionQn.equals(existingConnectionQn), ATTR_POLICY_CONNECTION_QN + " change not Allowed");

                        validateEntityResources(newConnectionQn, resources);
                    }


                    //validate persona policy actions
                    Set<String> validActions = PERSONA_POLICY_VALID_ACTIONS.get(getPolicyServiceName(policy) + policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam (CollectionUtils.isNotEmpty(copyOfActions),
                            "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS + ": Invalid actions "+ copyOfActions);

                    validateParentUpdate(policy, existingPolicy);
                }

                if (POLICY_CATEGORY_PURPOSE.equals(policyCategory)) {
                    //validateParam (policy.hasAttribute(ATTR_POLICY_RESOURCES) && CollectionUtils.isNotEmpty(getPolicyResources(policy)),
                    //        "Provided unexpected attribute " + ATTR_POLICY_RESOURCES);

                    //validate purpose policy actions
                    Set<String> validActions = PURPOSE_POLICY_VALID_ACTIONS.get(policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam (CollectionUtils.isNotEmpty(copyOfActions),
                            "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS + ": Invalid actions "+ copyOfActions);

                    validateParentUpdate(policy, existingPolicy);
                }

                if (POLICY_CATEGORY_DATAMESH.equals(policyCategory)) {
                    validateParam (!DATAMESH_POLICY_VALID_SUB_CATEGORIES.contains(policySubCategory),
                            "Please provide valid value for attribute " + ATTR_POLICY_SUB_CATEGORY + ":"+ DATAMESH_POLICY_VALID_SUB_CATEGORIES);

                    //validate datamesh policy actions
                    Set<String> validActions = DATAMESH_POLICY_VALID_ACTIONS.get(policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam (CollectionUtils.isNotEmpty(copyOfActions),
                            "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS + ": Invalid actions "+ copyOfActions);

                }

            }

        } else {
            //only allow argo & backend
            if (!RequestContext.get().isSkipAuthorizationCheck()) {
                String userName = RequestContext.getCurrentUser();
                validateOperation (!ARGO_SERVICE_USER_NAME.equals(userName) && 
                !BACKEND_SERVICE_USER_NAME.equals(userName) && 
                !GOVERNANCE_WORKFLOWS_SERVICE_USER_NAME.equals(userName),
                    "Create/Update AuthPolicy with policyCategory other than persona, purpose and datamesh");
            }
        }
    }

    private void validateEntityResources(String connQn, List<String> resources) throws AtlasBaseException {
        List<String> entityResources = getFilteredPolicyResources(resources, RESOURCES_ENTITY);

        for (String entity : entityResources) {
            if (!entity.startsWith(connQn)) {
                throw new AtlasBaseException(BAD_REQUEST, entity + " does not belong to connection " + connQn);
            }
        }
    }

    private void validateConnection(String connQn, List<String> resources) throws AtlasBaseException {
        validateParam(StringUtils.isEmpty(connQn), "Please provide attribute " + ATTR_POLICY_CONNECTION_QN);

        validateEntityResources(connQn, resources);

        AtlasEntity connection = getEntityByQualifiedName(entityRetriever, connQn);

        validateParam(!CONNECTION_ENTITY_TYPE.equals(connection.getTypeName()), "Please provide valid connectionQualifiedName");
    }

    private void validateParentUpdate(AtlasEntity policy, AtlasEntity existingPolicy) throws AtlasBaseException {

        Object object = policy.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL);
        if (object != null) {
            AtlasObjectId atlasObjectId = (AtlasObjectId) object;
            String newParentGuid = atlasObjectId.getGuid();

            object = existingPolicy.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL);
            if (object != null) {
                atlasObjectId = (AtlasObjectId) object;
                String existingParentGuid = atlasObjectId.getGuid();
                if (!newParentGuid.equals(existingParentGuid)) {
                    throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy parent (accesscontrol) change is not Allowed");
                }
            }
        }
    }

    private static void validateParam(boolean isInvalid, String errorMessage) throws AtlasBaseException {
        if (isInvalid)
            throw new AtlasBaseException(BAD_REQUEST, errorMessage);
    }

    private static void validateOperation(boolean isInvalid, String errorMessage) throws AtlasBaseException {
        if (isInvalid)
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, errorMessage);
    }

    private static void validatePolicyFilterCriteria(String filterCriteria) throws AtlasBaseException {
        JsonNode entityCriteriaNode = JsonToElasticsearchQuery.parseFilterJSON(filterCriteria, POLICY_FILTER_CRITERIA_ENTITY);
        validateParam(entityCriteriaNode == null, INVALID_FILTER_CRITERIA + "must contain entity root key");

        validateCriterionArray(entityCriteriaNode, 1);
    }

    private static void validateCriterionArray(JsonNode criteriaNode, int currentDepth) throws AtlasBaseException {

        JsonNode criterionArray = criteriaNode.get(POLICY_FILTER_CRITERIA_CRITERION);
        if (criterionArray == null) { // Leaf node
            JsonNode operator = criteriaNode.get(POLICY_FILTER_CRITERIA_OPERATAOR);
            validateParam(operator == null, INVALID_FILTER_CRITERIA + "operator is required");
            validateParam(!POLICY_FILTER_CRITERIA_VALID_OPS.contains(operator.asText()),
                INVALID_FILTER_CRITERIA + "operator must be one of: " + POLICY_FILTER_CRITERIA_VALID_OPS);
            return;
        }

        validateParam(currentDepth > FILTER_CRITERIA_MAX_NESTING_LEVEL, INVALID_FILTER_CRITERIA + "maximum supported nesting depth exceeded");
        validateParam(!criterionArray.isArray(), INVALID_FILTER_CRITERIA + "criterion must be array");
        validateParam(criterionArray.size() > 3, INVALID_FILTER_CRITERIA + "maximum of 3 conditions are supported currently");

        // Recursively validate nested criterion
        for (JsonNode nestedCriteriaNode : criterionArray) {
            validateCriterionArray(nestedCriteriaNode, currentDepth + 1);
        }
    }
}
