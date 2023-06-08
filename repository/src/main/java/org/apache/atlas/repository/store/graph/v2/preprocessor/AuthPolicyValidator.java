package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutations;
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
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ACTIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SERVICE_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SUB_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.BACKEND_SERVICE_USER_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PERSONA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PURPOSE;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_RESOURCE_CATEGORY_PERSONA_ENTITY;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_RESOURCE_CATEGORY_PURPOSE;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_DATA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_GLOSSARY;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_METADATA;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_ACCESS_CONTROL;
import static org.apache.atlas.repository.util.AccessControlUtils.RESOURCES_SPLITTER;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyActions;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyCategory;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyResourceCategory;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyResources;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyServiceName;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicySubCategory;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyType;

public class AuthPolicyValidator {

    private static final Set<String> PERSONA_POLICY_VALID_SUB_CATEGORIES = new HashSet<String>(){{
        add(POLICY_SUB_CATEGORY_METADATA);
        add(POLICY_SUB_CATEGORY_DATA);
        add(POLICY_SUB_CATEGORY_GLOSSARY);
    }};

    private static final Set<String> PURPOSE_POLICY_VALID_SUB_CATEGORIES = new HashSet<String>(){{
        add(POLICY_SUB_CATEGORY_METADATA);
        add(POLICY_SUB_CATEGORY_DATA);
    }};

    private static final Set<String> PERSONA_METADATA_ACTIONS = new HashSet<String>(){{
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

    private static final Set<String> DATA_ACTIONS = new HashSet<String>(){{
        add("select");
    }};

    private static final Set<String> PERSONA_GLOSSARY_ACTIONS = new HashSet<String>(){{
        add("persona-glossary-read");
        add("persona-glossary-update");
        add("persona-glossary-create");
        add("persona-glossary-delete");
        add("persona-glossary-update-custom-metadata");
        add("persona-glossary-add-classifications");
        add("persona-glossary-update-classifications");
        add("persona-glossary-delete-classifications");
    }};

    private static final Map<String, Set<String>> PERSONA_POLICY_VALID_ACTIONS = new HashMap<String, Set<String>>(){{
        put(POLICY_SUB_CATEGORY_METADATA, PERSONA_METADATA_ACTIONS);
        put(POLICY_SUB_CATEGORY_DATA, DATA_ACTIONS);
        put(POLICY_SUB_CATEGORY_GLOSSARY, PERSONA_GLOSSARY_ACTIONS);
    }};

    private static final Set<String> PURPOSE_METADATA_ACTIONS = new HashSet<String>(){{
        add(ENTITY_READ.getType());
        add(ENTITY_CREATE.getType());
        add(ENTITY_UPDATE.getType());
        add(ENTITY_DELETE.getType());
        add(ENTITY_READ_CLASSIFICATION.getType());
        add(ENTITY_ADD_CLASSIFICATION.getType());
        add(ENTITY_UPDATE_CLASSIFICATION.getType());
        add(ENTITY_REMOVE_CLASSIFICATION.getType());
        add(ENTITY_UPDATE_BUSINESS_METADATA.getType());
    }};

    private static final Map<String, Set<String>> PURPOSE_POLICY_VALID_ACTIONS = new HashMap<String, Set<String>>(){{
        put(POLICY_SUB_CATEGORY_METADATA, PURPOSE_METADATA_ACTIONS);
        put(POLICY_SUB_CATEGORY_DATA, DATA_ACTIONS);
    }};

    private static final Set<String> PERSONA_POLICY_VALID_RESOURCE_KEYS = new HashSet<String>() {{
        add("entity");
        add("entity-type");
    }};

    private static final Set<String> PURPOSE_POLICY_VALID_RESOURCE_KEYS = new HashSet<String>() {{
        add("tag");
    }};

    public void validate(AtlasEntity policy, AtlasEntity existingPolicy,
                         AtlasEntity accessControl, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        String policyCategory = null;

        if (policy.hasAttribute(ATTR_POLICY_CATEGORY)) {
            policyCategory = getPolicyCategory(policy);
        } else if (existingPolicy != null) {
            policyCategory = getPolicyCategory(existingPolicy);
        }

        if (POLICY_CATEGORY_PERSONA.equals(policyCategory) || POLICY_CATEGORY_PURPOSE.equals(policyCategory)) {

            if (operation == CREATE) {
                String policySubCategory = getPolicySubCategory(policy);
                List<String> policyActions = getPolicyActions(policy);

                validateParam(StringUtils.isEmpty(getPolicyServiceName(policy)), "Please provide attribute " + ATTR_POLICY_SERVICE_NAME);

                validateParam(StringUtils.isEmpty(getPolicyType(policy)), "Please provide attribute " + ATTR_POLICY_TYPE);

                validateParam(CollectionUtils.isEmpty(policyActions), "Please provide attribute " + ATTR_POLICY_ACTIONS);

                validateOperation (!AtlasEntity.Status.ACTIVE.equals(accessControl.getStatus()), accessControl.getTypeName() + " is not Active");

                if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
                    validateParam (!PERSONA_ENTITY_TYPE.equals(accessControl.getTypeName()),  "Please provide Persona as accesscontrol");

                    validateParam (!PERSONA_POLICY_VALID_SUB_CATEGORIES.contains(policySubCategory), "Please provide valid value for attribute " + ATTR_POLICY_SUB_CATEGORY);

                    if (POLICY_SUB_CATEGORY_DATA.equals(policySubCategory)) {
                        validateParam (!POLICY_RESOURCE_CATEGORY_PERSONA_ENTITY.equals(getPolicyResourceCategory(policy)), "Invalid resource category for Persona");
                    } else {
                        validateParam(!POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM.equals(getPolicyResourceCategory(policy)), "Invalid resource category for Persona");
                    }

                    validateParam (CollectionUtils.isEmpty(getPolicyResources(policy)), "Please provide attribute " + ATTR_POLICY_RESOURCES);

                    //validate persona policy actions
                    Set<String> validActions = PERSONA_POLICY_VALID_ACTIONS.get(policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam (CollectionUtils.isNotEmpty(copyOfActions), "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS);

                    //validate persona policy resources keys
                    Set<String> policyResourceKeys = getPolicyResources(policy).stream().map(x -> x.split(RESOURCES_SPLITTER)[0]).collect(Collectors.toSet());
                    policyResourceKeys.removeAll(PERSONA_POLICY_VALID_RESOURCE_KEYS);
                    validateParam(CollectionUtils.isNotEmpty(policyResourceKeys), "Please provide valid policy resources");
                }

                if (POLICY_CATEGORY_PURPOSE.equals(policyCategory)) {
                    validateParam (!PURPOSE_ENTITY_TYPE.equals(accessControl.getTypeName()),  "Please provide Purpose as accesscontrol");

                    validateParam (!PURPOSE_POLICY_VALID_SUB_CATEGORIES.contains(policySubCategory),
                            "Please provide valid value for attribute " + ATTR_POLICY_SUB_CATEGORY);

                    validateParam (!POLICY_RESOURCE_CATEGORY_PURPOSE.equals(getPolicyResourceCategory(policy)), "Invalid resource category for Purpose");

                    validateParam (CollectionUtils.isNotEmpty(getPolicyResources(policy)), "Provided unexpected attribute " + ATTR_POLICY_RESOURCES);

                    //validate purpose policy actions
                    Set<String> validActions = PURPOSE_POLICY_VALID_ACTIONS.get(policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam (CollectionUtils.isNotEmpty(copyOfActions), "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS);
                }

            } else {

                validateOperation (StringUtils.isNotEmpty(policyCategory) && !policyCategory.equals(getPolicyCategory(existingPolicy)),
                        ATTR_POLICY_CATEGORY + " change not Allowed");

                validateParam(policy.hasAttribute(ATTR_POLICY_ACTIONS) && CollectionUtils.isEmpty(getPolicyActions(policy)),
                        "Please provide attribute " + ATTR_POLICY_ACTIONS);

                String newServiceName = getPolicyServiceName(policy);
                validateOperation (StringUtils.isNotEmpty(newServiceName) && !newServiceName.equals(getPolicyServiceName(existingPolicy)),
                        ATTR_POLICY_SERVICE_NAME + " change not Allowed");

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
                    validateParam (policy.hasAttribute(ATTR_POLICY_RESOURCES) && CollectionUtils.isEmpty(getPolicyResources(policy)),
                            "Please provide attribute " + ATTR_POLICY_RESOURCES);

                    //validate persona policy actions
                    Set<String> validActions = PERSONA_POLICY_VALID_ACTIONS.get(policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam (CollectionUtils.isNotEmpty(copyOfActions), "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS);

                    //validate persona policy resources keys
                    Set<String> policyResourceKeys = getPolicyResources(policy).stream().map(x -> x.split(RESOURCES_SPLITTER)[0]).collect(Collectors.toSet());
                    policyResourceKeys.removeAll(PERSONA_POLICY_VALID_RESOURCE_KEYS);
                    validateParam(CollectionUtils.isNotEmpty(policyResourceKeys), "Please provide valid policy resources");

                    validateParentUpdate(policy, existingPolicy);
                }

                if (POLICY_CATEGORY_PURPOSE.equals(policyCategory)) {
                    validateParam (policy.hasAttribute(ATTR_POLICY_RESOURCES) && CollectionUtils.isNotEmpty(getPolicyResources(policy)),
                            "Provided unexpected attribute " + ATTR_POLICY_RESOURCES);

                    //validate purpose policy actions
                    Set<String> validActions = PURPOSE_POLICY_VALID_ACTIONS.get(policySubCategory);
                    List<String> copyOfActions = new ArrayList<>(policyActions);
                    copyOfActions.removeAll(validActions);
                    validateParam (CollectionUtils.isNotEmpty(copyOfActions), "Please provide valid values for attribute " + ATTR_POLICY_ACTIONS);

                    validateParentUpdate(policy, existingPolicy);
                }
            }

        } else {
            //only allow argo & backend
            String userName = RequestContext.getCurrentUser();
            validateOperation (!ARGO_SERVICE_USER_NAME.equals(userName) && !BACKEND_SERVICE_USER_NAME.equals(userName),
                    "Create/Update AuthPolicy with policyCategory other than persona & purpose");
        }
    }

    private static void validateParentUpdate(AtlasEntity policy, AtlasEntity existingPolicy) throws AtlasBaseException {

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
}
