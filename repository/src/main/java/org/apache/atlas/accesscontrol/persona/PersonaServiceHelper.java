/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.accesscontrol.persona;

import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.POLICY_ALREADY_EXISTS;
import static org.apache.atlas.AtlasErrorCode.RANGER_DUPLICATE_POLICY;
import static org.apache.atlas.AtlasErrorCode.UNAUTHORIZED_CONNECTION_ADMIN;
import static org.apache.atlas.accesscontrol.AccessControlUtil.ACCESS_ADD_REL;
import static org.apache.atlas.accesscontrol.AccessControlUtil.ACCESS_ENTITY_CREATE;
import static org.apache.atlas.accesscontrol.AccessControlUtil.ACCESS_REMOVE_REL;
import static org.apache.atlas.accesscontrol.AccessControlUtil.ACCESS_UPDATE_REL;
import static org.apache.atlas.accesscontrol.AccessControlUtil.LINK_ASSET_ACTION;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getActions;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getAssets;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getConnectionId;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getDataPolicies;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getDataPolicyMaskType;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getMetadataPolicies;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getName;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getPolicies;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getQualifiedName;
import static org.apache.atlas.accesscontrol.AccessControlUtil.isDataMaskPolicy;
import static org.apache.atlas.accesscontrol.AccessControlUtil.isDataPolicy;
import static org.apache.atlas.accesscontrol.AccessControlUtil.isMetadataPolicy;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.BM_ACTION;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.CLASSIFICATION_ACTIONS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.ENTITY_ACTIONS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.GLOSSARY_TERM_RELATIONSHIP;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.GLOSSARY_TYPES;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.LABEL_ACTIONS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RELATED_TERMS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_BM;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_CLASS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_END_ONE_ENTITY;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_END_ONE_ENTITY_CLASS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_END_ONE_ENTITY_TYPE;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_END_TWO_ENTITY;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_END_TWO_ENTITY_CLASS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_END_TWO_ENTITY_TYPE;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_ENTITY_CLASS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_ENTITY_LABEL;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_ENTITY_TYPE;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_KEY_ENTITY;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_REL_TYPE;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.SELECT_ACTION;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.TERM_ACTIONS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getGlossaryPolicies;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getLabelsForPersonaPolicy;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getRoleName;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.isGlossaryPolicy;
import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_TERM_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.CONNECTION_ENTITY_TYPE;


public class PersonaServiceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(PersonaServiceHelper.class);
    private static RangerRole adminRole;

    static {
        getAdminRole();
    }

    public static RangerRole getAdminRole() {
        if (adminRole == null) {
            LOG.info("Fetching Admin role from Ranger");
            AtlasRangerService atlasRangerService = new AtlasRangerService();
            try {
                adminRole = atlasRangerService.getRangerRole(1);
            } catch (AtlasBaseException e) {
                LOG.error("Admin Role not found");
            }
        }

        return adminRole;
    }

    public static void validatePersonaPolicy(PersonaContext context, EntityGraphRetriever entityRetriever,
                                             AtlasRangerService atlasRangerService) throws AtlasBaseException {
        validatePersonaPolicyRequest(context);
        validateConnectionAdmin(context, entityRetriever, atlasRangerService);
        verifyUniqueNameForPersonaPolicy(context);
        verifyUniquePersonaPolicy(context);
    }

    private static void validatePersonaPolicyRequest(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("validatePersonaPolicyRequest");

        AtlasEntity personaPolicy = context.getPersonaPolicy();

        try {
            if (!AtlasEntity.Status.ACTIVE.equals(context.getPersonaExtInfo().getEntity().getStatus())) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Persona is not Active");
            }

            if (CollectionUtils.isEmpty(getActions(personaPolicy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide actions for persona policy");
            }

            if (isMetadataPolicy(personaPolicy)) {
                if (CollectionUtils.isEmpty(getAssets(personaPolicy))) {
                    throw new AtlasBaseException(BAD_REQUEST, "Please provide assets for persona policy");
                }

                if (StringUtils.isEmpty(getConnectionId(personaPolicy))) {
                    throw new AtlasBaseException(BAD_REQUEST, "Please provide connectionGuid for persona policy");
                }
            }

            if (isGlossaryPolicy(personaPolicy)) {
                if (CollectionUtils.isEmpty(getAssets(personaPolicy))) {
                    throw new AtlasBaseException(BAD_REQUEST, "Please provide assets for persona policy");
                }
            }

            if (isDataPolicy(personaPolicy)) {
                if (CollectionUtils.isEmpty(getAssets(personaPolicy))) {
                    throw new AtlasBaseException(BAD_REQUEST, "Please provide assets for persona policy");
                }

                if (StringUtils.isEmpty(getConnectionId(personaPolicy))) {
                    throw new AtlasBaseException(BAD_REQUEST, "Please provide connectionGuid for persona policy");
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private static void validateConnectionAdmin(PersonaContext context, EntityGraphRetriever entityRetriever,
                                         AtlasRangerService atlasRangerService) throws AtlasBaseException {
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        if (isMetadataPolicy(personaPolicy) || isDataPolicy(personaPolicy)) {

            String connectionGuid = getConnectionId(personaPolicy);
            AtlasEntity connection = entityRetriever.toAtlasEntity(connectionGuid);

            if (connection != null) {
                if (!CONNECTION_ENTITY_TYPE.equals(connection.getTypeName())) {
                    throw new AtlasBaseException(BAD_REQUEST, "Invalid type for connectionGuid, expected Connection, passed " + connection.getTypeName());
                }
                context.setConnection(connection);
            }

            // case 0: all admins true
            /*List<String> adminRoles = (List<String>) connection.getAttribute("adminRoles");
            if (CollectionUtils.isNotEmpty(adminRoles) && adminRoles.contains(adminRole.getName())) {
                //valid user
                return;
            }*/

            // case 1: connection admins role
            String currentUserName = AtlasAuthorizationUtils.getCurrentUserName();
            String connectionRoleName = "connection_admins_" + connectionGuid;
            RangerRole connectionAdminRole = atlasRangerService.getRangerRole(connectionRoleName);

            List<String> users = connectionAdminRole.getUsers().stream().map(x -> x.getName()).collect(Collectors.toList());
            if (!users.contains(currentUserName)) {
                throw new AtlasBaseException(UNAUTHORIZED_CONNECTION_ADMIN, currentUserName, connectionGuid);
            }
        }
    }

    private static void verifyUniqueNameForPersonaPolicy(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("verifyUniqueNameForPersonaPolicy");

        if (!context.isCreateNewPersonaPolicy() && !getName(context.getExistingPersonaPolicy()).equals(getName(context.getPersona()))) {
            return;
        }
        List<String> policyNames = new ArrayList<>();

        List<AtlasEntity> personaPolicies = getPolicies(context.getPersonaExtInfo());
        if (CollectionUtils.isNotEmpty(personaPolicies)) {
            if (context.isCreateNewPersonaPolicy()) {
                personaPolicies = personaPolicies.stream()
                        .filter(x -> !x.getGuid().equals(context.getPersonaPolicy().getGuid()))
                        .collect(Collectors.toList());
            }
        }

        personaPolicies.forEach(x -> policyNames.add(getName(x)));

        try {
            String newPolicyName = getName(context.getPersonaPolicy());
            if (policyNames.contains(newPolicyName)) {
                throw new AtlasBaseException(POLICY_ALREADY_EXISTS, newPolicyName);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private static void verifyUniquePersonaPolicy(PersonaContext context) throws AtlasBaseException {
        List<AtlasEntity> policies = null;

        if (context.isMetadataPolicy()) {
            policies = getMetadataPolicies(context.getPersonaExtInfo());
        } else if (context.isGlossaryPolicy()) {
            policies = getGlossaryPolicies(context.getPersonaExtInfo());
        }  else if (context.isDataPolicy()) {
            policies = getDataPolicies(context.getPersonaExtInfo());
        }
        verifyUniqueAssetsForPolicy(context, policies, context.getPersonaPolicy().getGuid());
    }

    private static void verifyUniqueAssetsForPolicy(PersonaContext context, List<AtlasEntity> policies, String guidToExclude) throws AtlasBaseException {
        AtlasEntity newPersonaPolicy = context.getPersonaPolicy();
        List<String> newPersonaPolicyAssets = getAssets(newPersonaPolicy);

        if (CollectionUtils.isNotEmpty(policies)) {
            for (AtlasEntity policy : policies) {
                List<String> assets = getAssets(policy);

                if (!StringUtils.equals(guidToExclude, policy.getGuid()) && context.isDataMaskPolicy() == isDataMaskPolicy(policy) && assets.equals(newPersonaPolicyAssets)) {
                    throw new AtlasBaseException(RANGER_DUPLICATE_POLICY, getName(policy), policy.getGuid());
                }
            }
        }
    }

    /*
     * This method will convert a persona policy into multiple Ranger
     * policies based on actions in persona policy
     *
     * @param personaPolicy persona policy object
     * @returns List<RangerPolicy> list of Ranger policies corresponding to provided Persona policy
     * */
    protected static List<RangerPolicy> personaPolicyToRangerPolicies(PersonaContext context, List<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();

        if (context.isMetadataPolicy()) {
            rangerPolicies = metadataPolicyToRangerPolicy(context, new HashSet<>(actions));
        } else if (context.isGlossaryPolicy()) {
            rangerPolicies = glossaryPolicyToRangerPolicy(context, new HashSet<>(actions));
        } else if (context.isDataPolicy()) {
            rangerPolicies = dataPolicyToRangerPolicy(context, new HashSet<>(actions));
        }

        return rangerPolicies;
    }

    private static List<RangerPolicy> glossaryPolicyToRangerPolicy(PersonaContext context, Set<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        AtlasEntity persona = context.getPersona();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        List<String> assets = getAssets(personaPolicy);
        if (CollectionUtils.isEmpty(assets)) {
            throw new AtlasBaseException("Glossary qualified name list is empty");
        }

        List<String> rangerPolicyItemAssets = new ArrayList<>();
        assets.forEach(x -> rangerPolicyItemAssets.add("*" + x + "*"));

        String roleName = getRoleName(persona);

        for (String action : new HashSet<>(actions)) {
            if (!actions.contains(action)) {
                continue;
            }

            Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
            List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
            String policyName = "";

            if (ENTITY_ACTIONS.contains(action)) {
                if (actions.contains(ACCESS_ENTITY_CREATE)) {
                    rangerPolicies.addAll(glossaryPolicyToRangerPolicy(context, new HashSet<String>() {{
                        add(GLOSSARY_TERM_RELATIONSHIP);
                    }}));
                }

                policyName = "Glossary-" + UUID.randomUUID();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                for (String entityAction : ENTITY_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        actions.remove(entityAction);
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(entityAction));
                    }
                }
            }

            if (action.equals(GLOSSARY_TERM_RELATIONSHIP)) {
                policyName = "Glossary-term-relationship-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_ADD_REL));
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_UPDATE_REL));
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_REMOVE_REL));
                actions.remove(GLOSSARY_TERM_RELATIONSHIP);
            }

            if (LABEL_ACTIONS.contains(action)) {
                policyName = "Glossary-labels-" + UUID.randomUUID();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                resources.put(RESOURCE_ENTITY_LABEL, new RangerPolicy.RangerPolicyResource("*"));

                for (String labelAction : LABEL_ACTIONS) {
                    if (actions.contains(labelAction)) {
                        actions.remove(labelAction);
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(labelAction));
                    }
                }
            }

            if (CLASSIFICATION_ACTIONS.contains(action)) {
                policyName = "Glossary-classification-" + UUID.randomUUID();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                resources.put(RESOURCE_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                for (String tagAction : CLASSIFICATION_ACTIONS) {
                    if (actions.contains(tagAction)) {
                        actions.remove(tagAction);
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(tagAction));
                    }
                }
            }

            if (BM_ACTION.equals(action)) {
                policyName = "Glossary-entity-business-metadata-" + UUID.randomUUID();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_BM, new RangerPolicy.RangerPolicyResource("*"));

                accesses.add(new RangerPolicy.RangerPolicyItemAccess(BM_ACTION));
                actions.remove(BM_ACTION);
            }

            if (LINK_ASSET_ACTION.equals(action)) {
                policyName = "Glossary-relationship-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));


                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_ADD_REL));
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_UPDATE_REL));
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_REMOVE_REL));
                actions.remove(LINK_ASSET_ACTION);

                rangerPolicies.addAll(glossaryPolicyToRangerPolicy(context, new HashSet<String>() {{ add(RELATED_TERMS); }}));
            }

            if (action.equals(RELATED_TERMS)) {
                policyName = "Glossary-related-terms-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));


                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_ADD_REL));
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_UPDATE_REL));
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_REMOVE_REL));
                actions.remove(RELATED_TERMS);
            }

            if (MapUtils.isNotEmpty(resources)) {
                RangerPolicy rangerPolicy = getRangerPolicy(context);

                rangerPolicy.setName(policyName);
                rangerPolicy.setResources(resources);

                RangerPolicy.RangerPolicyItem policyItem = getPolicyItem(accesses, roleName);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.add(rangerPolicy);
            }
        }

        return rangerPolicies;
    }

    private static List<RangerPolicy> metadataPolicyToRangerPolicy(PersonaContext context, Set<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        AtlasEntity persona = context.getPersona();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        List<String> assets = getAssets(personaPolicy);
        if (CollectionUtils.isEmpty(assets)) {
            throw new AtlasBaseException("Assets list is empty");
        }

        boolean isConnection = false;
        if (assets.size() == 1 && context.getConnection() != null) {
            String connectionQualifiedName = getQualifiedName(context.getConnection());
            if (assets.get(0).equals(connectionQualifiedName)) {
                isConnection = true;
            }
        }

        List<String> rangerPolicyItemAssets = new ArrayList<>(assets);
        assets.forEach(x -> rangerPolicyItemAssets.add(x + "/*"));

        String roleName = getRoleName(persona);

        for (String action : new HashSet<>(actions)) {
            if (!actions.contains(action)) {
                continue;
            }

            Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
            List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
            String policyName = "";

            if (ENTITY_ACTIONS.contains(action)) {
                policyName = "CRUD-" + UUID.randomUUID();

                if (isConnection) {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                } else {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(Arrays.asList("Process", "Catalog"), false, false));
                }

                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                for (String entityAction : ENTITY_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        actions.remove(entityAction);
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(entityAction));
                    }
                }
            }

            if (CLASSIFICATION_ACTIONS.contains(action)) {
                policyName = "classification-" + UUID.randomUUID();

                if (isConnection) {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                } else {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(Arrays.asList("Process", "Catalog"), false, false));
                }

                resources.put(RESOURCE_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                for (String tagAction : CLASSIFICATION_ACTIONS) {
                    if (actions.contains(tagAction)) {
                        actions.remove(tagAction);
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(tagAction));
                    }
                }
            }

            if (BM_ACTION.equals(action)) {
                policyName = "entity-business-metadata-" + UUID.randomUUID();

                if (isConnection) {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                } else {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(Arrays.asList("Process", "Catalog"), false, false));
                }

                resources.put(RESOURCE_BM, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                accesses.add(new RangerPolicy.RangerPolicyItemAccess(BM_ACTION));
                actions.remove(BM_ACTION);
            }

            if (TERM_ACTIONS.contains(action)) {
                policyName = "terms-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(ATLAS_GLOSSARY_TERM_ENTITY_TYPE));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));


                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                for (String termAction : TERM_ACTIONS) {
                    if (actions.contains(termAction)) {
                        actions.remove(termAction);
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess("add-terms".equals(termAction) ? ACCESS_ADD_REL : ACCESS_REMOVE_REL));
                    }
                }
            }

            if (LINK_ASSET_ACTION.equals(action)) {
                policyName = "link-assets-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(Arrays.asList("Catalog", "Connection", "Dataset", "Infrastructure", "Process", "ProcessExecution", "Namespace"), false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_ADD_REL));
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_REMOVE_REL));
                actions.remove(LINK_ASSET_ACTION);
            }

            if (MapUtils.isNotEmpty(resources)) {
                RangerPolicy rangerPolicy = getRangerPolicy(context);

                rangerPolicy.setName(policyName);
                rangerPolicy.setResources(resources);

                RangerPolicy.RangerPolicyItem policyItem = getPolicyItem(accesses, roleName);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.add(rangerPolicy);
            }
        }

        return rangerPolicies;
    }

    private static List<RangerPolicy> dataPolicyToRangerPolicy(PersonaContext context, Set<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        AtlasEntity persona = context.getPersona();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        List<String> assets = getAssets(personaPolicy);
        if (CollectionUtils.isEmpty(assets)) {
            throw new AtlasBaseException("Assets list is empty");
        }

        List<String> rangerPolicyItemAssets = new ArrayList<>(assets);
        assets.forEach(x -> rangerPolicyItemAssets.add(x + "/*"));

        for (String action : new HashSet<>(actions)) {
            RangerPolicy rangerPolicy = getRangerPolicy(context);

            if (SELECT_ACTION.contains(action)) {
                rangerPolicy.setName("dataPolicy-" + UUID.randomUUID());

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = Collections.singletonList(new RangerPolicy.RangerPolicyItemAccess(SELECT_ACTION));

                if (context.isDataMaskPolicy()) {
                    rangerPolicy.setName("dataPolicy-mask" + UUID.randomUUID());
                    RangerPolicy.RangerPolicyItemDataMaskInfo maskInfo = new RangerPolicy.RangerPolicyItemDataMaskInfo(getDataPolicyMaskType(personaPolicy), null, null);

                    RangerPolicy.RangerDataMaskPolicyItem policyItem = new RangerPolicy.RangerDataMaskPolicyItem(accesses, maskInfo,  null,
                            null, Arrays.asList(getRoleName(persona)), null, false);

                    rangerPolicy.setDataMaskPolicyItems(Arrays.asList(policyItem));

                } else {
                    RangerPolicy.RangerPolicyItem policyItem = getPolicyItem(accesses, getRoleName(persona));

                    if (context.isAllowPolicy()) {
                        rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                    } else {
                        rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                    }
                }
            }

            if (MapUtils.isNotEmpty(rangerPolicy.getResources())) {
                rangerPolicies.add(rangerPolicy);
            }
        }

        return rangerPolicies;
    }

    private static RangerPolicy getRangerPolicy(PersonaContext context){
        RangerPolicy rangerPolicy = new RangerPolicy();
        AtlasEntity persona = context.getPersona();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        rangerPolicy.setPolicyLabels(getLabelsForPersonaPolicy(persona.getGuid(), personaPolicy.getGuid()));

        rangerPolicy.setPolicyType(context.isDataMaskPolicy() ? 1 : 0);

        rangerPolicy.setService(context.isDataPolicy() ? "heka" : "atlas");

        return rangerPolicy;
    }

    private static RangerPolicy.RangerPolicyItem getPolicyItem(List<RangerPolicy.RangerPolicyItemAccess> accesses, String roleName) {
        return new RangerPolicy.RangerPolicyItem(accesses, null,
                null, Arrays.asList(roleName), null, false);
    }
}
